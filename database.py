import sqlite3
from collections import defaultdict
from typing import Any

DB_PATH = "erp.db"


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_all_tables():
    """获取所有业务表，按模块前缀分组"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [t[0] for t in cursor.fetchall()]
    conn.close()

    # 模块分组映射
    module_names = {
        "addr": "地址管理",
        "app": "系统基础",
        "asset": "资产管理",
        "bbs": "内部论坛",
        "car": "车辆管理",
        "crm": "客户关系",
        "eas": "绩效考核",
        "eba": "会员管理",
        "ebm": "会员消费",
        "ebs": "进销存",
        "edoc": "电子文档",
        "edt": "库存管理",
        "ekg": "知识管理",
        "emf": "生产制造",
        "emp": "人事档案",
        "eqs": "问卷调查",
        "evm": "财务管理",
        "hrm": "招聘管理",
        "mio": "资金管理",
        "mup": "系统管理",
        "oa": "OA办公",
        "pm": "项目管理",
        "qm": "质量管理",
        "rep": "报表系统",
        "res": "物料管理",
        "rival": "竞争对手",
        "sc": "能力资源",
        "sup": "供应商管理",
        "tbx": "协同办公",
        "timer": "考勤管理",
        "train": "培训管理",
        "wage": "工资管理",
    }

    groups = defaultdict(list)
    singles = []
    for t in tables:
        prefix = t.split("_")[0] if "_" in t else t
        if prefix in module_names:
            groups[prefix].append(t)
        else:
            singles.append(t)

    # 对s singles也尝试用第一个under_score
    result = []
    for prefix, tbls in sorted(groups.items()):
        result.append({
            "prefix": prefix,
            "name": module_names.get(prefix, prefix.upper()),
            "tables": tbls,
            "count": sum(1 for t in tbls),
        })
    return result


def get_table_info(table_name: str) -> list[dict]:
    """获取表的列信息"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info('{table_name}')")
    cols = [
        {"name": c[1], "type": c[2], "notnull": bool(c[3]), "pk": bool(c[5])}
        for c in cursor.fetchall()
    ]
    conn.close()
    return cols


def get_pk_columns(table_name: str) -> list[str]:
    """获取主键列名（若表无 PK，返回 ['rowid']）"""
    cols = get_table_info(table_name)
    pk = [c["name"] for c in cols if c["pk"]]
    return pk if pk else ["rowid"]


def get_non_pk_columns(table_name: str) -> list[str]:
    """获取非主键列名"""
    cols = get_table_info(table_name)
    return [c["name"] for c in cols if not c["pk"]]


def insert_row(table_name: str, data: dict) -> dict:
    """插入一行数据"""
    cols = get_table_info(table_name)

    # 移除可能传进来的 rowid（用于无 PK 表）
    if "rowid" in data:
        del data["rowid"]

    insert_cols = [c["name"] for c in cols if c["name"] in data]
    if not insert_cols:
        return {"success": False, "message": "没有可插入的字段"}

    insert_vals = [data[c] for c in insert_cols]
    placeholders = ",".join(["?"] * len(insert_cols))
    col_names = ",".join([f'"{c}"' for c in insert_cols])

    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f'INSERT INTO "{table_name}" ({col_names}) VALUES ({placeholders})', insert_vals)
        conn.commit()
        new_id = cursor.lastrowid
        conn.close()
        return {"success": True, "row_id": new_id, "message": "插入成功"}
    except Exception as e:
        conn.close()
        return {"success": False, "message": str(e)}


def update_row(table_name: str, data: dict) -> dict:
    """更新一行数据"""
    cols = get_table_info(table_name)
    has_pk = any(c["pk"] for c in cols)

    if has_pk:
        pk_cols = [c["name"] for c in cols if c["pk"]]
        pk_vals = {k: v for k, v in data.items() if k in pk_cols}
        if not pk_vals:
            return {"success": False, "message": "缺少主键条件"}
        update_cols = {k: v for k, v in data.items() if k not in pk_cols}
        where_clause = " AND ".join([f'"{k}"=?' for k in pk_vals])
        where_params = list(pk_vals.values())
    else:
        # 无 PK 的表用 rowid 标识
        if "rowid" not in data:
            return {"success": False, "message": "缺少 rowid 条件"}
        update_cols = {k: v for k, v in data.items() if k != "rowid"}
        where_clause = "rowid=?"
        where_params = [data["rowid"]]

    if not update_cols:
        return {"success": False, "message": "无更新字段"}

    set_clause = ",".join([f'"{k}"=?' for k in update_cols])
    params = list(update_cols.values()) + where_params

    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f'UPDATE "{table_name}" SET {set_clause} WHERE {where_clause}', params)
        conn.commit()
        affected = cursor.rowcount
        conn.close()
        return {"success": True, "affected": affected, "message": f"更新成功，影响 {affected} 行"}
    except Exception as e:
        conn.close()
        return {"success": False, "message": str(e)}


def delete_row(table_name: str, pk_data: dict) -> dict:
    """删除一行数据"""
    if not pk_data:
        return {"success": False, "message": "缺少主键条件"}

    where_clause = " AND ".join([f'"{k}"=?' for k in pk_data])
    params = list(pk_data.values())

    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f'DELETE FROM "{table_name}" WHERE {where_clause}', params)
        conn.commit()
        affected = cursor.rowcount
        conn.close()
        return {"success": True, "affected": affected, "message": f"删除成功，影响 {affected} 行"}
    except Exception as e:
        conn.close()
        return {"success": False, "message": str(e)}


def get_table_data(table_name: str, page: int = 1, page_size: int = 50, search: str = ""):
    """分页查询表数据"""
    conn = get_connection()
    cursor = conn.cursor()

    # 先查总数
    if search:
        cols = get_table_info(table_name)
        like_clauses = " OR ".join(
            [f'"{c["name"]}" LIKE ?' for c in cols if c["type"].upper() in ("TEXT", "VARCHAR", "CHAR")]
        )
        if like_clauses:
            count_sql = f'SELECT COUNT(*) FROM "{table_name}" WHERE {like_clauses}'
            params = [f"%{search}%"] * len([c for c in cols if c["type"].upper() in ("TEXT", "VARCHAR", "CHAR")])
        else:
            count_sql = f'SELECT COUNT(*) FROM "{table_name}"'
            params = []
    else:
        count_sql = f'SELECT COUNT(*) FROM "{table_name}"'
        params = []

    cursor.execute(count_sql, params)
    total = cursor.fetchone()[0]

    # 查数据
    offset = (page - 1) * page_size
    has_pk = any(c["pk"] for c in get_table_info(table_name))

    select_cols = '"' + '","'.join([c["name"] for c in get_table_info(table_name)]) + '"'
    if not has_pk:
        select_cols = "rowid," + select_cols

    if search and like_clauses:
        data_sql = f'SELECT {select_cols} FROM "{table_name}" WHERE {like_clauses} ORDER BY rowid LIMIT ? OFFSET ?'
        data_params = params + [page_size, offset]
    else:
        data_sql = f'SELECT {select_cols} FROM "{table_name}" ORDER BY rowid LIMIT ? OFFSET ?'
        data_params = [page_size, offset]

    cursor.execute(data_sql, data_params)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description] if cursor.description else []
    data = [dict(zip(columns, row)) for row in rows]

    conn.close()
    return {
        "columns": columns,
        "rows": data,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": max(1, (total + page_size - 1) // page_size),
    }
