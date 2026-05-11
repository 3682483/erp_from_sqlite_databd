import os
import json
from fastapi import FastAPI, Request, Query, Body
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import database as db

HOST = "0.0.0.0"
PORT = 8000

app = FastAPI(title="ERP 数据管理平台")

templates_dir = os.path.join(os.path.dirname(__file__), "templates")
static_dir = os.path.join(os.path.dirname(__file__), "static")

os.makedirs(templates_dir, exist_ok=True)
os.makedirs(static_dir, exist_ok=True)

app.mount("/static", StaticFiles(directory=static_dir), name="static")
templates = Jinja2Templates(directory=templates_dir)


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    modules = db.get_all_tables()
    all_tables = []
    for m in modules:
        for t in m["tables"]:
            all_tables.append({"table_name": t, "module_name": m["name"]})

    return templates.TemplateResponse(
        "index.html",
        {"request": request, "modules": modules, "all_tables": all_tables, "current_table": None},
    )


@app.get("/table/{table_name}", response_class=HTMLResponse)
async def view_table(
    request: Request,
    table_name: str,
    page: int = Query(1, ge=1),
    search: str = Query(""),
):
    modules = db.get_all_tables()
    all_tables = []
    for m in modules:
        for t in m["tables"]:
            all_tables.append({"table_name": t, "module_name": m["name"]})

    try:
        cols = db.get_table_info(table_name)
        has_pk = any(c["pk"] for c in cols)
        result = db.get_table_data(table_name, page=page, search=search)
    except Exception as e:
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "modules": modules,
                "all_tables": all_tables,
                "current_table": table_name,
                "error": str(e),
                "columns": [],
                "rows": [],
                "total": 0,
                "page": 1,
                "page_size": 50,
                "total_pages": 1,
                "search": search,
                "has_pk": False,
            },
        )

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "modules": modules,
            "all_tables": all_tables,
            "current_table": table_name,
            "columns": cols,
            "rows": result["rows"],
            "total": result["total"],
            "page": result["page"],
            "page_size": result["page_size"],
            "total_pages": result["total_pages"],
            "search": search,
            "error": None,
            "has_pk": has_pk,
        },
    )


@app.post("/api/table/{table_name}/insert", response_class=JSONResponse)
async def api_insert(table_name: str, data: dict = Body(...)):
    result = db.insert_row(table_name, data)
    return result


@app.put("/api/table/{table_name}/update", response_class=JSONResponse)
async def api_update(table_name: str, data: dict = Body(...)):
    result = db.update_row(table_name, data)
    return result


@app.post("/api/table/{table_name}/delete", response_class=JSONResponse)
async def api_delete(table_name: str, data: dict = Body(...)):
    result = db.delete_row(table_name, data)
    return result


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=HOST, port=PORT)
