from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from pydantic import BaseModel

app = FastAPI()


class Key(BaseModel):
    value: str


memory_db = {}


@app.get("/")
def read_root():
    return {"msg": "Hello World"}


@app.get("/{key}")
def read_item(key: str):
    if key in memory_db:
        return {"value": memory_db[key], "key": key}
    raise HTTPException(status_code=404, detail="Item not found")


@app.put("/{key}")
def update_item(key: str, item: Key):
    memory_db[key] = item.value
    return {"value": item.value, "key": key}
