import json

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from pydantic import BaseModel

memory_db = {}
sst_num = 1
app = FastAPI()


class Key(BaseModel):
    value: str


with open("manifest.txt", "w") as fp:
    pass


@app.get("/")
def read_root():
    return {"msg": "Hello World"}


@app.get("/{key}")
def read_item(key: str):
    global memory_db
    print(f"this is {memory_db}")
    if key in memory_db:
        print("woop")
        return {"value": memory_db[key], "key": key}
    else:
        with open("manifest.txt", "r") as fp:
            for sstable in reversed(fp.readlines()):
                with open(sstable.strip(), "r") as d:
                    temp_db = json.load(d)
                if key in temp_db:
                    return {"value": temp_db[key], "key": key}
        raise HTTPException(status_code=404, detail="Item not found")


@app.put("/{key}")
def update_item(key: str, item: Key):
    global sst_num
    memory_db[key] = item.value
    new_sstable = f"sstable_{sst_num}.json"
    if len(list(memory_db.keys())) > 3:
        with open(new_sstable, "w") as d:
            json.dump(memory_db, d)
            d.close()
        with open("manifest.txt", "a") as d:
            d.write(new_sstable + "\n")
            d.close()
        sst_num += 1
        memory_db.clear()
    return {"value": item.value, "key": key}
