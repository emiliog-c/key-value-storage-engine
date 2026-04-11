import json
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from pydantic import BaseModel

memory_db = {}
app = FastAPI()


class Key(BaseModel):
    value: str


if not os.path.exists("manifest.txt"):
    with open("manifest.txt", "w") as fp:
        pass

if os.path.exists("wal.db"):
    with open("wal.db", "r") as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            memory_db[record["key"]] = record["value"]
        fp.close()
else:
    with open("wal.db", "w") as fp:
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
    with open("wal.db", "a") as fp:
        fp.write(json.dumps({"op": "put", "key": key, "value": item.value}) + "\n")
        os.fsync(fp.fileno())
        fp.close()
    memory_db[key] = item.value
    script_parent = Path(__file__).resolve().parent

    if len(list(memory_db.keys())) >= 2000:
        with open("manifest.txt", "r") as fp:
            sstables = [line.strip() for line in fp if line.strip()]
            if not sstables:
                new_sstable = "sstable_1.json"
            else:
                sst, sst_value = sstables[-1].strip().split("_")
                new_sst_val, ext = sst_value.strip().split(".")
                new_sstable = f"sstable_{int(new_sst_val) + 1}.json"
            fp.close()

        with open(new_sstable, "w") as d:
            sorted_mem = dict(sorted(memory_db.items()))
            json.dump(sorted_mem, d)
            d.flush()
            os.fsync(d.fileno())
            fd = os.open(script_parent, os.O_RDONLY)
            os.fsync(fd)
            os.close(fd)
            d.close()

        with open("manifest.tmp", "w") as fp:
            sstables.append(new_sstable)
            for line in sstables:
                fp.write(line + "\n")
            os.fsync(fp.fileno())
            fp.close()
        temp = Path("manifest.tmp")
        temp.rename("manifest.txt")
        with open("manifest.txt", "a") as fp:
            os.fsync(fp.fileno())
            fd = os.open(script_parent, os.O_RDONLY)
            os.fsync(fd)
            os.close(fd)
            fp.close()
        memory_db.clear()
        with open("wal.db", "w") as fp:
            os.fsync(fp.fileno())
            fp.close()

    return {"value": item.value, "key": key}
