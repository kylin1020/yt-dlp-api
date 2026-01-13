import asyncio
import hashlib
import json
import os
import shutil
import sqlite3
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from shutil import rmtree
from threading import Lock
from typing import Any

from dotenv import load_dotenv

load_dotenv()

from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from fastapi.responses import FileResponse
from pydantic import BaseModel
from yt_dlp import YoutubeDL

# 临时文件目录
TEMP_BASE_DIR = Path(os.getenv("TEMP_DIR", "/tmp/yt-dlp-downloads"))
TEMP_BASE_DIR.mkdir(parents=True, exist_ok=True)

# 数据库路径
DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "tasks.db"

# 并发控制
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT_DOWNLOADS", "64"))
executor = ThreadPoolExecutor(max_workers=MAX_CONCURRENT)
active_count = 0
count_lock = Lock()
db_lock = Lock()

# 任务清理配置（秒）
TASK_EXPIRE_SECONDS = int(os.getenv("TASK_EXPIRE_SECONDS", "300"))
CLEANUP_INTERVAL = int(os.getenv("CLEANUP_INTERVAL", "60"))

# 运行时状态（不持久化）
runtime_state: dict[str, dict] = {}  # task_id -> {progress, speed, total_bytes}
cancelled_tasks: set[str] = set()  # 被取消的任务ID

# 带宽监控
bandwidth_stats = {"total_bytes": 0, "last_reset": time.time()}


def init_db():
    """初始化数据库"""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT PRIMARY KEY,
                status TEXT,
                progress REAL,
                error TEXT,
                task_dir TEXT,
                finished_at REAL,
                files TEXT,
                info TEXT,
                url TEXT,
                params TEXT,
                created_at REAL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS file_mapping (
                file_id TEXT PRIMARY KEY,
                task_id TEXT,
                filename TEXT,
                filepath TEXT
            )
        """)


def db_get_task(task_id: str) -> dict | None:
    """从数据库获取单个任务"""
    with db_lock, sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,)).fetchone()
        if not row:
            return None
        return {
            "status": row["status"],
            "progress": row["progress"],
            "error": row["error"],
            "task_dir": row["task_dir"],
            "finished_at": row["finished_at"],
            "files": json.loads(row["files"]) if row["files"] else None,
            "info": json.loads(row["info"]) if row["info"] else None,
            "url": row["url"],
            "params": json.loads(row["params"]) if row["params"] else None,
            "created_at": row["created_at"],
        }


def db_save_task(task_id: str, task: dict):
    """保存任务到数据库"""
    info_json = json.dumps(task.get("info")) if task.get("info") else None
    files_json = json.dumps(task.get("files")) if task.get("files") else None
    params_json = json.dumps(task.get("params")) if task.get("params") else None
    with db_lock, sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            INSERT OR REPLACE INTO tasks (task_id, status, progress, error, task_dir, finished_at, files, info, url, params, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (task_id, task.get("status"), task.get("progress"), task.get("error"),
              task.get("task_dir"), task.get("finished_at"), files_json, info_json,
              task.get("url"), params_json, task.get("created_at")))


def db_update_status(task_id: str, status: str, **kwargs):
    """更新任务状态"""
    with db_lock, sqlite3.connect(DB_PATH) as conn:
        sets = ["status = ?"]
        values = [status]
        for k, v in kwargs.items():
            if k in ("files", "info", "params") and v is not None:
                v = json.dumps(v)
            sets.append(f"{k} = ?")
            values.append(v)
        values.append(task_id)
        conn.execute(f"UPDATE tasks SET {', '.join(sets)} WHERE task_id = ?", values)


def db_delete_task(task_id: str):
    """从数据库删除任务"""
    with db_lock, sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM file_mapping WHERE task_id = ?", (task_id,))
        conn.execute("DELETE FROM tasks WHERE task_id = ?", (task_id,))


def db_get_pending_tasks() -> list[tuple[str, str, dict | None]]:
    """获取所有等待中的任务，按创建时间排序"""
    with db_lock, sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT task_id, url, params FROM tasks WHERE status = 'pending' ORDER BY created_at"
        ).fetchall()
        return [(r["task_id"], r["url"], json.loads(r["params"]) if r["params"] else None) for r in rows]


def db_get_expired_tasks() -> list[tuple[str, str | None]]:
    """获取过期的已完成/失败任务"""
    cutoff = time.time() - TASK_EXPIRE_SECONDS
    with db_lock, sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT task_id, task_dir FROM tasks WHERE status IN ('completed', 'failed') AND finished_at < ?",
            (cutoff,)
        ).fetchall()
        return [(r["task_id"], r["task_dir"]) for r in rows]


def db_get_all_task_ids() -> set[str]:
    """获取所有任务ID"""
    with db_lock, sqlite3.connect(DB_PATH) as conn:
        return {r[0] for r in conn.execute("SELECT task_id FROM tasks").fetchall()}


def db_save_file_mapping(file_id: str, task_id: str, filename: str, filepath: str):
    """保存文件映射"""
    with db_lock, sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO file_mapping (file_id, task_id, filename, filepath) VALUES (?, ?, ?, ?)",
            (file_id, task_id, filename, filepath)
        )


def db_get_file_mapping(file_id: str) -> dict | None:
    """获取文件映射"""
    with db_lock, sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM file_mapping WHERE file_id = ?", (file_id,)).fetchone()
        if not row:
            return None
        return {"task_id": row["task_id"], "filename": row["filename"], "filepath": row["filepath"]}


def db_find_active_task_by_url(url: str) -> str | None:
    """查找相同URL的活跃任务（pending或downloading状态）"""
    with db_lock, sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT task_id FROM tasks WHERE url = ? AND status IN ('pending', 'downloading')",
            (url,)
        ).fetchone()
        return row[0] if row else None


def generate_file_id(task_id: str, filename: str) -> str:
    """生成文件ID（基于task_id和filename的hash）"""
    return hashlib.md5(f"{task_id}:{filename}".encode()).hexdigest()


def try_start_pending():
    """尝试启动等待中的任务"""
    global active_count
    with count_lock:
        if active_count >= MAX_CONCURRENT:
            return
    pending = db_get_pending_tasks()
    for task_id, url, params in pending:
        with count_lock:
            if active_count >= MAX_CONCURRENT:
                break
            active_count += 1
        executor.submit(download_task, task_id, url, params)


async def cleanup_expired_tasks():
    """后台协程：清理过期任务并启动等待中的任务"""
    while True:
        await asyncio.sleep(CLEANUP_INTERVAL)
        for task_id, task_dir in db_get_expired_tasks():
            if task_dir and Path(task_dir).exists():
                rmtree(task_dir)
            db_delete_task(task_id)
            runtime_state.pop(task_id, None)
        try_start_pending()


def cleanup_orphan_dirs():
    """清理孤立的任务目录"""
    db_task_ids = db_get_all_task_ids()
    for item in TEMP_BASE_DIR.iterdir():
        if item.is_dir() and item.name not in db_task_ids:
            rmtree(item)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db()
    # 将 downloading 状态重置为 pending（服务重启时恢复）
    with db_lock, sqlite3.connect(DB_PATH) as conn:
        conn.execute("UPDATE tasks SET status = 'pending', progress = 0 WHERE status = 'downloading'")
    cleanup_orphan_dirs()
    try_start_pending()
    task = asyncio.create_task(cleanup_expired_tasks())
    yield
    task.cancel()


app = FastAPI(title="yt-dlp API", lifespan=lifespan)


class DownloadRequest(BaseModel):
    url: str
    params: dict[str, Any] | None = None


class TaskResponse(BaseModel):
    task_id: str
    existed: bool = False


class FileInfo(BaseModel):
    filename: str
    size: int
    download_url: str


class TaskStatus(BaseModel):
    task_id: str
    status: str
    progress: float | None = None
    speed: str | None = None
    total_size: str | None = None
    error: str | None = None
    files: list[FileInfo] | None = None
    info: dict[str, Any] | None = None


def format_size(bytes_val: float) -> str:
    """格式化文件大小"""
    if bytes_val >= 1024 ** 3:
        return f"{bytes_val / (1024 ** 3):.2f} GB"
    elif bytes_val >= 1024 ** 2:
        return f"{bytes_val / (1024 ** 2):.2f} MB"
    elif bytes_val >= 1024:
        return f"{bytes_val / 1024:.2f} KB"
    return f"{bytes_val:.0f} B"


def progress_hook(task_id: str):
    def hook(d: dict):
        # 检查是否被取消
        if task_id in cancelled_tasks:
            raise Exception("任务已取消")
        if task_id not in runtime_state:
            runtime_state[task_id] = {}
        if d["status"] == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate", 0)
            downloaded = d.get("downloaded_bytes", 0)
            speed = d.get("speed")
            if total > 0:
                runtime_state[task_id]["progress"] = (downloaded / total) * 100
                runtime_state[task_id]["total_bytes"] = total
            if speed:
                runtime_state[task_id]["speed"] = speed
        elif d["status"] == "finished":
            runtime_state[task_id]["progress"] = 100
    return hook


def download_task(task_id: str, url: str, user_params: dict[str, Any] | None):
    global active_count
    task_dir = TEMP_BASE_DIR / task_id
    task_dir.mkdir(parents=True, exist_ok=True)
    runtime_state[task_id] = {"progress": 0}

    db_update_status(task_id, "downloading", task_dir=str(task_dir))

    base_params = {
        "noplaylist": True,
        "retries": 5,
        "extractor_retries": 1,
        "nocheckcertificate": True,
        "concurrent_fragment_downloads": 8,
        "outtmpl": str(task_dir / "%(title).100s.%(ext)s"),
        "progress_hooks": [progress_hook(task_id)],
    }

    if user_params:
        user_params.pop("outtmpl", None)
        user_params.pop("progress_hooks", None)
        base_params.update(user_params)

    started_at = time.time()
    info = None
    try:
        with YoutubeDL(base_params) as ydl:
            extracted_info = ydl.extract_info(url, download=True)
            info = ydl.sanitize_info(extracted_info)

        files = list(task_dir.iterdir())
        if files:
            file_list = []
            for f in files:
                if f.is_file():
                    file_id = generate_file_id(task_id, f.name)
                    db_save_file_mapping(file_id, task_id, f.name, str(f))
                    file_list.append({
                        "filename": f.name,
                        "size": f.stat().st_size,
                        "download_url": f"/download/{file_id}"
                    })
            total_size = sum(f.stat().st_size for f in files if f.is_file())
            elapsed = time.time() - started_at
            if elapsed > 0 and total_size > 0:
                runtime_state[task_id]["speed"] = total_size / elapsed
                runtime_state[task_id]["total_bytes"] = total_size
            db_update_status(task_id, "completed", files=file_list, info=info, finished_at=time.time())
        else:
            db_update_status(task_id, "failed", error="下载完成但未找到文件", finished_at=time.time())
    except Exception as e:
        if task_id in cancelled_tasks:
            db_update_status(task_id, "cancelled", finished_at=time.time())
            cancelled_tasks.discard(task_id)
            if task_dir.exists():
                rmtree(task_dir)
        else:
            db_update_status(task_id, "failed", error=str(e), finished_at=time.time())
    finally:
        with count_lock:
            active_count -= 1
        try_start_pending()


@app.post("/tasks", response_model=TaskResponse)
async def create_task(request: DownloadRequest):
    """创建下载任务"""
    global active_count

    # 检查是否已存在相同URL的活跃任务
    existing_task_id = db_find_active_task_by_url(request.url)
    if existing_task_id:
        return TaskResponse(task_id=existing_task_id, existed=True)

    task_id = str(uuid.uuid4())
    task = {
        "status": "pending",
        "progress": 0,
        "error": None,
        "url": request.url,
        "params": request.params,
        "created_at": time.time(),
    }
    db_save_task(task_id, task)

    with count_lock:
        if active_count < MAX_CONCURRENT:
            active_count += 1
            executor.submit(download_task, task_id, request.url, request.params)

    return TaskResponse(task_id=task_id)


@app.get("/tasks/{task_id}", response_model=TaskStatus)
async def get_task_status(task_id: str):
    """查询任务状态"""
    task = db_get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")

    files = None
    if task.get("files"):
        files = [FileInfo(**f) for f in task["files"]]

    # 合并运行时状态
    rt = runtime_state.get(task_id, {})
    progress = rt.get("progress", task.get("progress"))
    speed = None
    if rt.get("speed"):
        speed = format_size(rt["speed"]) + "/s"
    total_size = None
    if rt.get("total_bytes"):
        total_size = format_size(rt["total_bytes"])

    return TaskStatus(
        task_id=task_id,
        status=task["status"],
        progress=progress,
        speed=speed,
        total_size=total_size,
        error=task.get("error"),
        files=files,
        info=task.get("info"),
    )


@app.get("/download/{file_id}")
async def download_file(file_id: str):
    """下载文件"""
    mapping = db_get_file_mapping(file_id)
    if not mapping:
        raise HTTPException(status_code=404, detail="文件不存在")

    task = db_get_task(mapping["task_id"])
    if not task or task["status"] != "completed":
        raise HTTPException(status_code=400, detail="任务未完成")

    filepath = Path(mapping["filepath"])
    if not filepath.exists() or not filepath.is_file():
        raise HTTPException(status_code=404, detail="文件不存在")

    return FileResponse(str(filepath), filename=mapping["filename"], media_type="application/octet-stream")


@app.delete("/tasks/{task_id}")
async def delete_task(task_id: str):
    """删除任务及其临时文件"""
    task = db_get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")

    task_dir = task.get("task_dir")
    if task_dir and Path(task_dir).exists():
        rmtree(task_dir)

    db_delete_task(task_id)
    runtime_state.pop(task_id, None)
    cancelled_tasks.discard(task_id)
    return {"message": "任务已删除"}


@app.post("/tasks/{task_id}/cancel")
async def cancel_task(task_id: str):
    """取消任务"""
    task = db_get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")

    status = task["status"]
    if status in ("completed", "failed", "cancelled"):
        raise HTTPException(status_code=400, detail=f"任务已{status}，无法取消")

    # 标记为取消
    cancelled_tasks.add(task_id)

    if status == "pending":
        # pending状态直接更新数据库
        db_update_status(task_id, "cancelled", finished_at=time.time())
        cancelled_tasks.discard(task_id)
    # downloading状态会在progress_hook中抛出异常

    return {"message": "任务取消请求已提交"}


@app.get("/settings/max-concurrent")
async def get_max_concurrent():
    """获取当前最大并发数"""
    return {"max_concurrent": MAX_CONCURRENT, "active_count": active_count}


@app.put("/settings/max-concurrent")
async def set_max_concurrent(value: int):
    """设置最大并发数"""
    global MAX_CONCURRENT
    if value < 1:
        raise HTTPException(status_code=400, detail="最大并发数必须大于0")
    MAX_CONCURRENT = value
    try_start_pending()
    return {"max_concurrent": MAX_CONCURRENT}


@app.get("/monitor")
async def get_monitor():
    """获取系统监控信息"""
    disk = shutil.disk_usage(TEMP_BASE_DIR)
    total_speed = sum(rt.get("speed", 0) for rt in runtime_state.values())
    return {
        "disk": {
            "total": format_size(disk.total),
            "used": format_size(disk.used),
            "free": format_size(disk.free),
            "percent": round(disk.used / disk.total * 100, 1),
        },
        "bandwidth": {
            "current_speed": format_size(total_speed) + "/s",
            "active_downloads": active_count,
        },
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
