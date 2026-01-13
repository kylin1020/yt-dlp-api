import asyncio
import os
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
from starlette.background import BackgroundTask
from pydantic import BaseModel
from yt_dlp import YoutubeDL

# 任务存储
tasks: dict[str, dict[str, Any]] = {}
downloading_tasks: set[str] = set()  # 正在被用户下载的任务

# 临时文件目录
TEMP_BASE_DIR = Path(os.getenv("TEMP_DIR", "/tmp/yt-dlp-downloads"))
TEMP_BASE_DIR.mkdir(parents=True, exist_ok=True)

# 并发控制
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT_DOWNLOADS", "64"))
executor = ThreadPoolExecutor(max_workers=MAX_CONCURRENT)
active_count = 0
count_lock = Lock()

# 任务清理配置（秒）
TASK_EXPIRE_SECONDS = int(os.getenv("TASK_EXPIRE_SECONDS", "300"))
CLEANUP_INTERVAL = int(os.getenv("CLEANUP_INTERVAL", "60"))


async def cleanup_expired_tasks():
    """后台协程：清理过期的已完成/失败任务"""
    while True:
        await asyncio.sleep(CLEANUP_INTERVAL)
        now = time.time()
        expired = [
            tid for tid, t in tasks.items()
            if t["status"] in ("completed", "failed")
            and tid not in downloading_tasks
            and now - t.get("finished_at", now) > TASK_EXPIRE_SECONDS
        ]
        for tid in expired:
            task_dir = tasks[tid].get("task_dir")
            if task_dir and Path(task_dir).exists():
                rmtree(task_dir)
            del tasks[tid]


@asynccontextmanager
async def lifespan(_app: FastAPI):
    task = asyncio.create_task(cleanup_expired_tasks())
    yield
    task.cancel()


app = FastAPI(title="yt-dlp API", lifespan=lifespan)


class DownloadRequest(BaseModel):
    url: str
    params: dict[str, Any] | None = None


class TaskResponse(BaseModel):
    task_id: str


class TaskStatus(BaseModel):
    task_id: str
    status: str  # pending, downloading, completed, failed
    progress: float | None = None
    error: str | None = None
    download_url: str | None = None
    filename: str | None = None


def progress_hook(task_id: str):
    def hook(d: dict):
        if d["status"] == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate", 0)
            downloaded = d.get("downloaded_bytes", 0)
            if total > 0:
                tasks[task_id]["progress"] = (downloaded / total) * 100
        elif d["status"] == "finished":
            tasks[task_id]["progress"] = 100
    return hook


def download_task(task_id: str, url: str, user_params: dict[str, Any] | None):
    global active_count
    task_dir = TEMP_BASE_DIR / task_id
    task_dir.mkdir(parents=True, exist_ok=True)

    tasks[task_id]["status"] = "downloading"
    tasks[task_id]["task_dir"] = str(task_dir)

    base_params = {
        "noplaylist": True,
        "retries": 5,
        "extractor_retries": 1,
        "nocheckcertificate": True,
        "concurrent_fragment_downloads": 8,
        "outtmpl": str(task_dir / "%(title).100s.%(ext)s"),
        "progress_hooks": [progress_hook(task_id)],
    }

    # 合并用户参数
    if user_params:
        user_params.pop("outtmpl", None)
        user_params.pop("progress_hooks", None)
        base_params.update(user_params)

    try:
        with YoutubeDL(base_params) as ydl:
            ydl.extract_info(url, download=True)

        # 查找下载的文件
        files = list(task_dir.iterdir())
        if files:
            downloaded_file = files[0]
            tasks[task_id]["status"] = "completed"
            tasks[task_id]["filename"] = downloaded_file.name
            tasks[task_id]["filepath"] = str(downloaded_file)
            tasks[task_id]["download_url"] = f"/download/{task_id}"
        else:
            tasks[task_id]["status"] = "failed"
            tasks[task_id]["error"] = "下载完成但未找到文件"
    except Exception as e:
        tasks[task_id]["status"] = "failed"
        tasks[task_id]["error"] = str(e)
    finally:
        tasks[task_id]["finished_at"] = time.time()
        with count_lock:
            active_count -= 1


@app.post("/tasks", response_model=TaskResponse)
async def create_task(request: DownloadRequest):
    """创建下载任务"""
    global active_count

    with count_lock:
        if active_count >= MAX_CONCURRENT:
            raise HTTPException(status_code=429, detail="下载任务已达上限，请稍后重试")
        active_count += 1

    task_id = str(uuid.uuid4())
    tasks[task_id] = {
        "status": "pending",
        "progress": 0,
        "error": None,
        "download_url": None,
        "filename": None,
    }
    executor.submit(download_task, task_id, request.url, request.params)
    return TaskResponse(task_id=task_id)


@app.get("/tasks/{task_id}", response_model=TaskStatus)
async def get_task_status(task_id: str):
    """查询任务状态"""
    if task_id not in tasks:
        raise HTTPException(status_code=404, detail="任务不存在")

    task = tasks[task_id]
    return TaskStatus(
        task_id=task_id,
        status=task["status"],
        progress=task.get("progress"),
        error=task.get("error"),
        download_url=task.get("download_url"),
        filename=task.get("filename"),
    )


@app.get("/download/{task_id}")
async def download_file(task_id: str):
    """下载文件"""
    if task_id not in tasks:
        raise HTTPException(status_code=404, detail="任务不存在")

    task = tasks[task_id]
    if task["status"] != "completed":
        raise HTTPException(status_code=400, detail="任务未完成")

    filepath = task.get("filepath")
    if not filepath or not Path(filepath).exists():
        raise HTTPException(status_code=404, detail="文件不存在")

    downloading_tasks.add(task_id)
    return FileResponse(
        filepath,
        filename=task["filename"],
        media_type="application/octet-stream",
        background=BackgroundTask(downloading_tasks.discard, task_id),
    )


@app.delete("/tasks/{task_id}")
async def delete_task(task_id: str):
    """删除任务及其临时文件"""
    if task_id not in tasks:
        raise HTTPException(status_code=404, detail="任务不存在")

    task = tasks[task_id]
    task_dir = task.get("task_dir")

    if task_dir and Path(task_dir).exists():
        rmtree(task_dir)

    del tasks[task_id]
    return {"message": "任务已删除"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
