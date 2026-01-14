import asyncio
import hashlib
import json
import multiprocessing as mp
import os
import shutil
import sqlite3
import time
import uuid
from multiprocessing import Process, Queue
from pathlib import Path
from shutil import rmtree
from typing import Any

import psutil
from dotenv import load_dotenv

load_dotenv()

# R2 配置
R2_ENABLED = os.getenv("R2_ENABLED", "false").lower() == "true"
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID", "")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY", "")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY", "")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME", "")
R2_PUBLIC_DOMAIN = os.getenv("R2_PUBLIC_DOMAIN", "")  # 如 https://dl.yourdomain.com

# 初始化 R2 客户端
r2_client = None
if R2_ENABLED and R2_ACCOUNT_ID and R2_ACCESS_KEY and R2_SECRET_KEY and R2_BUCKET_NAME and R2_PUBLIC_DOMAIN:
    import boto3
    from boto3.s3.transfer import TransferConfig
    r2_client = boto3.client(
        "s3",
        endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
    )
    r2_transfer_config = TransferConfig(
        multipart_threshold=1024 * 1024 * 100,   # 100MB 开始分片
        multipart_chunksize=1024 * 1024 * 256,  # 256MB 分片大小
        max_concurrency=10,
        use_threads=True
    )

from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from fastapi.responses import FileResponse
from pydantic import BaseModel

# 临时文件目录
TEMP_BASE_DIR = Path(os.getenv("TEMP_DIR", "/tmp/yt-dlp-downloads"))
TEMP_BASE_DIR.mkdir(parents=True, exist_ok=True)

# 数据库路径
DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "tasks.db"

# 并发控制
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT_DOWNLOADS", "64"))
active_count = 0

# 进程间通信队列
task_queue: Queue = None      # 发送任务给worker
progress_queue: Queue = None  # 接收worker进度
cancel_set = None             # 取消标记
worker_process: Process = None

# 任务清理配置（秒）
TASK_EXPIRE_SECONDS = int(os.getenv("TASK_EXPIRE_SECONDS", "300"))
CLEANUP_INTERVAL = int(os.getenv("CLEANUP_INTERVAL", "60"))

# 运行时状态（不持久化）
runtime_state: dict[str, dict] = {}  # task_id -> {progress, speed, total_bytes}

# 网络速度监控
net_stats = {"last_bytes": 0, "last_time": 0.0, "speed": 0.0}


def init_db():
    """初始化数据库"""
    with sqlite3.connect(DB_PATH) as conn:
        # 启用 WAL 模式，提高多进程并发性能
        conn.execute("PRAGMA journal_mode=WAL")
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
                created_at REAL,
                format TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_url_format_status ON tasks(url, format, status)")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS file_mapping (
                file_id TEXT PRIMARY KEY,
                task_id TEXT,
                filename TEXT,
                filepath TEXT,
                r2_key TEXT
            )
        """)
        # 兼容旧表结构
        try:
            conn.execute("ALTER TABLE file_mapping ADD COLUMN r2_key TEXT")
        except sqlite3.OperationalError:
            pass


class DBConnection:
    """数据库连接上下文管理器，确保连接被正确关闭"""
    def __init__(self):
        self.conn = None

    def __enter__(self):
        import logging
        max_retries = 3
        retry_delay = 0.1

        for attempt in range(max_retries):
            try:
                DB_PATH.parent.mkdir(parents=True, exist_ok=True)
                self.conn = sqlite3.connect(str(DB_PATH), timeout=30, check_same_thread=False)
                self.conn.execute("PRAGMA busy_timeout = 30000")
                return self.conn
            except sqlite3.OperationalError as e:
                if attempt < max_retries - 1:
                    logging.warning(f"数据库连接失败(尝试 {attempt + 1}/{max_retries}): {e}, 重试中...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    logging.error(f"无法打开数据库文件: {DB_PATH}, 错误: {e}")
                    logging.error(f"数据库目录存在: {DB_PATH.parent.exists()}, 可写: {os.access(DB_PATH.parent, os.W_OK)}")
                    if DB_PATH.exists():
                        logging.error(f"数据库文件存在: True, 可读: {os.access(DB_PATH, os.R_OK)}, 可写: {os.access(DB_PATH, os.W_OK)}")
                    raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            try:
                if exc_type is None:
                    self.conn.commit()
                else:
                    self.conn.rollback()
            finally:
                self.conn.close()
        return False

def _get_db_conn():
    """获取数据库连接上下文管理器"""
    return DBConnection()


def db_get_task(task_id: str) -> dict | None:
    """从数据库获取单个任务"""
    with _get_db_conn() as conn:
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
    format_val = (task.get("params") or {}).get("format")
    with _get_db_conn() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO tasks (task_id, status, progress, error, task_dir, finished_at, files, info, url, params, created_at, format)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (task_id, task.get("status"), task.get("progress"), task.get("error"),
              task.get("task_dir"), task.get("finished_at"), files_json, info_json,
              task.get("url"), params_json, task.get("created_at"), format_val))


def db_update_status(task_id: str, status: str, **kwargs):
    """更新任务状态"""
    with _get_db_conn() as conn:
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
    with _get_db_conn() as conn:
        conn.execute("DELETE FROM file_mapping WHERE task_id = ?", (task_id,))
        conn.execute("DELETE FROM tasks WHERE task_id = ?", (task_id,))


def db_get_pending_tasks() -> list[tuple[str, str, dict | None]]:
    """获取所有等待中的任务，按创建时间排序"""
    with _get_db_conn() as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT task_id, url, params FROM tasks WHERE status = 'pending' ORDER BY created_at"
        ).fetchall()
        return [(r["task_id"], r["url"], json.loads(r["params"]) if r["params"] else None) for r in rows]


def db_get_expired_tasks() -> list[tuple[str, str | None]]:
    """获取过期的已完成/失败任务"""
    cutoff = time.time() - TASK_EXPIRE_SECONDS
    with _get_db_conn() as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT task_id, task_dir FROM tasks WHERE status IN ('completed', 'failed') AND finished_at < ?",
            (cutoff,)
        ).fetchall()
        return [(r["task_id"], r["task_dir"]) for r in rows]


def db_get_all_task_ids() -> set[str]:
    """获取所有任务ID"""
    with _get_db_conn() as conn:
        return {r[0] for r in conn.execute("SELECT task_id FROM tasks").fetchall()}


def db_save_file_mapping(file_id: str, task_id: str, filename: str, filepath: str, r2_key: str = None):
    """保存文件映射"""
    with _get_db_conn() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO file_mapping (file_id, task_id, filename, filepath, r2_key) VALUES (?, ?, ?, ?, ?)",
            (file_id, task_id, filename, filepath, r2_key)
        )


def db_get_file_mapping(file_id: str) -> dict | None:
    """获取文件映射"""
    with _get_db_conn() as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM file_mapping WHERE file_id = ?", (file_id,)).fetchone()
        if not row:
            return None
        return {"task_id": row["task_id"], "filename": row["filename"], "filepath": row["filepath"]}


def db_get_r2_keys_by_task(task_id: str) -> list[str]:
    """获取任务的所有R2 keys"""
    with _get_db_conn() as conn:
        rows = conn.execute("SELECT r2_key FROM file_mapping WHERE task_id = ? AND r2_key IS NOT NULL", (task_id,)).fetchall()
        return [r[0] for r in rows]


def db_delete_file_mappings_by_task(task_id: str):
    """删除任务的所有文件映射"""
    with _get_db_conn() as conn:
        conn.execute("DELETE FROM file_mapping WHERE task_id = ?", (task_id,))


def db_find_reusable_task(url: str, params: dict | None) -> str | None:
    """查找可复用的任务（相同URL和format参数）"""
    format_val = (params or {}).get("format")
    with _get_db_conn() as conn:
        row = conn.execute(
            "SELECT task_id FROM tasks WHERE url = ? AND format IS ? AND status IN ('pending', 'downloading', 'completed')",
            (url, format_val)
        ).fetchone()
        return row[0] if row else None


def generate_file_id(task_id: str, filename: str) -> str:
    """生成文件ID（基于task_id和filename的hash）"""
    return hashlib.md5(f"{task_id}:{filename}".encode()).hexdigest()


def submit_task(task_id: str, url: str, params: dict | None):
    """提交任务到worker进程"""
    task_queue.put({"task_id": task_id, "url": url, "params": params})


def start_pending_tasks():
    """启动所有pending任务"""
    pending = db_get_pending_tasks()
    for task_id, url, params in pending:
        submit_task(task_id, url, params)


def _cleanup_task(task_id: str, task_dir: str | None):
    """同步清理单个任务（在线程池中执行）"""
    if task_dir and Path(task_dir).exists():
        rmtree(task_dir)
    if r2_client:
        try:
            r2_keys = db_get_r2_keys_by_task(task_id)
            if r2_keys:
                r2_client.delete_objects(Bucket=R2_BUCKET_NAME, Delete={"Objects": [{"Key": k} for k in r2_keys]})
        except Exception:
            pass
    db_delete_file_mappings_by_task(task_id)
    db_delete_task(task_id)


async def cleanup_expired_tasks():
    """后台协程：清理过期任务"""
    loop = asyncio.get_event_loop()
    while True:
        await asyncio.sleep(CLEANUP_INTERVAL)
        expired = await loop.run_in_executor(None, db_get_expired_tasks)
        for task_id, task_dir in expired:
            await loop.run_in_executor(None, _cleanup_task, task_id, task_dir)
            runtime_state.pop(task_id, None)


def cleanup_orphan_dirs():
    """清理孤立的任务目录"""
    db_task_ids = db_get_all_task_ids()
    for item in TEMP_BASE_DIR.iterdir():
        if item.is_dir() and item.name not in db_task_ids:
            rmtree(item)


async def progress_listener():
    """后台协程：监听worker进程的进度更新"""
    global active_count
    loop = asyncio.get_event_loop()
    while True:
        try:
            msg = await loop.run_in_executor(None, lambda: progress_queue.get(timeout=0.1))
            task_id = msg.get("task_id")
            if msg.get("type") == "done":
                active_count = max(0, active_count - 1)
            elif msg.get("type") == "progress":
                if task_id not in runtime_state:
                    runtime_state[task_id] = {}
                for key in ("progress", "speed", "total_bytes", "downloaded_bytes"):
                    if key in msg:
                        runtime_state[task_id][key] = msg[key]
        except:
            await asyncio.sleep(0.05)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global task_queue, progress_queue, cancel_set, worker_process
    manager = mp.Manager()
    task_queue = manager.Queue()
    progress_queue = manager.Queue()
    cancel_set = manager.dict()

    init_db()
    # 将 downloading/uploading 状态重置为 pending（服务重启时恢复）
    with _get_db_conn() as conn:
        conn.execute("UPDATE tasks SET status = 'pending', progress = 0 WHERE status IN ('downloading', 'uploading')")
    cleanup_orphan_dirs()

    # 启动单一后台worker进程
    from download_worker import worker_process as worker_func
    worker_config = {"max_concurrent": MAX_CONCURRENT}
    worker_process = Process(target=worker_func, args=(task_queue, progress_queue, cancel_set, worker_config))
    worker_process.start()

    # 启动pending任务
    start_pending_tasks()

    cleanup_task = asyncio.create_task(cleanup_expired_tasks())
    progress_task = asyncio.create_task(progress_listener())
    yield
    cleanup_task.cancel()
    progress_task.cancel()

    # 停止worker进程
    task_queue.put(None)  # 发送退出信号
    worker_process.join(timeout=5)
    if worker_process.is_alive():
        worker_process.terminate()
    manager.shutdown()


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
    eta: str | None = None  # 预估剩余时间
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


def format_eta(seconds: float) -> str:
    """格式化剩余时间"""
    if seconds < 60:
        return f"{int(seconds)}秒"
    elif seconds < 3600:
        return f"{int(seconds // 60)}分{int(seconds % 60)}秒"
    else:
        h, m = divmod(int(seconds), 3600)
        return f"{h}时{m // 60}分"


@app.post("/tasks", response_model=TaskResponse)
async def create_task(request: DownloadRequest):
    """创建下载任务"""
    global active_count

    # 检查是否已存在可复用的任务（相同URL和format）
    existing_task_id = db_find_reusable_task(request.url, request.params)
    if existing_task_id:
        return TaskResponse(task_id=existing_task_id, existed=True)

    task_id = uuid.uuid4().hex
    task = {
        "status": "pending",
        "progress": 0,
        "error": None,
        "url": request.url,
        "params": request.params,
        "created_at": time.time(),
    }
    db_save_task(task_id, task)

    # 提交任务到worker
    active_count += 1
    submit_task(task_id, request.url, request.params)

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
    speed_val = rt.get("speed")
    if speed_val:
        speed = format_size(speed_val) + "/s"
    total_size = None
    total_bytes = rt.get("total_bytes")
    if total_bytes:
        total_size = format_size(total_bytes)

    # 计算 ETA
    eta = None
    downloaded = rt.get("downloaded_bytes", 0)
    if speed_val and total_bytes and downloaded < total_bytes:
        remaining = total_bytes - downloaded
        eta = format_eta(remaining / speed_val)

    return TaskStatus(
        task_id=task_id,
        status=task["status"],
        progress=progress,
        speed=speed,
        total_size=total_size,
        eta=eta,
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


def _delete_task_sync(task_id: str, task_dir: str | None):
    """同步删除任务（在线程池中执行）"""
    if task_dir and Path(task_dir).exists():
        rmtree(task_dir)
    if r2_client:
        try:
            r2_keys = db_get_r2_keys_by_task(task_id)
            if r2_keys:
                r2_client.delete_objects(Bucket=R2_BUCKET_NAME, Delete={"Objects": [{"Key": k} for k in r2_keys]})
        except Exception:
            pass
    db_delete_file_mappings_by_task(task_id)
    db_delete_task(task_id)


@app.delete("/tasks/{task_id}")
async def delete_task(task_id: str):
    """删除任务及其临时文件"""
    task = db_get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")

    task_dir = task.get("task_dir")
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _delete_task_sync, task_id, task_dir)
    runtime_state.pop(task_id, None)
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

    if status == "pending":
        db_update_status(task_id, "cancelled", finished_at=time.time())
    else:
        # 通知worker取消
        cancel_set[task_id] = True

    return {"message": "任务取消请求已提交"}


@app.get("/settings/max-concurrent")
async def get_max_concurrent():
    """获取当前最大并发数"""
    return {"max_concurrent": MAX_CONCURRENT, "active_count": active_count}


@app.put("/settings/max-concurrent")
async def set_max_concurrent(value: int):
    """动态设置最大并发数"""
    global MAX_CONCURRENT
    if value < 1:
        raise HTTPException(status_code=400, detail="最大并发数必须大于0")
    MAX_CONCURRENT = value
    # 通知worker进程更新并发数
    task_queue.put({"type": "config", "max_concurrent": value})
    return {"max_concurrent": MAX_CONCURRENT}


@app.get("/monitor")
async def get_monitor():
    """获取系统监控信息"""
    disk = shutil.disk_usage(TEMP_BASE_DIR)

    # 使用 psutil 获取系统网络速度
    net_io = psutil.net_io_counters()
    current_bytes = net_io.bytes_recv
    current_time = time.time()

    speed = 0.0
    if net_stats["last_time"] > 0:
        time_diff = current_time - net_stats["last_time"]
        if time_diff > 0:
            speed = (current_bytes - net_stats["last_bytes"]) / time_diff

    net_stats["last_bytes"] = current_bytes
    net_stats["last_time"] = current_time
    net_stats["speed"] = speed

    # 计算预估空闲时间（所有任务中最长的剩余时间）
    max_eta_seconds = 0
    for rt in runtime_state.values():
        speed = rt.get("speed", 0)
        total = rt.get("total_bytes", 0)
        downloaded = rt.get("downloaded_bytes", 0)
        if speed > 0 and total > downloaded:
            eta_seconds = (total - downloaded) / speed
            max_eta_seconds = max(max_eta_seconds, eta_seconds)

    return {
        "disk": {
            "total": format_size(disk.total),
            "used": format_size(disk.used),
            "free": format_size(disk.free),
            "percent": round(disk.used / disk.total * 100, 1),
        },
        "bandwidth": {
            "current_speed": format_size(speed) + "/s",
            "active_downloads": active_count,
            "max_concurrent": MAX_CONCURRENT,
            "estimated_idle": format_eta(max_eta_seconds) if max_eta_seconds > 0 else "空闲",
        },
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
