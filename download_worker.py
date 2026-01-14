"""子进程下载模块 - 在独立进程中执行 yt-dlp 下载，避免 GIL 阻塞主进程"""
import hashlib
import json
import os
import sqlite3
import time
from multiprocessing import Queue
from pathlib import Path
from shutil import rmtree
from typing import Any
from urllib.parse import quote

from dotenv import load_dotenv
from yt_dlp import YoutubeDL

load_dotenv()

# R2 配置
R2_ENABLED = os.getenv("R2_ENABLED", "false").lower() == "true"
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID", "")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY", "")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY", "")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME", "")
R2_PUBLIC_DOMAIN = os.getenv("R2_PUBLIC_DOMAIN", "")

# 临时文件目录
TEMP_BASE_DIR = Path(os.getenv("TEMP_DIR", "/tmp/yt-dlp-downloads"))

# 数据库路径
DATA_DIR = Path(__file__).parent / "data"
DB_PATH = DATA_DIR / "tasks.db"


def _get_db_conn():
    """获取数据库连接，设置 timeout 避免锁冲突"""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _init_r2_client():
    """初始化 R2 客户端"""
    if R2_ENABLED and R2_ACCOUNT_ID and R2_ACCESS_KEY and R2_SECRET_KEY and R2_BUCKET_NAME and R2_PUBLIC_DOMAIN:
        import boto3
        from boto3.s3.transfer import TransferConfig
        client = boto3.client(
            "s3",
            endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
            aws_access_key_id=R2_ACCESS_KEY,
            aws_secret_access_key=R2_SECRET_KEY,
        )
        config = TransferConfig(
            multipart_threshold=1024 * 1024 * 100,
            multipart_chunksize=1024 * 1024 * 256,
            max_concurrency=10,
            use_threads=True
        )
        return client, config
    return None, None


def _db_update_status(task_id: str, status: str, **kwargs):
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


def _db_save_file_mapping(file_id: str, task_id: str, filename: str, filepath: str, r2_key: str = None):
    """保存文件映射"""
    with _get_db_conn() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO file_mapping (file_id, task_id, filename, filepath, r2_key) VALUES (?, ?, ?, ?, ?)",
            (file_id, task_id, filename, filepath, r2_key)
        )


def _generate_file_id(task_id: str, filename: str) -> str:
    """生成文件ID"""
    return hashlib.md5(f"{task_id}:{filename}".encode()).hexdigest()


def download_in_subprocess(task_id: str, url: str, user_params: dict[str, Any] | None,
                           progress_queue: Queue, cancel_set: dict):
    """在子进程中执行下载任务"""
    task_dir = TEMP_BASE_DIR / task_id
    task_dir.mkdir(parents=True, exist_ok=True)

    r2_client, r2_transfer_config = _init_r2_client()

    def check_cancelled():
        return cancel_set.get(task_id, False)

    def progress_hook(d: dict):
        if check_cancelled():
            raise Exception("任务已取消")
        if d["status"] == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate", 0)
            downloaded = d.get("downloaded_bytes", 0)
            speed = d.get("speed")
            progress_queue.put({
                "task_id": task_id,
                "type": "progress",
                "progress": (downloaded / total * 100) if total > 0 else 0,
                "total_bytes": total,
                "downloaded_bytes": downloaded,
                "speed": speed or 0
            })
        elif d["status"] == "finished":
            progress_queue.put({
                "task_id": task_id,
                "type": "progress",
                "progress": 100
            })

    _db_update_status(task_id, "downloading", task_dir=str(task_dir))

    base_params = {
        "noplaylist": True,
        "retries": 5,
        "extractor_retries": 1,
        "nocheckcertificate": True,
        "concurrent_fragment_downloads": 8,
        "outtmpl": str(task_dir / "%(title).100s.%(ext)s"),
        "progress_hooks": [progress_hook]
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
            if r2_client:
                _db_update_status(task_id, "uploading")
                progress_queue.put({"task_id": task_id, "type": "progress", "progress": 0})

            file_count = len([x for x in files if x.is_file()])
            for idx, f in enumerate(files):
                if f.is_file():
                    file_id = _generate_file_id(task_id, f.name)
                    file_size = f.stat().st_size

                    if r2_client:
                        ext = f.suffix
                        hash_name = f"{file_id}{ext}"
                        object_key = f"{task_id}/{hash_name}"
                        r2_client.upload_file(
                            str(f), R2_BUCKET_NAME, object_key,
                            Config=r2_transfer_config,
                            ExtraArgs={"ContentDisposition": f"attachment; filename*=UTF-8''{quote(f.name)}"}
                        )
                        _db_save_file_mapping(file_id, task_id, f.name, str(f), r2_key=object_key)
                        download_url = f"{R2_PUBLIC_DOMAIN.rstrip('/')}/{object_key}"
                        progress_queue.put({
                            "task_id": task_id,
                            "type": "progress",
                            "progress": ((idx + 1) / file_count) * 100
                        })
                    else:
                        _db_save_file_mapping(file_id, task_id, f.name, str(f))
                        download_url = f"/download/{file_id}"

                    file_list.append({
                        "filename": f.name,
                        "size": file_size,
                        "download_url": download_url
                    })

            total_size = sum(f.stat().st_size for f in files if f.is_file())
            elapsed = time.time() - started_at
            if elapsed > 0 and total_size > 0:
                progress_queue.put({
                    "task_id": task_id,
                    "type": "progress",
                    "speed": total_size / elapsed,
                    "total_bytes": total_size
                })

            _db_update_status(task_id, "completed", files=file_list, info=info, finished_at=time.time())

            if r2_client and task_dir.exists():
                rmtree(task_dir)
        else:
            _db_update_status(task_id, "failed", error="下载完成但未找到文件", finished_at=time.time())

    except Exception as e:
        if check_cancelled():
            _db_update_status(task_id, "cancelled", finished_at=time.time())
            if task_dir.exists():
                rmtree(task_dir)
        else:
            _db_update_status(task_id, "failed", error=str(e), finished_at=time.time())

    finally:
        progress_queue.put({"task_id": task_id, "type": "done"})
        # 清理取消标记
        cancel_set.pop(task_id, None)
