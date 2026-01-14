"""单一后台下载进程 - 内部使用线程池并发下载"""
import hashlib
import json
import logging
import os
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from queue import Queue, Empty
from shutil import rmtree
from typing import Any
from urllib.parse import quote

import ffmpeg
from dotenv import load_dotenv
from yt_dlp import YoutubeDL

logger = logging.getLogger(__name__)

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

# 并发下载数 - 默认8，每个任务内部还有分片线程
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT_DOWNLOADS", "8"))

# 全局状态
_cancel_set: set[str] = set()
_cancel_lock = threading.Lock()


def _get_db_conn():
    """获取数据库连接"""
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


def _is_cancelled(task_id: str) -> bool:
    """检查任务是否被取消"""
    with _cancel_lock:
        return task_id in _cancel_set


def _mark_cancelled(task_id: str):
    """标记任务为取消"""
    with _cancel_lock:
        _cancel_set.add(task_id)


def _clear_cancelled(task_id: str):
    """清除取消标记"""
    with _cancel_lock:
        _cancel_set.discard(task_id)


def _check_file_valid(file_path: str) -> bool:
    """使用 ffmpeg 检查文件是否有效"""
    try:
        ffmpeg.probe(file_path)
        return True
    except ffmpeg.Error as e:
        stderr = e.stderr.decode() if e.stderr else ""
        logger.error(f"文件损坏 {file_path}: {stderr[:200]}")
        return False
    except FileNotFoundError:
        logger.error("ffprobe 未安装，跳过文件检查")
        return True  # ffprobe 不存在时跳过检查
    except Exception as e:
        logger.error(f"检查文件 {file_path} 失败: {str(e)[:100]}")
        return True  # 其他异常时跳过检查，避免误删


class DownloadWorker:
    """下载工作器 - 在主进程中运行，使用线程池"""

    def __init__(self, max_concurrent: int = MAX_CONCURRENT):
        self.max_concurrent = max_concurrent
        # 线程池大小直接等于最大并发数，不要设置过大
        self.executor = ThreadPoolExecutor(max_workers=max_concurrent)
        self.task_queue: Queue = Queue()
        self.progress_callbacks: list = []
        self._running = False
        self._worker_thread: threading.Thread | None = None

    def set_max_concurrent(self, value: int):
        """动态设置最大并发数 - 需要重启才能生效"""
        if value > 0:
            self.max_concurrent = value
            # 注意：ThreadPoolExecutor不支持动态调整，需要重启服务才能生效

    def add_progress_callback(self, callback):
        """添加进度回调"""
        self.progress_callbacks.append(callback)

    def _notify_progress(self, msg: dict):
        """通知进度更新"""
        for cb in self.progress_callbacks:
            try:
                cb(msg)
            except:
                pass

    def submit_task(self, task_id: str, url: str, params: dict | None):
        """提交下载任务"""
        self.task_queue.put({"task_id": task_id, "url": url, "params": params})

    def cancel_task(self, task_id: str):
        """取消任务"""
        _mark_cancelled(task_id)

    def start(self):
        """启动工作器"""
        self._running = True
        self._worker_thread = threading.Thread(target=self._run, daemon=True)
        self._worker_thread.start()

    def stop(self):
        """停止工作器"""
        self._running = False
        self.task_queue.put(None)  # 发送退出信号
        if self._worker_thread:
            self._worker_thread.join(timeout=5)
        self.executor.shutdown(wait=False, cancel_futures=True)

    def _run(self):
        """工作器主循环"""
        while self._running:
            try:
                msg = self.task_queue.get(timeout=0.5)
                if msg is None:
                    break
                task_id = msg["task_id"]
                url = msg["url"]
                params = msg.get("params")
                self.executor.submit(self._download_single_task, task_id, url, params)
            except Empty:
                continue
            except:
                pass

    def _download_single_task(self, task_id: str, url: str, user_params: dict[str, Any] | None):
        """执行单个下载任务"""
        task_dir = TEMP_BASE_DIR / task_id
        task_dir.mkdir(parents=True, exist_ok=True)

        r2_client, r2_transfer_config = _init_r2_client()

        def check_cancelled():
            return _is_cancelled(task_id)

        def progress_hook(d: dict):
            if check_cancelled():
                raise Exception("任务已取消")
            if d["status"] == "downloading":
                total = d.get("total_bytes") or d.get("total_bytes_estimate", 0)
                downloaded = d.get("downloaded_bytes", 0)
                speed = d.get("speed")
                self._notify_progress({
                    "task_id": task_id,
                    "type": "progress",
                    "progress": (downloaded / total * 100) if total > 0 else 0,
                    "total_bytes": total,
                    "downloaded_bytes": downloaded,
                    "speed": speed or 0
                })
            elif d["status"] == "finished":
                self._notify_progress({
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
            "concurrent_fragment_downloads": 4,  # 降低分片并发，避免线程爆炸
            "outtmpl": str(task_dir / "%(id)s.%(ext)s"),
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
                # 检查文件有效性
                valid_files = []
                for f in files:
                    if f.is_file():
                        if _check_file_valid(str(f)):
                            valid_files.append(f)
                        else:
                            logger.warning(f"文件损坏，已删除: {f.name}")
                            f.unlink()

                if not valid_files:
                    _db_update_status(task_id, "failed", error="下载的文件全部损坏", finished_at=time.time())
                    if task_dir.exists():
                        rmtree(task_dir)
                    return

                file_list = []
                if r2_client:
                    _db_update_status(task_id, "uploading")
                    self._notify_progress({"task_id": task_id, "type": "progress", "progress": 0})

                file_count = len(valid_files)
                for idx, f in enumerate(valid_files):
                    file_id = _generate_file_id(task_id, f.name)
                    file_size = f.stat().st_size

                    if r2_client:
                        object_key = f"{task_id}/{f.name}"
                        r2_client.upload_file(
                            str(f), R2_BUCKET_NAME, object_key,
                            Config=r2_transfer_config,
                            ExtraArgs={"ContentDisposition": f"attachment; filename*=UTF-8''{quote(f.name)}"}
                        )
                        _db_save_file_mapping(file_id, task_id, f.name, str(f), r2_key=object_key)
                        download_url = f"{R2_PUBLIC_DOMAIN.rstrip('/')}/{object_key}"
                        self._notify_progress({
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

                total_size = sum(f["size"] for f in file_list)
                elapsed = time.time() - started_at
                if elapsed > 0 and total_size > 0:
                    self._notify_progress({
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
            self._notify_progress({"task_id": task_id, "type": "done"})
            _clear_cancelled(task_id)
