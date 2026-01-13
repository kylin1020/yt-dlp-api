"""
yt-dlp API 异步客户端 SDK
用于管理视频下载任务并显示下载进度
"""

import asyncio
import os
import sys
from dataclasses import dataclass, field
from typing import Optional

import httpx
from parfive import Downloader
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


# ============ 异常定义 ============

class YtDlpClientError(Exception):
    """客户端基础异常"""
    pass


class TaskNotFoundError(YtDlpClientError):
    """任务不存在"""
    def __init__(self, task_id: str):
        self.task_id = task_id
        super().__init__(f"任务不存在: {task_id}")


class TaskFailedError(YtDlpClientError):
    """任务执行失败"""
    def __init__(self, task_id: str, error: str):
        self.task_id = task_id
        self.error = error
        super().__init__(f"任务失败 [{task_id}]: {error}")


class TaskCancelledError(YtDlpClientError):
    """任务已取消"""
    def __init__(self, task_id: str):
        self.task_id = task_id
        super().__init__(f"任务已取消: {task_id}")


class ApiError(YtDlpClientError):
    """API请求错误"""
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"API错误 [{status_code}]: {message}")


class FileDownloadError(YtDlpClientError):
    """文件下载失败"""
    def __init__(self, filename: str, errors: list):
        self.filename = filename
        self.errors = errors
        super().__init__(f"文件下载失败 [{filename}]: {errors}")


class NetworkError(YtDlpClientError):
    """网络请求错误（可重试）"""
    pass


# ============ 数据模型 ============

@dataclass
class TaskFile:
    """下载完成的文件信息"""
    filename: str
    size: int
    download_url: str
    file_id: str


@dataclass
class TaskStatus:
    """任务状态信息"""
    task_id: str
    status: str  # pending, downloading, completed, failed, cancelled
    progress: float = 0.0
    speed: str = ""
    total_size: str = ""
    error: str = ""
    files: list[TaskFile] = field(default_factory=list)
    info: Optional[dict] = None

    _STATUS_MAP = {
        "pending": "等待中", "downloading": "下载中", "completed": "已完成",
        "failed": "失败", "cancelled": "已取消"
    }

    @property
    def is_done(self) -> bool:
        return self.status in ("completed", "failed", "cancelled")

    @property
    def status_text(self) -> str:
        return self._STATUS_MAP.get(self.status, self.status)


# ============ 客户端 ============

class AsyncYtDlpClient:
    """yt-dlp API 异步客户端"""

    DEFAULT_DOWNLOAD_CONCURRENT = 2  # 文件下载默认并发数，避免占满带宽

    def __init__(self, base_url: str = "http://localhost:8000", timeout: float = 30.0,
                 download_concurrent: int = 2):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.download_concurrent = download_concurrent
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self):
        self._client = httpx.AsyncClient(timeout=self.timeout)
        return self

    async def __aexit__(self, *args):
        await self.close()

    @property
    def client(self) -> httpx.AsyncClient:
        if not self._client:
            self._client = httpx.AsyncClient(timeout=self.timeout)
        return self._client

    async def close(self):
        """关闭客户端连接"""
        if self._client:
            await self._client.aclose()
            self._client = None

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(NetworkError),
        reraise=True
    )
    async def _request(self, method: str, path: str, **kwargs) -> dict:
        """统一请求处理（自动重试网络错误）"""
        try:
            resp = await self.client.request(method, f"{self.base_url}{path}", **kwargs)
            if resp.status_code == 404:
                raise TaskNotFoundError(path.split("/")[-1])
            if resp.status_code >= 500:
                raise NetworkError(f"服务器错误 [{resp.status_code}]")
            if resp.status_code >= 400:
                detail = resp.json().get("detail", resp.text) if resp.text else "未知错误"
                raise ApiError(resp.status_code, detail)
            return resp.json()
        except httpx.RequestError as e:
            raise NetworkError(f"网络请求失败: {e}") from e

    async def create_task(self, url: str, params: Optional[dict] = None) -> tuple[str, bool]:
        """创建下载任务，返回 (task_id, existed)"""
        data = await self._request("POST", "/tasks", json={"url": url, "params": params or {}})
        return data["task_id"], data.get("existed", False)

    async def get_task(self, task_id: str) -> TaskStatus:
        """获取任务状态"""
        data = await self._request("GET", f"/tasks/{task_id}")
        files = [
            TaskFile(f["filename"], f["size"], f["download_url"], f["download_url"].split("/")[-1])
            for f in data.get("files") or []
        ]
        return TaskStatus(
            task_id=data["task_id"], status=data["status"],
            progress=data.get("progress", 0), speed=data.get("speed", ""),
            total_size=data.get("total_size", ""), error=data.get("error", ""),
            files=files, info=data.get("info")
        )

    async def cancel_task(self, task_id: str) -> str:
        """取消任务"""
        data = await self._request("POST", f"/tasks/{task_id}/cancel")
        return data["message"]

    async def delete_task(self, task_id: str) -> str:
        """删除任务"""
        data = await self._request("DELETE", f"/tasks/{task_id}")
        return data["message"]

    async def download_file(self, file_id: str, save_path: str, show_progress: bool = True) -> str:
        """使用 parfive 下载单个文件到本地"""
        url = f"{self.base_url}/download/{file_id}"
        save_dir = os.path.dirname(save_path) or "."
        filename = os.path.basename(save_path)

        dl = Downloader(max_conn=1, progress=show_progress)
        dl.enqueue_file(url, path=save_dir, filename=filename)
        results = dl.download()

        if results.errors:
            raise FileDownloadError(filename, results.errors)
        return save_path

    def download_files(
        self, files: list[tuple[str, str]], show_progress: bool = True
    ) -> list[str]:
        """
        批量下载文件（同步方法，带进度显示）
        files: [(file_id, save_path), ...]
        """
        dl = Downloader(max_conn=self.download_concurrent, progress=show_progress)
        for file_id, save_path in files:
            url = f"{self.base_url}/download/{file_id}"
            save_dir = os.path.dirname(save_path) or "."
            filename = os.path.basename(save_path)
            dl.enqueue_file(url, path=save_dir, filename=filename)

        results = dl.download()
        if results.errors:
            raise FileDownloadError("batch", results.errors)
        return [save_path for _, save_path in files]

    async def get_max_concurrent(self) -> tuple[int, int]:
        """获取最大并发数和当前活跃数"""
        data = await self._request("GET", "/settings/max-concurrent")
        return data["max_concurrent"], data["active_count"]

    async def set_max_concurrent(self, value: int) -> int:
        """设置最大并发数"""
        data = await self._request("PUT", "/settings/max-concurrent", json={"value": value})
        return data["max_concurrent"]

    async def get_monitor(self) -> dict:
        """获取系统监控信息"""
        return await self._request("GET", "/monitor")

    async def wait_for_task(
        self, task_id: str, show_progress: bool = True, poll_interval: float = 1.0
    ) -> TaskStatus:
        """等待任务完成，可选显示进度"""
        while True:
            status = await self.get_task(task_id)
            if show_progress:
                self._print_progress(status)
            if status.is_done:
                if show_progress:
                    print()
                return status
            await asyncio.sleep(poll_interval)

    def _print_progress(self, status: TaskStatus):
        """打印进度条"""
        filled = int(30 * status.progress / 100)
        bar = "█" * filled + "░" * (30 - filled)
        info = f"\r[{bar}] {status.progress:5.1f}% | {status.status_text}"
        if status.speed:
            info += f" | {status.speed}"
        if status.total_size:
            info += f" | {status.total_size}"
        sys.stdout.write(info.ljust(80))
        sys.stdout.flush()

    async def download(
        self, url: str, params: Optional[dict] = None,
        save_dir: str = ".", show_progress: bool = True
    ) -> list[str]:
        """一站式下载：创建任务、等待完成、下载文件"""
        task_id, existed = await self.create_task(url, params)
        if show_progress:
            print(f"{'任务已存在' if existed else '创建任务'}: {task_id}")

        status = await self.wait_for_task(task_id, show_progress)

        if status.status == "failed":
            raise TaskFailedError(task_id, status.error)
        if status.status == "cancelled":
            raise TaskCancelledError(task_id)

        # 使用 parfive 批量下载所有文件
        files_to_download = [
            (f.file_id, os.path.join(save_dir, f.filename))
            for f in status.files
        ]
        if show_progress:
            print(f"开始下载 {len(files_to_download)} 个文件...")
        return self.download_files(files_to_download, show_progress)

    async def batch_download(
        self, urls: list[str], params: Optional[dict] = None, show_progress: bool = True
    ) -> list[TaskStatus]:
        """批量创建任务并等待全部完成"""
        tasks = [await self.create_task(url, params) for url in urls]
        task_ids = [t[0] for t in tasks]

        if show_progress:
            print(f"创建了 {len(task_ids)} 个任务")

        while True:
            statuses = await asyncio.gather(*[self.get_task(tid) for tid in task_ids])
            if show_progress:
                done = sum(1 for s in statuses if s.is_done)
                downloading = sum(1 for s in statuses if s.status == "downloading")
                print(f"\r完成: {done}/{len(statuses)} | 下载中: {downloading}", end="")
            if all(s.is_done for s in statuses):
                if show_progress:
                    print()
                return statuses
            await asyncio.sleep(1)


# ============ 使用示例 ============

if __name__ == "__main__":
    async def main():
        async with AsyncYtDlpClient("http://localhost:8000") as client:
            try:
                # 一站式下载
                # files = await client.download("https://www.youtube.com/watch?v=xxx")

                # 批量下载
                # statuses = await client.batch_download(["https://...", "https://..."])

                monitor = await client.get_monitor()
                print(f"磁盘可用: {monitor['disk']['free']}")

            except TaskFailedError as e:
                print(f"下载失败: {e.error}")
            except TaskCancelledError as e:
                print(f"任务被取消: {e.task_id}")
            except FileDownloadError as e:
                print(f"文件下载失败: {e.filename} - {e.errors}")
            except ApiError as e:
                print(f"API错误 [{e.status_code}]: {e.message}")
            except YtDlpClientError as e:
                print(f"客户端错误: {e}")

    asyncio.run(main())
