"""
yt-dlp API 异步客户端 SDK
用于管理视频下载任务并显示下载进度
"""

import asyncio
import os
import random
import shutil
import sys
import tempfile
import time
from dataclasses import dataclass, field
from typing import Optional, Union

import httpx
from parfive import Downloader


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


class RateLimitError(YtDlpClientError):
    """请求频率限制（429）"""
    def __init__(self, retry_after: float = 60.0):
        self.retry_after = retry_after
        super().__init__(f"请求频率限制，需等待 {retry_after} 秒")


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
    eta: str = ""  # 预估剩余时间
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
    """yt-dlp API 异步客户端（支持多服务器负载均衡和故障转移）"""

    DEFAULT_DOWNLOAD_CONCURRENT = 2  # 文件下载默认并发数，避免占满带宽
    DEFAULT_COOLDOWN = 60.0  # 服务器冷却时间（秒）

    def __init__(
        self,
        base_urls: Union[str, list[str]] = "http://localhost:8000",
        timeout: float = 30.0,
        download_concurrent: int = 2,
        retry_attempts: int = 3,
        cooldown_time: float = 5.0,
    ):
        # 支持单个 URL、逗号分隔的字符串或 URL 列表
        if isinstance(base_urls, str):
            base_urls = [u.strip() for u in base_urls.split(",")]
        self.base_urls = [url.rstrip("/") for url in base_urls]
        self.timeout = timeout
        self.download_concurrent = download_concurrent
        self.retry_attempts = retry_attempts
        self.cooldown_time = cooldown_time
        self._client: Optional[httpx.AsyncClient] = None
        # 服务器状态：{url: 可用时间戳}，0 表示可用
        self._server_cooldown: dict[str, float] = {url: 0 for url in self.base_urls}

    @property
    def base_url(self) -> str:
        """兼容旧代码，返回当前可用的服务器"""
        return self._get_available_server() or self.base_urls[0]

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

    def _get_available_server(self) -> Optional[str]:
        """获取一个可用的服务器（随机选择以实现负载均衡）"""
        now = time.time()
        available = [url for url, cooldown in self._server_cooldown.items() if cooldown <= now]
        return random.choice(available) if available else None

    def _mark_server_unavailable(self, url: str, cooldown: float = None):
        """标记服务器暂时不可用"""
        self._server_cooldown[url] = time.time() + (cooldown or self.cooldown_time)

    def _mark_server_available(self, url: str):
        """标记服务器可用"""
        self._server_cooldown[url] = 0

    async def _wait_for_server(self) -> str:
        """等待直到有服务器可用"""
        while True:
            server = self._get_available_server()
            if server:
                return server
            # 找出最快恢复的服务器
            min_wait = min(self._server_cooldown.values()) - time.time()
            if min_wait > 0:
                await asyncio.sleep(min(min_wait, 5.0))  # 最多等 5 秒后重新检查

    async def _request(self, method: str, path: str, **kwargs) -> dict:
        """统一请求处理（自动故障转移和重试）"""
        last_error = None
        tried_servers = set()

        for attempt in range(self.retry_attempts):
            server = self._get_available_server()
            if not server:
                server = await self._wait_for_server()

            tried_servers.add(server)
            try:
                resp = await self.client.request(method, f"{server}{path}", **kwargs)

                if resp.status_code == 404:
                    raise TaskNotFoundError(path.split("/")[-1])

                if resp.status_code == 429:
                    # 获取 Retry-After 头，默认 60 秒
                    retry_after = float(resp.headers.get("Retry-After", self.cooldown_time))
                    self._mark_server_unavailable(server, retry_after)
                    last_error = RateLimitError(retry_after)
                    continue

                if resp.status_code >= 500:
                    self._mark_server_unavailable(server)
                    last_error = NetworkError(f"服务器错误 [{resp.status_code}]")
                    continue

                if resp.status_code >= 400:
                    detail = resp.json().get("detail", resp.text) if resp.text else "未知错误"
                    raise ApiError(resp.status_code, detail)

                # 请求成功，确保服务器标记为可用
                self._mark_server_available(server)
                return resp.json()

            except httpx.RequestError as e:
                self._mark_server_unavailable(server)
                last_error = NetworkError(f"网络请求失败: {e}")
                continue

        # 所有重试都失败
        raise last_error or NetworkError("所有服务器都不可用")

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
            total_size=data.get("total_size", ""), eta=data.get("eta", ""),
            error=data.get("error", ""), files=files, info=data.get("info")
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
        """使用 parfive 下载单个文件到本地（先下载到临时目录再移动，支持故障转移）"""
        filename = os.path.basename(save_path)

        with tempfile.TemporaryDirectory() as tmp_dir:
            for _ in range(self.retry_attempts):
                server = self._get_available_server() or self.base_urls[0]
                url = f"{server}/download/{file_id}"

                dl = Downloader(max_conn=1, progress=show_progress)
                dl.enqueue_file(url, path=tmp_dir, filename=filename)
                results = dl.download()

                if not results.errors:
                    break
                self._mark_server_unavailable(server)
            else:
                raise FileDownloadError(filename, results.errors)

            tmp_path = os.path.join(tmp_dir, filename)
            os.makedirs(os.path.dirname(save_path) or ".", exist_ok=True)
            shutil.move(tmp_path, save_path)
        return save_path

    def download_files(
        self, files: list[tuple[str, str]], show_progress: bool = True,
        max_retries: int = 3
    ) -> list[str]:
        """
        批量下载文件（同步方法，带进度显示）
        files: [(file_id, save_path), ...]
        先下载到临时目录，全部完成后再移动到目标位置
        支持多服务器故障转移
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_files = []
            pending = []
            # 初始使用第一个可用服务器
            server = self._get_available_server() or self.base_urls[0]
            for file_id, save_path in files:
                url = f"{server}/download/{file_id}"
                filename = os.path.basename(save_path)
                tmp_files.append((os.path.join(tmp_dir, filename), save_path, file_id))
                pending.append((url, tmp_dir, filename, file_id))

            for attempt in range(max_retries):
                dl = Downloader(max_conn=self.download_concurrent, progress=show_progress)
                for url, path, filename, _ in pending:
                    dl.enqueue_file(url, path=path, filename=filename)
                results = dl.download()

                if not results.errors:
                    break

                # 标记当前服务器不可用，切换到其他服务器
                self._mark_server_unavailable(server)
                server = self._get_available_server() or self.base_urls[0]

                # 筛选失败的文件，用新服务器重试
                failed_urls = {str(e.url) for e in results.errors}
                new_pending = []
                for url, path, filename, file_id in pending:
                    if url in failed_urls:
                        new_url = f"{server}/download/{file_id}"
                        new_pending.append((new_url, path, filename, file_id))
                pending = new_pending

                if show_progress:
                    print(f"\n重试 {len(pending)} 个失败文件 ({attempt + 1}/{max_retries})...")
            else:
                if results.errors:
                    raise FileDownloadError("batch", results.errors)

            # 全部下载成功后移动到目标位置
            for tmp_path, save_path, _ in tmp_files:
                os.makedirs(os.path.dirname(save_path) or ".", exist_ok=True)
                shutil.move(tmp_path, save_path)

        return [save_path for _, save_path, _ in tmp_files]

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
        if status.eta:
            info += f" | ETA: {status.eta}"
        sys.stdout.write(info.ljust(100))
        sys.stdout.flush()

    async def download(
        self, url: str, params: Optional[dict] = None,
        save_dir: str = ".", show_progress: bool = True
    ) -> list[str]:
        """一站式下载：创建任务、等待完成、下载文件。支持 Ctrl+C 取消任务。"""
        task_id = None
        try:
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
        except (KeyboardInterrupt, asyncio.CancelledError):
            if task_id:
                if show_progress:
                    print(f"\n检测到中断，正在取消任务 {task_id}...")
                try:
                    await self.cancel_task(task_id)
                    if show_progress:
                        print(f"任务 {task_id} 已取消")
                except Exception:
                    pass
            raise

    async def batch_download(
        self, urls: list[str], params: Optional[dict] = None, show_progress: bool = True
    ) -> list[TaskStatus]:
        """批量创建任务并等待全部完成。支持 Ctrl+C 取消所有任务。"""
        task_ids = []
        try:
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
        except (KeyboardInterrupt, asyncio.CancelledError):
            if task_ids and show_progress:
                print(f"\n检测到中断，正在取消 {len(task_ids)} 个任务...")
            for tid in task_ids:
                try:
                    await self.cancel_task(tid)
                except Exception:
                    pass
            if show_progress:
                print("所有任务已取消")
            raise


# ============ 使用示例 ============

if __name__ == "__main__":
    async def main():
        # 支持多服务器负载均衡和故障转移
        servers = [
            "http://localhost:8000",
            "http://localhost:8001",
            "http://backup-server:8000",
        ]
        async with AsyncYtDlpClient(servers) as client:
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
            except RateLimitError as e:
                print(f"请求频率限制: {e.retry_after}秒后重试")
            except ApiError as e:
                print(f"API错误 [{e.status_code}]: {e.message}")
            except YtDlpClientError as e:
                print(f"客户端错误: {e}")

    asyncio.run(main())
