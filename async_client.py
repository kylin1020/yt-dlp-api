"""
yt-dlp API 异步客户端 SDK
用于管理视频下载任务并显示下载进度
"""

import asyncio
import os
import shutil
import sys
import tempfile
import time
from dataclasses import dataclass, field
from typing import Optional, Union

import httpx
from parfive import Downloader
from tenacity import retry, stop_after_attempt, wait_exponential


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

    @property
    def is_external_url(self) -> bool:
        """判断是否为外部 URL（如 R2 CDN 链接）"""
        return self.download_url.startswith(("http://", "https://"))


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

    DEFAULT_DOWNLOAD_CONCURRENT = 8  # 文件下载默认并发数
    DEFAULT_COOLDOWN = 10.0  # 服务器冷却时间（秒）

    def __init__(
        self,
        base_urls: Union[str, list[str]] = "http://localhost:8000",
        timeout: float = 30.0,
        download_concurrent: int = 4,
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
        # 任务亲和性：{task_id: server_url}，记录任务创建在哪个服务器
        self._task_server: dict[str, str] = {}
        # 轮转索引
        self._round_robin_index: int = 0

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
        """获取一个可用的服务器（轮转方式实现负载均衡）"""
        now = time.time()
        n = len(self.base_urls)
        start_index = self._round_robin_index
        for i in range(n):
            idx = (start_index + i) % n
            server = self.base_urls[idx]
            if self._server_cooldown.get(server, 0) <= now:
                # 只有成功选中服务器后才更新索引，指向下一个位置
                self._round_robin_index = (idx + 1) % n
                return server
        return None

    def get_server(self, preferred: str = None) -> Optional[str]:
        """获取服务器，优先使用指定的服务器，否则轮转获取"""
        now = time.time()
        if preferred and self._server_cooldown.get(preferred, 0) <= now:
            return preferred
        return self._get_available_server()

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

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10), reraise=True)
    async def _request(self, method: str, path: str, preferred_server: str = None, **kwargs) -> tuple[dict, str]:
        """统一请求处理（自动故障转移和重试）

        返回: (响应数据, 实际使用的服务器URL)
        """
        last_error = None

        for _ in range(self.retry_attempts):
            # 优先使用指定的服务器（任务亲和性）
            if preferred_server and self._server_cooldown.get(preferred_server, 0) <= time.time():
                server = preferred_server
            else:
                server = self._get_available_server()
                if not server:
                    server = await self._wait_for_server()

            try:
                resp = await self.client.request(method, f"{server}{path}", **kwargs)

                if resp.status_code == 404:
                    raise TaskNotFoundError(path.split("/")[-1])

                if resp.status_code == 429:
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

                self._mark_server_available(server)
                return resp.json(), server

            except httpx.RequestError as e:
                self._mark_server_unavailable(server)
                last_error = NetworkError(f"网络请求失败: {e}")
                continue

        raise last_error or NetworkError("所有服务器都不可用")

    async def create_task(self, url: str, params: Optional[dict] = None) -> tuple[str, bool]:
        """创建下载任务，返回 (task_id, existed)"""
        data, server = await self._request("POST", "/tasks", json={"url": url, "params": params or {}})
        task_id = data["task_id"]
        # 记录任务创建在哪个服务器
        self._task_server[task_id] = server
        return task_id, data.get("existed", False)

    async def get_task(self, task_id: str) -> TaskStatus:
        """获取任务状态"""
        # 优先使用任务创建时的服务器
        preferred = self._task_server.get(task_id)
        data, _ = await self._request("GET", f"/tasks/{task_id}", preferred_server=preferred)
        files = [
            TaskFile(f["filename"], f["size"], f["download_url"])
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
        preferred = self._task_server.get(task_id)
        data, _ = await self._request("POST", f"/tasks/{task_id}/cancel", preferred_server=preferred)
        return data["message"]

    async def delete_task(self, task_id: str) -> str:
        """删除任务"""
        preferred = self._task_server.get(task_id)
        data, _ = await self._request("DELETE", f"/tasks/{task_id}", preferred_server=preferred)
        self._task_server.pop(task_id, None)
        return data["message"]

    async def download_file(self, file_id: str, save_path: str, show_progress: bool = True) -> str:
        """使用 parfive 下载单个文件到本地（先下载到临时目录再移动，支持故障转移）"""
        filename = os.path.basename(save_path)

        with tempfile.TemporaryDirectory() as tmp_dir:
            for _ in range(self.retry_attempts):
                server = self._get_available_server() or self.base_urls[0]
                url = f"{server}/download/{file_id}"

                dl = Downloader(max_conn=self.download_concurrent, progress=show_progress)
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

    async def download_files(
        self, files: list[tuple[str, str]], show_progress: bool = True,
        max_retries: int = 3
    ) -> list[str]:
        """
        批量下载文件（异步方法，带进度显示）
        files: [(url_or_file_id, save_path), ...]
        如果是完整 URL 则直接下载，否则通过服务器 API 下载
        先下载到临时目录，全部完成后再移动到目标位置
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_files = []
            pending = []
            server = self._get_available_server() or self.base_urls[0]

            for url_or_id, save_path in files:
                filename = os.path.basename(save_path)
                # 判断是完整 URL 还是 file_id
                if url_or_id.startswith(("http://", "https://")):
                    url = url_or_id
                    is_external = True
                else:
                    url = f"{server}/download/{url_or_id}"
                    is_external = False
                tmp_files.append((os.path.join(tmp_dir, filename), save_path, url_or_id, is_external))
                pending.append((url, tmp_dir, filename, url_or_id, is_external))

            for attempt in range(max_retries):
                dl = Downloader(max_conn=self.download_concurrent, progress=show_progress)
                for url, path, filename, _, _ in pending:
                    dl.enqueue_file(url, path=path, filename=filename)
                results = await dl.run_download()

                if not results.errors:
                    break

                # 标记当前服务器不可用，切换到其他服务器（仅对非外部 URL）
                self._mark_server_unavailable(server)
                server = self._get_available_server() or self.base_urls[0]

                # 筛选失败的文件重试
                failed_urls = {str(e.url) for e in results.errors}
                new_pending = []
                for url, path, filename, url_or_id, is_external in pending:
                    if url in failed_urls:
                        # 外部 URL 保持不变，内部 URL 切换服务器
                        new_url = url if is_external else f"{server}/download/{url_or_id}"
                        new_pending.append((new_url, path, filename, url_or_id, is_external))
                pending = new_pending

                if show_progress:
                    print(f"\n重试 {len(pending)} 个失败文件 ({attempt + 1}/{max_retries})...")
            else:
                if results.errors:
                    raise FileDownloadError("batch", results.errors)

            # 全部下载成功后移动到目标位置
            for tmp_path, save_path, _, _ in tmp_files:
                os.makedirs(os.path.dirname(save_path) or ".", exist_ok=True)
                shutil.move(tmp_path, save_path)

        return [save_path for _, save_path, _, _ in tmp_files]

    async def get_max_concurrent(self) -> tuple[int, int]:
        """获取最大并发数和当前活跃数"""
        data, _ = await self._request("GET", "/settings/max-concurrent")
        return data["max_concurrent"], data["active_count"]

    async def set_max_concurrent(self, value: int) -> int:
        """设置最大并发数"""
        data, _ = await self._request("PUT", "/settings/max-concurrent", json={"value": value})
        return data["max_concurrent"]

    async def get_monitor(self) -> dict:
        """获取系统监控信息"""
        data, _ = await self._request("GET", "/monitor")
        return data

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
        save_dir: str = ".", show_progress: bool = True,
        server: str = None
    ) -> tuple[TaskStatus, list[str]]:
        """一站式下载：创建任务、等待完成、下载文件。支持 Ctrl+C 取消任务。

        如果任务不存在或服务器不可用，会自动切换到其他服务器重试。

        Args:
            server: 指定使用的服务器，不可用时自动切换

        返回: (任务状态详情, 下载的文件路径列表)
        """
        tried_servers = set()
        task_id = None

        while True:
            current_server = self.get_server(server)
            if not current_server:
                current_server = await self._wait_for_server()

            if current_server in tried_servers:
                # 所有服务器都试过了
                raise NetworkError("所有服务器都不可用")
            tried_servers.add(current_server)

            try:
                task_id, existed = await self._create_task_on_server(url, params, current_server)
                if show_progress:
                    print(f"{'任务已存在' if existed else '创建任务'}: {task_id} @ {current_server}")

                status = await self.wait_for_task(task_id, show_progress)

                if status.status == "failed":
                    raise TaskFailedError(task_id, status.error)
                if status.status == "cancelled":
                    raise TaskCancelledError(task_id)

                # 使用 parfive 批量下载所有文件
                files_to_download = [
                    (f.download_url, os.path.join(save_dir, f.filename))
                    for f in status.files
                ]
                if show_progress:
                    print(f"开始下载 {len(files_to_download)} 个文件...")
                downloaded = await self.download_files(files_to_download, show_progress)

                return status, downloaded

            except TaskNotFoundError:
                if show_progress:
                    print(f"\n任务 {task_id} 不存在，切换服务器重试...")
                self._task_server.pop(task_id, None)
                self._mark_server_unavailable(current_server)
                server = None  # 清除指定服务器，让下次轮转选择
                continue

            except NetworkError:
                if show_progress:
                    print(f"\n服务器 {current_server} 不可用，切换服务器重试...")
                self._mark_server_unavailable(current_server)
                server = None
                continue

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

    async def _create_task_on_server(self, url: str, params: Optional[dict], server: str) -> tuple[str, bool]:
        """在指定服务器上创建任务"""
        data, actual_server = await self._request("POST", "/tasks", preferred_server=server, json={"url": url, "params": params or {}})
        task_id = data["task_id"]
        self._task_server[task_id] = actual_server
        return task_id, data.get("existed", False)

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
