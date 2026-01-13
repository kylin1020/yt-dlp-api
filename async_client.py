"""
yt-dlp API 异步客户端 SDK
用于管理视频下载任务并显示下载进度
"""

import asyncio
import sys
import httpx
from typing import Optional
from dataclasses import dataclass, field


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
    files: list = field(default_factory=list)
    info: dict = None

    @property
    def is_done(self) -> bool:
        return self.status in ("completed", "failed", "cancelled")

    @property
    def status_text(self) -> str:
        return {"pending": "等待中", "downloading": "下载中", "completed": "已完成",
                "failed": "失败", "cancelled": "已取消"}.get(self.status, self.status)


class AsyncYtDlpClient:
    """yt-dlp API 异步客户端"""

    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url.rstrip("/")
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self):
        self._client = httpx.AsyncClient(timeout=30.0)
        return self

    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()

    @property
    def client(self) -> httpx.AsyncClient:
        if not self._client:
            self._client = httpx.AsyncClient(timeout=30.0)
        return self._client

    async def create_task(self, url: str, params: dict = None) -> tuple[str, bool]:
        """创建下载任务，返回 (task_id, existed)"""
        resp = await self.client.post(
            f"{self.base_url}/tasks",
            json={"url": url, "params": params or {}}
        )
        resp.raise_for_status()
        data = resp.json()
        return data["task_id"], data.get("existed", False)

    async def get_task(self, task_id: str) -> TaskStatus:
        """获取任务状态"""
        resp = await self.client.get(f"{self.base_url}/tasks/{task_id}")
        resp.raise_for_status()
        data = resp.json()
        files = [
            TaskFile(f["filename"], f["size"], f["download_url"],
                     f["download_url"].split("/")[-1])
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
        resp = await self.client.post(f"{self.base_url}/tasks/{task_id}/cancel")
        resp.raise_for_status()
        return resp.json()["message"]

    async def delete_task(self, task_id: str) -> str:
        """删除任务"""
        resp = await self.client.delete(f"{self.base_url}/tasks/{task_id}")
        resp.raise_for_status()
        return resp.json()["message"]

    async def download_file(self, file_id: str, save_path: str = None) -> str:
        """下载文件到本地"""
        async with self.client.stream("GET", f"{self.base_url}/download/{file_id}") as resp:
            resp.raise_for_status()
            filename = save_path
            if not filename:
                cd = resp.headers.get("content-disposition", "")
                filename = cd.split("filename=")[-1].strip('"') if "filename=" in cd else file_id
            with open(filename, "wb") as f:
                async for chunk in resp.aiter_bytes(8192):
                    f.write(chunk)
        return filename

    async def get_max_concurrent(self) -> tuple[int, int]:
        """获取最大并发数和当前活跃数"""
        resp = await self.client.get(f"{self.base_url}/settings/max-concurrent")
        resp.raise_for_status()
        data = resp.json()
        return data["max_concurrent"], data["active_count"]

    async def set_max_concurrent(self, value: int) -> int:
        """设置最大并发数"""
        resp = await self.client.put(
            f"{self.base_url}/settings/max-concurrent",
            json={"value": value}
        )
        resp.raise_for_status()
        return resp.json()["max_concurrent"]

    async def get_monitor(self) -> dict:
        """获取系统监控信息"""
        resp = await self.client.get(f"{self.base_url}/monitor")
        resp.raise_for_status()
        return resp.json()

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
        self, url: str, params: dict = None, save_dir: str = ".", show_progress: bool = True
    ) -> list[str]:
        """一站式下载：创建任务、等待完成、下载文件"""
        task_id, existed = await self.create_task(url, params)
        if show_progress:
            print(f"{'任务已存在' if existed else '创建任务'}: {task_id}")

        status = await self.wait_for_task(task_id, show_progress)

        if status.status == "failed":
            raise Exception(f"下载失败: {status.error}")
        if status.status == "cancelled":
            raise Exception("任务已取消")

        import os
        saved_files = []
        for f in status.files:
            if show_progress:
                print(f"保存文件: {f.filename}")
            save_path = os.path.join(save_dir, f.filename)
            await self.download_file(f.file_id, save_path)
            saved_files.append(save_path)
        return saved_files

    async def batch_download(
        self, urls: list[str], params: dict = None, show_progress: bool = True
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

    async def close(self):
        """关闭客户端连接"""
        if self._client:
            await self._client.aclose()
            self._client = None


# 使用示例
if __name__ == "__main__":
    async def main():
        async with AsyncYtDlpClient("http://localhost:8000") as client:
            # 示例1: 一站式下载
            # files = await client.download("https://www.youtube.com/watch?v=xxx")

            # 示例2: 批量下载
            # urls = ["https://...", "https://..."]
            # statuses = await client.batch_download(urls)

            # 示例3: 并发创建多个任务
            # tasks = await asyncio.gather(
            #     client.create_task("https://..."),
            #     client.create_task("https://..."),
            # )

            monitor = await client.get_monitor()
            print(f"磁盘可用: {monitor['disk']['free']}")

    asyncio.run(main())
