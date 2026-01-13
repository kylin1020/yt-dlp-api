"""
yt-dlp API 客户端 SDK
用于管理视频下载任务并显示下载进度
"""

import time
import sys
import requests
from typing import Optional
from dataclasses import dataclass


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
    files: list = None
    info: dict = None

    @property
    def is_done(self) -> bool:
        return self.status in ("completed", "failed", "cancelled")

    @property
    def status_text(self) -> str:
        status_map = {
            "pending": "等待中",
            "downloading": "下载中",
            "completed": "已完成",
            "failed": "失败",
            "cancelled": "已取消"
        }
        return status_map.get(self.status, self.status)


class YtDlpClient:
    """yt-dlp API 客户端"""

    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()

    def create_task(self, url: str, params: dict = None) -> tuple[str, bool]:
        """
        创建下载任务
        返回: (task_id, existed) - existed为True表示任务已存在
        """
        resp = self.session.post(
            f"{self.base_url}/tasks",
            json={"url": url, "params": params or {}}
        )
        resp.raise_for_status()
        data = resp.json()
        return data["task_id"], data.get("existed", False)

    def get_task(self, task_id: str) -> TaskStatus:
        """获取任务状态"""
        resp = self.session.get(f"{self.base_url}/tasks/{task_id}")
        resp.raise_for_status()
        data = resp.json()
        files = []
        for f in data.get("files") or []:
            files.append(TaskFile(
                filename=f["filename"],
                size=f["size"],
                download_url=f["download_url"],
                file_id=f["download_url"].split("/")[-1]
            ))
        return TaskStatus(
            task_id=data["task_id"],
            status=data["status"],
            progress=data.get("progress", 0),
            speed=data.get("speed", ""),
            total_size=data.get("total_size", ""),
            error=data.get("error", ""),
            files=files,
            info=data.get("info")
        )

    def cancel_task(self, task_id: str) -> str:
        """取消任务"""
        resp = self.session.post(f"{self.base_url}/tasks/{task_id}/cancel")
        resp.raise_for_status()
        return resp.json()["message"]

    def delete_task(self, task_id: str) -> str:
        """删除任务"""
        resp = self.session.delete(f"{self.base_url}/tasks/{task_id}")
        resp.raise_for_status()
        return resp.json()["message"]

    def download_file(self, file_id: str, save_path: str = None) -> str:
        """
        下载文件到本地
        返回保存的文件路径
        """
        resp = self.session.get(
            f"{self.base_url}/download/{file_id}",
            stream=True
        )
        resp.raise_for_status()

        # 从响应头获取文件名
        filename = save_path
        if not filename:
            cd = resp.headers.get("content-disposition", "")
            if "filename=" in cd:
                filename = cd.split("filename=")[-1].strip('"')
            else:
                filename = file_id

        with open(filename, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        return filename

    def get_max_concurrent(self) -> tuple[int, int]:
        """获取最大并发数和当前活跃数"""
        resp = self.session.get(f"{self.base_url}/settings/max-concurrent")
        resp.raise_for_status()
        data = resp.json()
        return data["max_concurrent"], data["active_count"]

    def set_max_concurrent(self, value: int) -> int:
        """设置最大并发数"""
        resp = self.session.put(
            f"{self.base_url}/settings/max-concurrent",
            json={"value": value}
        )
        resp.raise_for_status()
        return resp.json()["max_concurrent"]

    def get_monitor(self) -> dict:
        """获取系统监控信息"""
        resp = self.session.get(f"{self.base_url}/monitor")
        resp.raise_for_status()
        return resp.json()

    def wait_for_task(
        self,
        task_id: str,
        show_progress: bool = True,
        poll_interval: float = 1.0
    ) -> TaskStatus:
        """
        等待任务完成，可选显示进度
        """
        while True:
            status = self.get_task(task_id)

            if show_progress:
                self._print_progress(status)

            if status.is_done:
                if show_progress:
                    print()  # 换行
                return status

            time.sleep(poll_interval)

    def _print_progress(self, status: TaskStatus):
        """打印进度条"""
        bar_width = 30
        filled = int(bar_width * status.progress / 100)
        bar = "█" * filled + "░" * (bar_width - filled)

        info = f"\r[{bar}] {status.progress:5.1f}% | {status.status_text}"
        if status.speed:
            info += f" | {status.speed}"
        if status.total_size:
            info += f" | {status.total_size}"

        sys.stdout.write(info.ljust(80))
        sys.stdout.flush()

    def download(
        self,
        url: str,
        params: dict = None,
        save_dir: str = ".",
        show_progress: bool = True
    ) -> list[str]:
        """
        一站式下载：创建任务、等待完成、下载文件
        返回下载的文件路径列表
        """
        task_id, existed = self.create_task(url, params)
        if existed and show_progress:
            print(f"任务已存在: {task_id}")
        elif show_progress:
            print(f"创建任务: {task_id}")

        status = self.wait_for_task(task_id, show_progress)

        if status.status == "failed":
            raise Exception(f"下载失败: {status.error}")
        if status.status == "cancelled":
            raise Exception("任务已取消")

        # 下载所有文件
        saved_files = []
        for f in status.files:
            if show_progress:
                print(f"保存文件: {f.filename}")
            import os
            save_path = os.path.join(save_dir, f.filename)
            self.download_file(f.file_id, save_path)
            saved_files.append(save_path)

        return saved_files


# 使用示例
if __name__ == "__main__":
    client = YtDlpClient("http://localhost:8000")

    # 示例1: 一站式下载
    # files = client.download("https://www.youtube.com/watch?v=xxx")

    # 示例2: 分步操作
    # task_id, _ = client.create_task("https://www.youtube.com/watch?v=xxx")
    # status = client.wait_for_task(task_id)
    # if status.status == "completed":
    #     for f in status.files:
    #         client.download_file(f.file_id, f.filename)

    # 示例3: 查看系统状态
    # monitor = client.get_monitor()
    # print(f"磁盘: {monitor['disk']['free']} 可用")
    # print(f"带宽: {monitor['bandwidth']['current_speed']}")

    print("客户端已就绪，请参考示例代码使用")
