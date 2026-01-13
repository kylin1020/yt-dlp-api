# yt-dlp API

基于 FastAPI 的视频下载 API 服务，封装 yt-dlp。

## 安装

```bash
uv sync
```

## 配置

复制环境变量示例文件：

```bash
cp .env.example .env
```

可配置项：

| 变量 | 默认值 | 说明 |
|------|--------|------|
| TEMP_DIR | /tmp/yt-dlp-downloads | 临时文件目录 |
| MAX_CONCURRENT_DOWNLOADS | 3 | 最大并发下载数 |
| TASK_EXPIRE_SECONDS | 300 | 任务过期时间（秒） |
| CLEANUP_INTERVAL | 60 | 清理间隔（秒） |

## 运行

```bash
uv run python main.py
```

服务启动在 `http://localhost:8000`

## API

### 创建下载任务

```bash
curl -X POST http://localhost:8000/tasks \
  -H "Content-Type: application/json" \
  -d '{"url": "https://www.youtube.com/watch?v=VIDEO_ID"}'
```

响应：
```json
{"task_id": "uuid"}
```

### 查询任务状态

```bash
curl http://localhost:8000/tasks/{task_id}
```

响应：
```json
{
  "task_id": "uuid",
  "status": "completed",
  "progress": 100,
  "download_url": "/download/{task_id}",
  "filename": "video.mp4"
}
```

状态值：`pending` | `downloading` | `completed` | `failed`

### 下载文件

```bash
curl -O http://localhost:8000/download/{task_id}
```

### 删除任务

```bash
curl -X DELETE http://localhost:8000/tasks/{task_id}
```
