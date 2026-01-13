#!/bin/bash
set -e

SERVICE_NAME="yt-dlp-api"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"

echo "安装 yt-dlp-api 服务..."
echo "项目目录: $PROJECT_DIR"

# 检查是否有 sudo 权限
if [ "$EUID" -ne 0 ]; then
    echo "请使用 sudo 运行此脚本"
    exit 1
fi

# 获取实际用户（非 root）
REAL_USER="${SUDO_USER:-$USER}"

# 检查并安装 uv
if ! command -v uv &> /dev/null; then
    echo "安装 uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.local/bin:$PATH"
fi

# 检查并安装依赖
if [ ! -d "$PROJECT_DIR/.venv" ]; then
    echo "安装 Python 依赖..."
    cd "$PROJECT_DIR"
    sudo -u "$REAL_USER" uv sync
fi

# 生成 service 文件
sed -e "s|%USER%|$REAL_USER|g" \
    -e "s|%PROJECT_DIR%|$PROJECT_DIR|g" \
    "$PROJECT_DIR/scripts/yt-dlp-api.service" > "$SERVICE_FILE"

# 创建 .env 文件（如果不存在）
if [ ! -f "$PROJECT_DIR/.env" ]; then
    cp "$PROJECT_DIR/.env.example" "$PROJECT_DIR/.env" 2>/dev/null || touch "$PROJECT_DIR/.env"
fi

# 重新加载 systemd 并启动服务
systemctl daemon-reload
systemctl enable "$SERVICE_NAME"
systemctl restart "$SERVICE_NAME"

echo ""
echo "服务已安装并启动"
echo ""
echo "管理命令:"
echo "  sudo systemctl start $SERVICE_NAME    # 启动"
echo "  sudo systemctl stop $SERVICE_NAME     # 停止"
echo "  sudo systemctl restart $SERVICE_NAME  # 重启"
echo "  sudo systemctl status $SERVICE_NAME   # 状态"
echo "  sudo journalctl -u $SERVICE_NAME -f   # 查看日志"
echo ""
echo "API 地址: http://localhost:8000"
