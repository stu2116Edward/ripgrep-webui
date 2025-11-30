# 使用官方 Python 镜像作为基础镜像
FROM python:3.9-slim

# 设置工作目录
WORKDIR /app

# 复制 requirements.txt 并安装Python依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    p7zip-full \
    unzip \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# 创建必要的目录结构
RUN mkdir -p templates exports

# 复制 ripgrep 二进制文件
COPY rg /usr/bin/rg
RUN chmod +x /usr/bin/rg

# 复制应用代码
COPY main.py .
COPY config.py .
COPY routes.py .
COPY utils.py .
COPY file_handlers.py .
COPY search_engine.py .
COPY process_manager.py .
COPY export_manager.py .
COPY templates/ ./templates/

# 设置环境变量
ENV GUNICORN_CMD_ARGS="-w 1 -k geventwebsocket.gunicorn.workers.GeventWebSocketWorker --preload --max-requests 0 --timeout 0 --graceful-timeout 0 --keep-alive 30"
ENV PYTHONUNBUFFERED="TRUE"

# 验证 ripgrep 安装
RUN rg --version

# 暴露端口
EXPOSE 5000

# 运行命令
CMD ["gunicorn", "main:app", "--bind", "0.0.0.0:5000"]
