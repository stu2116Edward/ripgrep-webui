# 构建阶段（使用完整镜像）
FROM python:3.9-alpine AS builder

WORKDIR /app

# 安装构建依赖
RUN apk add --no-cache --virtual .build-deps \
    gcc \
    musl-dev \
    linux-headers

# 复制 requirements.txt
COPY requirements.txt .

# 安装Python包
RUN pip install --user --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

# 清理构建依赖
RUN apk del .build-deps

# 运行阶段（使用更小的基础镜像）
FROM python:3.9-alpine

WORKDIR /app

# 安装运行时依赖
RUN apk add --no-cache \
    p7zip \
    unzip

# 从构建阶段复制已安装的Python包
COPY --from=builder /root/.local /root/.local

# 确保Python可以找到用户安装的包
ENV PYTHONPATH=/root/.local/lib/python3.9/site-packages:/root/.local/lib/python3.9/site-packages
ENV PATH=/root/.local/bin:$PATH

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
ENV GUNICORN_CMD_ARGS="-w 1 -k geventwebsocket.gunicorn.workers.GeventWebSocketWorker --max-requests 1000 --max-requests-jitter 100 --timeout 30 --graceful-timeout 30"
ENV PYTHONUNBUFFERED="TRUE"

# 验证安装
RUN rg --version && \
    python -c "import flask; print('Flask version:', flask.__version__)"

# 暴露端口
EXPOSE 5000

# 运行命令
CMD ["gunicorn", "main:app", "--bind", "0.0.0.0:5000"]
