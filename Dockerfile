# 构建阶段（使用完整镜像）
FROM python:3.9-alpine AS builder

WORKDIR /app

# 安装构建依赖
RUN apk add --no-cache --virtual .build-deps \
    gcc \
    musl-dev \
    linux-headers \
    binutils

# 复制 requirements.txt
COPY requirements.txt .

# 安装Python包（禁用字节码编译以减小体积）
RUN pip install --user --no-cache-dir --no-compile -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

# 清理Python非必要内容并优化二进制体积
RUN find /root/.local/lib/python3.9/site-packages -type d -name '__pycache__' -exec rm -rf {} + && \
    find /root/.local/lib/python3.9/site-packages -type f -name '*.pyc' -delete && \
    find /root/.local/lib/python3.9/site-packages -type d \( -name 'tests' -o -name 'test' -o -name 'testing' -o -name 'docs' -o -name '.pytest_cache' \) -exec rm -rf {} + && \
    find /root/.local/lib/python3.9/site-packages -type f -name '*.so' -exec strip --strip-unneeded {} + || true

# 清理构建依赖与临时缓存，减小镜像体积
RUN apk del --purge .build-deps || apk del .build-deps; \
    rm -rf /root/.cache /tmp/* /var/cache/apk/*

# 运行阶段（使用更小的基础镜像）
FROM python:3.9-alpine

WORKDIR /app

# 安装运行时依赖
RUN apk add --no-cache \
    p7zip \
    unzip

# 从构建阶段仅复制必要的Python运行内容
COPY --from=builder /root/.local/lib/python3.9/site-packages /root/.local/lib/python3.9/site-packages
COPY --from=builder /root/.local/bin /root/.local/bin

# 确保Python可以找到用户安装的包
ENV PYTHONPATH=/root/.local/lib/python3.9/site-packages:/root/.local/lib/python3.9/site-packages
ENV PATH=/root/.local/bin:$PATH
ENV PYTHONDONTWRITEBYTECODE=1

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
ENV GUNICORN_CMD_ARGS="-w 1 -k geventwebsocket.gunicorn.workers.GeventWebSocketWorker"
ENV PYTHONUNBUFFERED="TRUE"

# 验证安装
RUN rg --version && \
    python -c "import flask; print('Flask version:', flask.__version__)"

# 暴露端口
EXPOSE 5000

# 运行命令
CMD ["gunicorn", "main:app", "--bind", "0.0.0.0:5000"]
