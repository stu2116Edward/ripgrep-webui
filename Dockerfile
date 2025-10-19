# 使用官方 Python 镜像作为基础镜像
FROM python:3.9-slim

# 设置工作目录
WORKDIR /app

# 先复制 requirements.txt 并安装依赖（利用 Docker 缓存层）
COPY requirements.txt .

# 安装依赖
RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

# 创建必要的目录结构
RUN mkdir -p templates exports

# 复制 ripgrep 二进制文件
COPY rg /usr/bin/rg
RUN chmod +x /usr/bin/rg

# 复制应用代码（放在后面以利用缓存）
COPY main.py .
COPY templates/ ./templates/

# 设置环境变量
ENV GUNICORN_CMD_ARGS="-w 1 -k geventwebsocket.gunicorn.workers.GeventWebSocketWorker"
ENV PYTHONUNBUFFERED="TRUE"

# 验证 ripgrep 安装
RUN rg --version

# 暴露端口
EXPOSE 5000

# 运行命令
CMD ["gunicorn", "main:app", "--bind", "0.0.0.0:5000"]
