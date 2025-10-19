# 使用官方 Python 镜像作为基础镜像
FROM python:3.9-slim

# 设置工作目录
WORKDIR /app

# 将当前目录内容复制到工作目录中
COPY . /app

# 安装依赖
RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

# 将 ripgrep 复制到 /usr/bin
COPY rg /usr/bin/rg
RUN chmod +x /usr/bin/rg

# 暴露端口
EXPOSE 5000

# 运行命令
CMD ["gunicorn", "main:app", "--bind", "0.0.0.0:5000"]
