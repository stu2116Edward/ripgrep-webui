# ripgrep-webui
Visual Text Content Retrieval Tool

本项目是基于 https://github.com/BurntSushi/ripgrep 实现的webui可视化操作工具

### 安装Docker环境
使用脚本自行安装
```bash
curl -sS -O https://gitee.com/stu2116Edward/docker-tools/raw/master/docker_tools.sh && chmod +x docker_tools.sh && ./docker_tools.sh
```

### 使用命令一键部署
- 创建文件存储目录：
```bash
mkdir -p /data/kuzi
```
- 使用Docker命令部署
```bash
docker run -d \
  --name ripgrep-webui \
  -p 5757:5000 \
  -v /data/kuzi:/data:ro \
  -v $(pwd)/exports:/app/exports \
  --restart always \
  stu2116edwardhu/ripgrep-webui
```
- 使用docker-compose部署
编辑`docker-compose.yml`配置文件
```yml
services:
  ripgrep-webui:
    image: stu2116edwardhu/ripgrep-webui
    container_name: ripgrep-webui
    ports:
      - "5757:5000"
    volumes:
      - /data/kuzi:/data:ro
      - ./exports:/app/exports
    restart: always
```
将`/data/kuzi`替换为你存放文件的路径  
`./exports`是存放历史查询的目录

运行项目
```bash
docker-compose up -d
```

### 界面展示
<img width="1196" height="695" alt="屏幕截图 2025-10-19 211813" src="https://github.com/user-attachments/assets/bfa8f45b-e9ad-4f94-ace8-62b8d8ff879b" />

### 自行编译
可以自行替换ripgrep的版本去[ripgrep-release](https://github.com/BurntSushi/ripgrep/releases)下载你系统版本的二进制，比如你是`x86_64_linux`，  
就下`ripgrep-14.1.0-x86_64-unknown-linux-musl.tar.gz`，把`rg`文件解压出来替换到当前目录下  
使用Docker进行编译
```bash
docker build -t ripgrep-webui .
```
