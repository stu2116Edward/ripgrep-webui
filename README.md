# ripgrep-webui
Visual Text Content Retrieval Tool

### 使用命令一键部署
创建文件存储目录：
```
mkdir -p /data/kuzi
```
使用Docker命令部署
```
docker run -d \
  --name ripgrep-webui \
  -p 5757:5000 \
  -v /data/kuzi:/data:ro \
  -v $(pwd)/exports:/app/exports \
  --restart always \
  stu2116edwardhu/ripgrep-webui
```

### 界面展示
<img width="1196" height="695" alt="屏幕截图 2025-10-19 211813" src="https://github.com/user-attachments/assets/bfa8f45b-e9ad-4f94-ace8-62b8d8ff879b" />
