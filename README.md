# ripgrep-webui
Visual Text Content Retrieval Tool

**注意**：如果是大文件检索或文件中关键字匹配数量过多建议关闭预览模式，如果检索崩溃建议刷新页面或手动重启容器  

本项目是基于 https://github.com/BurntSushi/ripgrep 实现的webui可视化操作工具  
注意：在检索excel格式文件如`xls`,`xlsx`格式建议先转为csv格式（对于较大的excel格式文件会检索失败）


## 项目结构
<pre>
项目根目录/
├── main.py                 	# 应用入口
├── config.py              	  # 配置项
├── routes.py             	  # API路由
├── search.py             	  # 流式检索核心
├── export.py             	  # 导出管理
├── process.py             	  # 进程管理
├── utils.py                  # 通用工具
├── handlers/              	  # 文件处理器
│   ├── text.py            	  # 文本文件处理器
│   ├── csv.py                # CSV处理器
│   ├── compressed.py         # 压缩文件处理器
├── templates/			          # 前端主题
│   ├── index.html		        # 前端框架
│   ├── css/			            #  样式目录
│   │   └── style.css		      #  样式文件
│   └── js/			              # 前端逻辑处理目录
│       └── app.js		        # 前端逻辑脚本
├── exports/			            # 导出文件目录
└── requirements.txt	        # Python依赖清单
</pre>


## Docker
### 安装Docker环境
使用脚本自行安装
```
curl -sS -O https://raw.githubusercontent.com/stu2116Edward/my-sh/refs/heads/main/docker_tools.sh && chmod +x docker_tools.sh && ./docker_tools.sh
```
镜像加速
```bash
curl -sS -O https://gitee.com/stu2116Edward/docker-tools/raw/master/docker_tools.sh && chmod +x docker_tools.sh && ./docker_tools.sh
```

### 使用命令一键部署
- 创建文件存储目录：
```bash
mkdir -p /data/kuzi
cd /data/kuzi
```
- 使用Docker命令部署
```bash
  docker run -d \
  --name ripgrep-webui \
  -p 5757:5000 \
  -v /data/kuzi:/data:ro \
  -v $(pwd)/exports:/app/exports \
  --restart unless-stopped \
  stu2116edwardhu/ripgrep-webui
```
资源限制
```bash
docker run -d \
  --name ripgrep-webui \
  -p 5757:5000 \
  -v /data/kuzi:/data:ro \
  -v $(pwd)/exports:/app/exports \
  --restart unless-stopped \
  --cpus=1.0 \
  --cpuset-cpus="0" \
  --cpu-shares=1024 \
  --memory="2g" \
  --memory-reservation="512m" \
  --memory-swap="4g" \
  stu2116edwardhu/ripgrep-webui
```
- 使用docker-compose部署
编辑`docker-compose.yml`配置文件
```yml
services:
  ripgrep-webui:
    cpu_count: 1                  # CPU核心数
    cpuset: '0'                   # 绑定到特定CPU
    cpu_shares: 1024              # CPU相对权重（默认1024）
    cpus: 1.0                     # CPU限制（docker-compose v2.3+）
    mem_limit: 2g                 # 内存硬限制
    mem_reservation: 512m         # 内存软限制
    memswap_limit: 4g             # 内存+交换空间总限制
    image: stu2116edwardhu/ripgrep-webui
    container_name: ripgrep-webui
    ports:
      - "5757:5000"
    volumes:
      - /data/kuzi:/data:ro
      - ./exports:/app/exports
    restart: unless-stopped
```
将`/data/kuzi`替换为你存放文件的路径  
`./exports`是存放历史查询的目录

运行项目
```bash
docker-compose up -d
```

## Windows
本项目可以在windows中安装python环境并配置对应库直接运行  
还需要在 https://github.com/BurntSushi/ripgrep/releases 下载所需的环境`ripgrep-15.1.0-x86_64-pc-windows-gnu.zip`  
执行主函数：
```
python main.py
```
在浏览器中输入`127.0.0.1:5000`  
数据目录即当前项目所在目录，把需要检索的文件直接放到该项目所在文件夹内即可  
注意：检索数据的回退目录会在项目所在盘符下的如`E:\app\exports`目录中写入数据（自行清理和备份）  


### 界面展示
<img width="1223" height="712" alt="屏幕截图 2025-12-07 212649" src="https://github.com/user-attachments/assets/fe56e185-7498-471f-b910-d91c2e894b78" />

### 自行编译
可以自行替换ripgrep的版本去[ripgrep-release](https://github.com/BurntSushi/ripgrep/releases)下载你系统版本的二进制，比如你是`x86_64_linux`，  
就下`ripgrep-14.1.0-x86_64-unknown-linux-musl.tar.gz`，把`rg`文件解压出来替换到当前目录下  
使用Docker进行编译
```bash
docker build -t ripgrep-webui .
```
