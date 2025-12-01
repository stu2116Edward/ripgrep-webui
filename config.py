# -*- coding: utf-8 -*-
"""
配置与常量模块
- 统一管理扩展名集合、路径常量与 Socket.IO 参数
"""

# Socket.IO 配置
CORS_ALLOWED_ORIGINS = "*"
SOCKETIO_PATH = "/io"
SOCKETIO_ASYNC_MODE = "gevent"
SOCKETIO_PING_TIMEOUT = 30
SOCKETIO_PING_INTERVAL = 25

# 默认数据目录（优先 /data，不存在则回退到程序目录）
DEFAULT_DATA_DIR = "/data"

# 流式分块大小（用于大量文件 I/O 的统一块尺寸）
STREAM_CHUNK_SIZE = 256 * 1024  # 256KB

# 支持的压缩/归档后缀（用于目录扫描时识别并单独处理）
SINGLE_COMPRESSED_EXTS = (".gz", ".bz2", ".xz", ".lz4", ".lzma")
ARCHIVE_EXTS = (
    ".zip", ".jar", ".war",
    ".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tbz2", ".tar.xz", ".txz",
    ".7z", ".rar"
)

# 支持的电子表格/文本后缀
EXCEL_EXTS = (".xls", ".xlsx")
CSV_EXTS = (".csv",)

# 额外的文本/数据类型扩展名
TEXT_EXTS = (
    ".txt", ".log", ".json", ".xml", ".md", ".ini", ".yaml", ".yml"
)