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

# 可配置的队列与写盘参数（用于控制内存与吞吐）
# rg 输出队列最大项数（施加背压，避免内存膨胀）
SEARCH_RG_QUEUE_MAXSIZE = 16384
# 导出写入队列最大项数（施加背压）
EXPORT_WRITE_QUEUE_MAXSIZE = 4096
# 导出文件缓冲区大小（字节，越大写盘吞吐越高）
EXPORT_WRITE_BUFFER_SIZE = 1024 * 1024  # 1MB
# 写入线程批量写入的最大项数（减少频繁系统调用）
EXPORT_WRITER_BATCH_MAX_ITEMS = 256
# 写入线程批量写入的目标字符数（近似控制批大小）
EXPORT_WRITER_BATCH_MAX_BYTES = 128 * 1024  # 128KB（按字符近似）
# 周期性刷新间隔（毫秒，0 表示仅在关闭时刷新）
EXPORT_WRITER_FLUSH_INTERVAL_MS = 0
