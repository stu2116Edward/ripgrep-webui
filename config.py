# -*- coding: utf-8 -*-
"""
配置与常量模块
- 统一管理扩展名集合、路径常量与 Socket.IO 参数
- 支持通过环境变量覆盖关键配置，便于在容器/系统环境中调整
"""

import os

# 环境变量辅助读取（带类型与健壮解析）
def _env_int(name: str, default: int) -> int:
    try:
        v = os.environ.get(name)
        if v is None:
            return default
        v = v.strip()
        if v.startswith(('0x', '0X')):
            return int(v, 16)
        return int(v)
    except Exception:
        return default

def _env_bool(name: str, default: bool) -> bool:
    try:
        v = os.environ.get(name)
        if v is None:
            return default
        s = v.strip().lower()
        if s in ("1", "true", "t", "yes", "y", "on"):
            return True
        if s in ("0", "false", "f", "no", "n", "off"):
            return False
        try:
            return bool(int(s))
        except Exception:
            return default
    except Exception:
        return default

# Socket.IO 配置
CORS_ALLOWED_ORIGINS = os.environ.get("RGWEBUI_CORS_ALLOWED_ORIGINS", "*")
SOCKETIO_PATH = os.environ.get("RGWEBUI_SOCKETIO_PATH", "/io")
SOCKETIO_ASYNC_MODE = os.environ.get("RGWEBUI_SOCKETIO_ASYNC_MODE", "gevent")
SOCKETIO_PING_TIMEOUT = _env_int("RGWEBUI_SOCKETIO_PING_TIMEOUT", 30)
SOCKETIO_PING_INTERVAL = _env_int("RGWEBUI_SOCKETIO_PING_INTERVAL", 25)

# 默认数据目录（优先 /data，不存在则回退到程序目录）
DEFAULT_DATA_DIR = os.environ.get("RGWEBUI_DATA_DIR", "/data")

# 流式分块大小（用于大量文件 I/O 的统一块尺寸）
STREAM_CHUNK_SIZE = _env_int("RGWEBUI_STREAM_CHUNK_SIZE", 256 * 1024)  # 256KB

# 支持的压缩/归档后缀（用于目录扫描时识别并单独处理）
SINGLE_COMPRESSED_EXTS = (".gz", ".bz2", ".xz", ".lz4", ".lzma")
ARCHIVE_EXTS = (
    ".zip", ".jar", ".war",
    ".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tbz2", ".tar.xz", ".txz",
    ".7z", ".rar"
)

# 支持的文本后缀
CSV_EXTS = (".csv",)

# 额外的文本/数据类型扩展名
TEXT_EXTS = (
    ".txt", ".log", ".json", ".xml", ".md", ".ini", ".yaml", ".yml"
)

# 可配置的队列与写盘参数（用于控制内存与吞吐）
# rg 输出队列最大项数（施加背压，避免内存膨胀）
SEARCH_RG_QUEUE_MAXSIZE = 256
# 允许通过环境变量覆盖（最小为 1，避免无界/非法值）
SEARCH_RG_QUEUE_MAXSIZE = max(1, _env_int("RGWEBUI_SEARCH_RG_QUEUE_MAXSIZE", SEARCH_RG_QUEUE_MAXSIZE))
# 导出写入队列最大项数（施加背压）
EXPORT_WRITE_QUEUE_MAXSIZE = 256
EXPORT_WRITE_QUEUE_MAXSIZE = max(1, _env_int("RGWEBUI_EXPORT_WRITE_QUEUE_MAXSIZE", EXPORT_WRITE_QUEUE_MAXSIZE))
# 导出文件缓冲区大小（字节，越大写盘吞吐越高）
EXPORT_WRITE_BUFFER_SIZE = _env_int("RGWEBUI_EXPORT_WRITE_BUFFER_SIZE", 256 * 1024)  # 256KB
# 写入线程批量写入的最大项数（减少频繁系统调用）
EXPORT_WRITER_BATCH_MAX_ITEMS = max(1, _env_int("RGWEBUI_EXPORT_WRITER_BATCH_MAX_ITEMS", 64))
# 写入线程批量写入的目标字符数（近似控制批大小）
EXPORT_WRITER_BATCH_MAX_BYTES = _env_int("RGWEBUI_EXPORT_WRITER_BATCH_MAX_BYTES", 32 * 1024)  # 32KB（按字符近似）
# 周期性刷新间隔（毫秒，0 表示仅在关闭时刷新）
EXPORT_WRITER_FLUSH_INTERVAL_MS = _env_int("RGWEBUI_EXPORT_WRITER_FLUSH_INTERVAL_MS", 100)	# 每100ms强制刷新一次

# 内存回收与页面缓存控制参数（可按需调整）
# 非预览（仅计数）模式下的轻量内存修剪间隔（毫秒）
NON_PREVIEW_LIGHT_TRIM_INTERVAL_MS = _env_int("RGWEBUI_NON_PREVIEW_LIGHT_TRIM_INTERVAL_MS", 1000)
# 非预览模式下的增强回收间隔（毫秒，包含尝试 drop_caches）
NON_PREVIEW_AGGRESSIVE_TRIM_INTERVAL_MS = _env_int("RGWEBUI_NON_PREVIEW_AGGRESSIVE_TRIM_INTERVAL_MS", 5000)
# 非预览模式下针对当前扫描文件的页面缓存丢弃间隔（毫秒）
SCAN_FILE_CACHE_DROP_INTERVAL_MS = _env_int("RGWEBUI_SCAN_FILE_CACHE_DROP_INTERVAL_MS", 2000)

# 导出写入线程的页面缓存丢弃间隔（毫秒）
EXPORT_WRITER_FADVISE_INTERVAL_MS = _env_int("RGWEBUI_EXPORT_WRITER_FADVISE_INTERVAL_MS", 500)

# 读取大文件时的页面缓存范围丢弃步长与尾部保留（字节）
FILE_READ_CACHE_DROP_STRIDE_BYTES = _env_int("RGWEBUI_FILE_READ_CACHE_DROP_STRIDE_BYTES", 64 * 1024 * 1024)  # 64MB
FILE_READ_CACHE_KEEP_TAIL_BYTES = _env_int("RGWEBUI_FILE_READ_CACHE_KEEP_TAIL_BYTES", 4 * 1024 * 1024)     # 4MB
# 是否为源文件读取设置 NOREUSE 建议（提示内核尽快丢弃已读页）
FILE_READ_SET_NOREUSE = _env_bool("RGWEBUI_FILE_READ_SET_NOREUSE", True)

# ripgrep 在非预览（仅计数）模式下的参数开关
RG_NO_MMAP_IN_COUNT_MODE = _env_bool("RGWEBUI_RG_NO_MMAP_IN_COUNT_MODE", True)
RG_LINE_BUFFERED_IN_COUNT_MODE = _env_bool("RGWEBUI_RG_LINE_BUFFERED_IN_COUNT_MODE", True)

# 导出目录（容器中默认 /app/exports，可用环境变量覆盖）
EXPORTS_DIR = os.environ.get("RGWEBUI_EXPORTS_DIR", "/app/exports")

# 检索结束与导出结束的最终回收策略（可调节）
# 是否在检索流程最终阶段执行强回收
SEARCH_FINAL_RECLAIM_ENABLED = _env_bool("RGWEBUI_SEARCH_FINAL_RECLAIM_ENABLED", True)
# 强回收重复次数（>=1），每次之间可休眠等待内核收敛
FINAL_AGGRESSIVE_RECLAIM_REPEATS = max(1, _env_int("RGWEBUI_FINAL_AGGRESSIVE_RECLAIM_REPEATS", 1))
# 每次强回收之间的休眠（毫秒）
FINAL_AGGRESSIVE_RECLAIM_SLEEP_MS = _env_int("RGWEBUI_FINAL_AGGRESSIVE_RECLAIM_SLEEP_MS", 500)
# 是否在检索最终阶段尝试丢弃导出文件页面缓存
SEARCH_FINAL_DROP_EXPORT_CACHE_ENABLED = _env_bool("RGWEBUI_SEARCH_FINAL_DROP_EXPORT_CACHE_ENABLED", True)
# 是否在检索最终阶段丢弃本次已扫描过的源文件页面缓存
SEARCH_FINAL_DROP_SCANNED_FILE_CACHE_ENABLED = _env_bool("RGWEBUI_SEARCH_FINAL_DROP_SCANNED_FILE_CACHE_ENABLED", True)
# 导出流关闭后是否执行回收（丢缓存+轻量修剪+强回收）
EXPORT_CLOSE_RECLAIM_ENABLED = _env_bool("RGWEBUI_EXPORT_CLOSE_RECLAIM_ENABLED", True)
# 导出关闭阶段的强回收重复次数与休眠（毫秒）
EXPORT_CLOSE_AGGRESSIVE_RECLAIM_REPEATS = max(1, _env_int("RGWEBUI_EXPORT_CLOSE_AGGRESSIVE_RECLAIM_REPEATS", 2))
EXPORT_CLOSE_AGGRESSIVE_RECLAIM_SLEEP_MS = _env_int("RGWEBUI_EXPORT_CLOSE_AGGRESSIVE_RECLAIM_SLEEP_MS", 500)

# 空闲阶段的内存回收策略（页面空闲、无检索/导出活动时）
# 是否启用空闲内存回收
IDLE_RECLAIM_ENABLED = _env_bool("RGWEBUI_IDLE_RECLAIM_ENABLED", True)
# 空闲时的轻量修剪间隔（毫秒）：gc + 工作集压缩/malloc_trim
IDLE_LIGHT_TRIM_INTERVAL_MS = _env_int("RGWEBUI_IDLE_LIGHT_TRIM_INTERVAL_MS", 2000)
# 空闲时的增强回收间隔（毫秒）：包含尝试 drop_caches（Linux）
IDLE_AGGRESSIVE_TRIM_INTERVAL_MS = _env_int("RGWEBUI_IDLE_AGGRESSIVE_TRIM_INTERVAL_MS", 30000)
