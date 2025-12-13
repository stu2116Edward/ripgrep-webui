# -*- coding: utf-8 -*-
"""
工具函数模块
- 提供事件发送包装、关键字安全化、外部命令检测、进程创建标志等
- 维护全局 app 与 socketio 的上下文注入，避免循环依赖
"""

import os
import shutil
from functools import lru_cache
import gc
import ctypes

from config import (
    SINGLE_COMPRESSED_EXTS, ARCHIVE_EXTS, EXCEL_EXTS, CSV_EXTS, TEXT_EXTS
)

# 全局应用上下文（由 main.py 注入）
_app = None
_socketio = None


def init_app(app, socketio):
    """由主入口注入 app 与 socketio 实例，供各模块调用。"""
    global _app, _socketio
    _app = app
    _socketio = socketio


def get_app():
    """返回注入的 Flask app 实例。"""
    return _app


def get_socketio():
    """返回注入的 SocketIO 实例。"""
    return _socketio


def emit_progress_ex(phase=None, file_type=None, elapsed_ms=None,
                     bytes_done=None, bytes_total=None,
                     matches=None, files_total=None, files_done=None,
                     label=None):
    """扩展进度事件发射器：统一处理进度字段并通过 socketio 推送。"""
    try:
        payload = {}
        if matches is not None:
            payload['matches'] = int(matches)
        if files_total is not None:
            payload['files_total'] = int(files_total)
        if files_done is not None:
            payload['files_done'] = int(files_done)
        if phase:
            payload['phase'] = str(phase)
        if file_type:
            payload['file_type'] = str(file_type)
        if bytes_done is not None:
            try:
                bd = int(bytes_done)
                bt = int(bytes_total) if bytes_total is not None else None
                if bt is not None and bd > bt:
                    bd = bt
                payload['bytes_done'] = bd
                if bt is not None:
                    payload['bytes_total'] = bt
            except Exception:
                pass
        if label:
            payload['label'] = label
        if _socketio:
            _socketio.emit('progress', payload)
    except Exception:
        pass


def emit_message_utf(text):
    """统一 UTF-8 文本发送包装，避免乱码。"""
    try:
        if not isinstance(text, str):
            try:
                text = str(text)
            except Exception:
                text = ''
        sanitized = text.encode('utf-8', errors='replace').decode('utf-8', errors='replace')
        if _socketio:
            _socketio.emit('message', {'message': sanitized})
    except Exception:
        pass


def sanitize_keyword(keyword: str) -> str:
    """关键字安全化（用于文件名前缀）。"""
    try:
        safe = ''.join(c for c in (keyword or '') if c.isalnum() or c in (' ', '_', '-')).strip()
        return safe or 'search'
    except Exception:
        return 'search'


@lru_cache(maxsize=64)
def has_cmd(name):
    """检查外部命令是否在 PATH 中（带 LRU 缓存）。"""
    try:
        return shutil.which(name) is not None
    except Exception:
        return False


def popen_creationflags():
    """Windows: 使用 CREATE_NEW_PROCESS_GROUP 以改进终止可靠性；其他平台返回 0。"""
    try:
        if os.name == 'nt':
            import subprocess as sp
            return getattr(sp, 'CREATE_NEW_PROCESS_GROUP', 0)
        return 0
    except Exception:
        return 0


def is_single_file_compressed(filename_lower: str) -> bool:
    """判断是否为单一压缩文件（排除含 tar 的组合压缩）。"""
    if filename_lower.endswith(('.tar.gz', '.tgz', '.tar.bz2', '.tbz2', '.tar.xz', '.txz')):
        return False
    return filename_lower.endswith(SINGLE_COMPRESSED_EXTS)


def is_archive_multi_file(filename_lower: str) -> bool:
    """判断是否为多文件归档（zip/7z/rar/tar等）。"""
    return filename_lower.endswith(ARCHIVE_EXTS)


def is_excel_file(filename_lower: str) -> bool:
    """判断是否为 Excel 文件。"""
    return filename_lower.endswith(EXCEL_EXTS)


def is_csv_file(filename_lower: str) -> bool:
    """判断是否为 CSV 文件。"""
    return filename_lower.endswith(CSV_EXTS)


def classify_file_type(filename_lower: str):
    """根据文件名后缀分类文件类型：archive/compressed/excel/csv/text/other"""
    fl = (filename_lower or '').lower()
    if is_archive_multi_file(fl):
        return 'archive'
    if is_single_file_compressed(fl):
        return 'compressed'
    if is_excel_file(fl):
        return 'excel'
    if is_csv_file(fl):
        return 'csv'
    if fl.endswith(TEXT_EXTS):
        return 'text'
    return 'other'


def strip_single_compress_ext(filename_lower: str) -> str:
    """
    去掉单一压缩扩展，返回内部真实扩展（小写，如 .xlsx/.csv），不匹配则返回空串。
    """
    if not filename_lower:
        return ''
    for ce in SINGLE_COMPRESSED_EXTS:
        if filename_lower.endswith(ce):
            base = filename_lower[:-len(ce)]
            inner_ext = os.path.splitext(base)[1].lower()
            return inner_ext
    return ''


def trim_process_memory():
    """
    主动尝试将进程内存归还给操作系统：
    - 始终先运行一次 gc.collect()
    - Windows: 调用 psapi.EmptyWorkingSet 将工作集尽量压缩
    - Linux: 尝试 libc.malloc_trim(0) 释放未使用的堆内存
    其他平台若不可用则静默跳过。
    """
    try:
        # 先进行 Python 层垃圾回收
        try:
            gc.collect()
        except Exception:
            pass

        if os.name == 'nt':
            try:
                # 压缩当前进程工作集，降低常驻内存占用
                hproc = ctypes.windll.kernel32.GetCurrentProcess()
                ctypes.windll.psapi.EmptyWorkingSet(hproc)
            except Exception:
                pass
        else:
            try:
                # 通过 libc 的 malloc_trim 归还未使用的堆内存（GLIBC）
                libc = ctypes.CDLL('libc.so.6')
                try:
                    libc.malloc_trim(0)
                except Exception:
                    pass
            except Exception:
                # 非 GLIBC 或不可用时跳过
                pass
    except Exception:
        pass


def drop_file_cache_fd(fd: int):
    """
    尝试丢弃指定文件描述符的页面缓存（Linux：posix_fadvise DONTNEED）。
    - 仅在类 Unix 环境下有效；Windows 跳过。
    - 失败时静默忽略。
    """
    try:
        if os.name == 'nt':
            return
        try:
            libc = ctypes.CDLL('libc.so.6')
        except Exception:
            return
        POSIX_FADV_DONTNEED = 4
        try:
            libc.posix_fadvise(ctypes.c_int(fd), ctypes.c_long(0), ctypes.c_long(0), ctypes.c_int(POSIX_FADV_DONTNEED))
        except Exception:
            try:
                libc.posix_fadvise64(ctypes.c_int(fd), ctypes.c_long(0), ctypes.c_long(0), ctypes.c_int(POSIX_FADV_DONTNEED))
            except Exception:
                pass
    except Exception:
        pass


def drop_file_cache_range(fd: int, offset: int, length: int):
    """
    丢弃指定文件描述符在给定范围 [offset, offset+length) 的页面缓存。
    - 仅类 Unix 环境有效；Windows 跳过。
    - offset/length 需为非负且 length>0。
    """
    try:
        if os.name == 'nt':
            return
        if offset is None or length is None:
            return
        if offset < 0 or length <= 0:
            return
        try:
            libc = ctypes.CDLL('libc.so.6')
        except Exception:
            return
        POSIX_FADV_DONTNEED = 4
        # 优先使用 64 位版本，处理超大文件偏移
        try:
            libc.posix_fadvise64(ctypes.c_int(fd), ctypes.c_longlong(offset), ctypes.c_longlong(length), ctypes.c_int(POSIX_FADV_DONTNEED))
        except Exception:
            try:
                libc.posix_fadvise(ctypes.c_int(fd), ctypes.c_long(offset), ctypes.c_long(length), ctypes.c_int(POSIX_FADV_DONTNEED))
            except Exception:
                pass
    except Exception:
        pass


def set_file_access_noreuse_fd(fd: int):
    """
    为给定文件描述符设置访问建议为 NOREUSE（不复用），提示内核尽快丢弃已用页面。
    - 仅类 Unix 环境有效；Windows 跳过。
    """
    try:
        if os.name == 'nt':
            return
        try:
            libc = ctypes.CDLL('libc.so.6')
        except Exception:
            return
        POSIX_FADV_NOREUSE = 5
        try:
            libc.posix_fadvise(ctypes.c_int(fd), ctypes.c_long(0), ctypes.c_long(0), ctypes.c_int(POSIX_FADV_NOREUSE))
        except Exception:
            try:
                libc.posix_fadvise64(ctypes.c_int(fd), ctypes.c_long(0), ctypes.c_long(0), ctypes.c_int(POSIX_FADV_NOREUSE))
            except Exception:
                pass
    except Exception:
        pass
def drop_file_cache_path(path: str):
    """
    尝试丢弃指定路径文件的页面缓存（打开只读后调用 fadvise DONTNEED）。
    - 仅在类 Unix 环境下有效；Windows 跳过。
    - 失败时静默忽略。
    """
    try:
        if os.name == 'nt':
            return
        fd = None
        try:
            fd = os.open(path, os.O_RDONLY)
        except Exception:
            fd = None
        if fd is None:
            return
        try:
            drop_file_cache_fd(fd)
        finally:
            try:
                os.close(fd)
            except Exception:
                pass
    except Exception:
        pass


def aggressive_memory_reclaim():
    """
    在容器等环境下进行更强的内存回收尝试：
    - 多次 gc.collect 与 malloc_trim，尽可能归还未使用堆内存
    - 尝试 sync 并写入 /proc/sys/vm/drop_caches（需要权限，失败则忽略）
    - Windows 下重复压缩工作集
    """
    try:
        try:
            gc.collect()
            gc.collect()
        except Exception:
            pass

        if os.name == 'nt':
            try:
                hproc = ctypes.windll.kernel32.GetCurrentProcess()
                ctypes.windll.psapi.EmptyWorkingSet(hproc)
                ctypes.windll.psapi.EmptyWorkingSet(hproc)
            except Exception:
                pass
            return

        # Unix/Linux: malloc_trim + 尝试 drop_caches
        try:
            libc = ctypes.CDLL('libc.so.6')
            try:
                libc.malloc_trim(0)
                libc.malloc_trim(0)
            except Exception:
                pass
            # 同步文件系统缓冲，提升丢弃页面缓存的成功率
            try:
                libc.sync()
            except Exception:
                pass
        except Exception:
            libc = None

        # 尝试系统级丢弃缓存（可能需要 CAP_SYS_ADMIN，失败则忽略）
        try:
            with open('/proc/sys/vm/drop_caches', 'w') as f:
                # 3 = pagecache + dentries + inodes
                f.write('3\n')
        except Exception:
            pass
    except Exception:
        pass
