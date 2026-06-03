# -*- coding: utf-8 -*-
"""
文本文件处理器
- 提供分块复制等文本文件流式处理工具
"""

import os
import time

from config import (
    STREAM_CHUNK_SIZE,
    FILE_READ_CACHE_DROP_STRIDE_BYTES,
    FILE_READ_CACHE_KEEP_TAIL_BYTES,
    FILE_READ_SET_NOREUSE,
)
from utils import (
    emit_progress_ex,
    drop_file_cache_range,
    set_file_access_noreuse_fd,
)
import process as pm


def copy_fileobj_chunked(src, dst, chunk_size: int = STREAM_CHUNK_SIZE, progress_cb=None, bytes_total=None):
    """
    分块复制 src->dst，支持可选的字节级进度回调（毫秒级计时）。
    progress_cb: callable(done_bytes:int, total_bytes:Optional[int], elapsed_ms:int)
    """
    start_ns = time.perf_counter_ns()
    done = 0
    # 针对源文件的页面缓存控制：设置不复用建议，并进行范围丢弃（可调）
    src_fd = None
    last_drop_to = 0
    keep_tail_bytes = int(FILE_READ_CACHE_KEEP_TAIL_BYTES) if FILE_READ_CACHE_KEEP_TAIL_BYTES else (4 * 1024 * 1024)
    drop_stride_bytes = int(FILE_READ_CACHE_DROP_STRIDE_BYTES) if FILE_READ_CACHE_DROP_STRIDE_BYTES else (64 * 1024 * 1024)
    try:
        try:
            if hasattr(src, 'fileno'):
                src_fd = src.fileno()
                if FILE_READ_SET_NOREUSE:
                    try:
                        set_file_access_noreuse_fd(src_fd)
                    except Exception:
                        pass
        except Exception:
            src_fd = None
        while True:
            if pm.cancel_requested:
                try:
                    if hasattr(dst, 'flush'):
                        dst.flush()
                except Exception:
                    pass
                try:
                    if hasattr(dst, 'close'):
                        dst.close()
                except Exception:
                    pass
                break
            chunk = src.read(chunk_size)
            if not chunk:
                break
            try:
                dst.write(chunk)
                done += len(chunk)
                # 精确范围丢弃：对已读旧页按步长丢弃，抑制 page cache 累积
                if src_fd is not None and drop_stride_bytes and drop_stride_bytes > 0:
                    try:
                        drop_to_target = max(0, done - keep_tail_bytes)
                        drop_len = drop_to_target - last_drop_to
                        if drop_len >= drop_stride_bytes:
                            try:
                                drop_file_cache_range(src_fd, last_drop_to, drop_len)
                            except Exception:
                                pass
                            last_drop_to += drop_len
                    except Exception:
                        pass
                if progress_cb:
                    elapsed_ms = int((time.perf_counter_ns() - start_ns) / 1_000_000)
                    try:
                        progress_cb(done, bytes_total, elapsed_ms)
                    except Exception:
                        pass
            except BrokenPipeError:
                break
    except Exception:
        # 读异常时不尝试整文件回读，直接终止以避免内存峰值
        pass
    try:
        if hasattr(dst, 'flush'):
            dst.flush()
    except Exception:
        pass
