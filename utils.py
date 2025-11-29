# -*- coding: utf-8 -*-
"""
工具函数模块
- 提供事件发送包装、关键字安全化、外部命令检测、进程创建标志等
- 维护全局 app 与 socketio 的上下文注入，避免循环依赖
"""

import os
import shutil
from functools import lru_cache

from config import (
    STREAM_CHUNK_SIZE,
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