# -*- coding: utf-8 -*-
"""
导出逻辑模块
- 管理导出文件目录与流式写入
- 以关键字（安全化）为粒度进行独立的后台写盘
"""

import os
import time
import queue
import threading

# 后台导出文件的流式写入句柄（按安全化后的关键字区分）
export_streams = {}
_EXPORTS_DIR = '/app/exports'


def get_exports_dir():
    """统一导出目录选择：固定为 /app/exports（Docker 容器卷）。"""
    try:
        os.makedirs(_EXPORTS_DIR, exist_ok=True)
    except Exception:
        pass
    return _EXPORTS_DIR


def start_export_stream(safe_kw: str):
    """启动一个新的导出文件写入流（每次搜索唯一文件）。返回文件路径或 None。"""
    import datetime
    try:
        exports_dir = get_exports_dir()
        os.makedirs(exports_dir, exist_ok=True)
        today = datetime.datetime.now().strftime('%Y-%m-%d')
        ts = int(time.time())
        filename = f"{safe_kw}_{today}_{ts}.txt"
        filepath = os.path.join(exports_dir, filename)
        fh = open(filepath, 'w', encoding='utf-8')
        q = queue.Queue()

        def _writer_loop():
            try:
                while True:
                    item = q.get()
                    if item is None:
                        break
                    if not isinstance(item, str):
                        try:
                            item = str(item)
                        except Exception:
                            item = ''
                    try:
                        sanitized = item.encode('utf-8', errors='replace').decode('utf-8', errors='replace')
                        fh.write(sanitized)
                    except Exception:
                        pass
            finally:
                try:
                    fh.flush()
                except Exception:
                    pass
                try:
                    fh.close()
                except Exception:
                    pass

        t = threading.Thread(target=_writer_loop, daemon=True)
        t.start()
        export_streams[safe_kw] = {'fh': fh, 'path': filepath, 'queue': q, 'thread': t}
        return filepath
    except Exception:
        return None


def append_export_text(safe_kw: str, text: str):
    """追加写入文本到导出文件（若未初始化则尝试创建）。"""
    try:
        info = export_streams.get(safe_kw)
        if not info or not info.get('queue'):
            start_export_stream(safe_kw)
            info = export_streams.get(safe_kw)
        q = info and info.get('queue')
        if not q:
            return
        q.put(text)
    except Exception:
        pass


def close_export_stream(safe_kw: str):
    """关闭指定关键字的导出流。"""
    try:
        info = export_streams.pop(safe_kw, None)
        if not info:
            return
        q = info.get('queue')
        t = info.get('thread')
        try:
            if q:
                q.put(None)
        except Exception:
            pass
        try:
            if t:
                t.join(timeout=1.0)
        except Exception:
            pass
        fh = info.get('fh')
        if fh:
            try:
                fh.flush()
                try:
                    os.fsync(fh.fileno())
                except Exception:
                    pass
            except Exception:
                pass
            try:
                fh.close()
            except Exception:
                pass
    except Exception:
        pass


def close_all_export_streams():
    """关闭所有导出流（用于取消或搜索结束清理）。"""
    try:
        for safe_kw in list(export_streams.keys()):
            close_export_stream(safe_kw)
        export_streams.clear()
    except Exception:
        pass