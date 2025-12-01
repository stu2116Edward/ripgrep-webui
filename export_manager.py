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
    """统一导出目录选择：容器优先使用 /app/exports，本地环境回退到项目内 exports 目录。"""
    try:
        base = _EXPORTS_DIR
        # 优先尝试创建容器卷目录
        try:
            os.makedirs(base, exist_ok=True)
        except Exception:
            base = os.path.join(os.path.dirname(__file__), 'exports')
            try:
                os.makedirs(base, exist_ok=True)
            except Exception:
                pass

        # 如果不可写则回退到项目本地目录
        try:
            if not os.access(base, os.W_OK):
                fallback = os.path.join(os.path.dirname(__file__), 'exports')
                os.makedirs(fallback, exist_ok=True)
                base = fallback
        except Exception:
            pass

        return base
    except Exception:
        # 兜底：始终尝试项目内 exports
        fallback = os.path.join(os.path.dirname(__file__), 'exports')
        try:
            os.makedirs(fallback, exist_ok=True)
        except Exception:
            pass
        return fallback


def start_export_stream(safe_kw: str, scope: str = 'single'):
    """启动（或重新启动）导出文件写入流。
    - 'all' 模式：按时间戳创建 <keyword>__all_<YYYY-MM-DD>_<ts>.txt，复用同一会话的写入流，采用追加写入。
    - 'single' 模式：按时间戳创建 <keyword>__single_<YYYY-MM-DD>_<ts>.txt，覆盖写入新文件。
    返回文件路径或 None。
    """
    import datetime
    try:
        # 若已存在同关键字的导出流，先关闭以避免线程与句柄泄漏
        try:
            if safe_kw in export_streams:
                # 始终关闭旧会话，确保新文件创建（避免跨提交追加）
                close_export_stream(safe_kw)
        except Exception:
            pass

        exports_dir = get_exports_dir()
        os.makedirs(exports_dir, exist_ok=True)
        today = datetime.datetime.now().strftime('%Y-%m-%d')
        ts = int(time.time())
        # 区分文件名：all 模式稳定名，single 模式时间戳名
        if scope not in ('single', 'all'):
            scope = 'single'
        if scope == 'all':
            filename = f"{safe_kw}__all_{today}_{ts}.txt"
        else:
            filename = f"{safe_kw}__single_{today}_{ts}.txt"
        filepath = os.path.join(exports_dir, filename)
        # all 采用追加模式，single 采用覆盖模式
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
                        try:
                            fh.flush()
                        except Exception:
                            pass
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
        export_streams[safe_kw] = {'fh': fh, 'path': filepath, 'queue': q, 'thread': t, 'scope': scope}
        return filepath
    except Exception:
        return None


def append_export_text(safe_kw: str, text: str, scope: str = None):
    """追加写入文本到导出文件。
    若未初始化则按给定 scope 创建；scope 为 None 时回退 'single'。
    """
    try:
        info = export_streams.get(safe_kw)
        if not info or not info.get('queue'):
            start_export_stream(safe_kw, scope=(scope if scope in ('single', 'all') else 'single'))
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
