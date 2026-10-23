# -*- coding: utf-8 -*-
"""
CSV处理器
- 提供 CSV 文件对象的流式复制处理
"""

import time

from utils import emit_progress_ex
import process as pm


def stream_csv_fileobj_to_writer(fileobj, out_stream, progress_cb=None, bytes_total=None):
    """CSV 文件对象直接复制到输出（提供统一接口），自动补尾换行。
    修复：在取消或写入错误时停止并不回读整文件，防止内存泄漏。
    """
    last_byte = None
    done = 0
    start_ns = time.perf_counter_ns()
    try:
        while True:
            if pm.cancel_requested:
                # 取消时尽快释放管道资源
                try:
                    if hasattr(out_stream, 'flush'):
                        out_stream.flush()
                except Exception:
                    pass
                try:
                    if hasattr(out_stream, 'close'):
                        out_stream.close()
                except Exception:
                    pass
                break
            try:
                chunk = fileobj.read(64 * 1024)
            except Exception:
                # 读异常：直接停止，避免后续整文件回读
                break
            if not chunk:
                break
            try:
                out_stream.write(chunk)
            except BrokenPipeError:
                # 写端已关闭（常见于取消），停止即可
                break
            except Exception:
                # 写异常：停止，避免触发整文件读入造成泄漏
                break
            done += len(chunk)
            if progress_cb:
                elapsed_ms = int((time.perf_counter_ns() - start_ns) / 1_000_000)
                try:
                    progress_cb(done, bytes_total, elapsed_ms)
                except Exception:
                    pass
            try:
                last_byte = chunk[-1]
            except Exception:
                pass
        # 仅在未取消且最后字节不是换行时补尾
        if (not pm.cancel_requested) and (last_byte is not None) and (last_byte != 0x0A):
            try:
                out_stream.write(b'\n')
            except Exception:
                pass
    except Exception:
        # 顶层异常：安全停止，不做任何整文件读取以避免泄漏
        pass
