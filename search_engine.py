# -*- coding: utf-8 -*-
"""
搜索逻辑模块
- 负责 /search 请求的整个管道：构建 rg 命令、查询框架
- 调用feed.py模块流式喂入 ripgrep，调用output_loop.py模块输出解析与导出
"""

import os
import time
import queue
import threading

from config import DEFAULT_DATA_DIR, TEXT_EXTS, SEARCH_RG_QUEUE_MAXSIZE
from utils import (
    get_socketio, emit_message_utf, has_cmd,
    sanitize_keyword,
    is_single_file_compressed, is_archive_multi_file, is_excel_file, is_csv_file,
    trim_process_memory,
)
from export_manager import start_export_stream, close_export_stream
from feed import handle_single_file as feed_handle_single_file
import process_manager as pm
from output_loop import run_output_loop


def start_search(keyword: str, context_before: int, context_after: int, file: str, scope_override: str = None, reset_all: bool = False, final_all: bool = False, count_only: bool = False):
    """
    启动搜索：复用原始流程与细节，返回字符串状态（'Started' 或错误字符串）。
    """
    # 使用启动锁与原子 Busy 标记，确保串行检索
    try:
        with pm._search_lock:
            if pm.proc is not None:
                try:
                    emit_message_utf('Busy\n')
                except Exception:
                    pass
                return "Busy"
            pm.proc = 'starting'
    except Exception:
        pass

    # 在开始新搜索时重置取消标志，并进行一次轻量内存修剪
    pm.cancel_requested = False
    try:
        trim_process_memory()
    except Exception:
        pass

    data_dir = DEFAULT_DATA_DIR
    if not os.path.isdir(data_dir):
        data_dir = os.path.dirname(__file__)
    data_dir_abs = os.path.abspath(data_dir)


    # 基本 rg 参数
    rg_base = ['rg', '-uuu', '--smart-case', '--json']

    # 确认 rg 可用
    if not has_cmd('rg'):
        emit_message_utf('ripgrep 未安装或不可用，请在系统 PATH 中提供 rg。')
        try:
            pm.proc = None
        except Exception:
            pass
        return "rg not found"

    # 预创建/管理导出写入流
    try:
        safe_kw_init = sanitize_keyword(keyword)
        scope = 'all' if (file == '__ALL__') else 'single'
        if scope_override in ('all', 'single'):
            scope = scope_override
        if scope == 'all':
            if reset_all:
                try:
                    close_export_stream(safe_kw_init, scope='all')
                except Exception:
                    pass
                start_export_stream(safe_kw_init, scope='all')
            else:
                # 复用现有会话：若尚未初始化，将在首次 append 时懒加载
                pass
        else:
            start_export_stream(safe_kw_init, scope='single')
    except Exception:
        pass

    # 上下文参数
    if context_before and context_before > 0:
        rg_base += ['-B', str(context_before)]
    if context_after and context_after > 0:
        rg_base += ['-A', str(context_after)]

    # 计算搜索路径与可能的 basename glob
    include_glob_for_basename = None
    if file:
        candidate_norm = os.path.normpath(os.path.join(data_dir_abs, file))
        candidate_abs = os.path.abspath(candidate_norm)
        if candidate_abs.startswith(data_dir_abs + os.sep) or candidate_abs == data_dir_abs:
            if os.path.exists(candidate_abs):
                search_path = candidate_abs
            else:
                base_only = (os.path.basename(file) == file) and ('/' not in file) and ('\\' not in file)
                if base_only:
                    include_glob_for_basename = os.path.basename(file)
                    search_path = data_dir_abs
                else:
                    search_path = os.path.join(data_dir_abs, os.path.basename(file))
        else:
            search_path = os.path.join(data_dir_abs, os.path.basename(file))
    else:
        search_path = data_dir_abs

    if include_glob_for_basename:
        rg_base += ['--glob', f'**/{include_glob_for_basename}']

    # 启动并管理若干 rg 进程
    all_rg_procs = []
    extra_procs_local = []
    forward_threads_local = []
    total_files = 0

    try:
        # 使用有界队列限制rg输出缓冲，施加背压避免内存增长（从配置读取）
        q = queue.Queue(maxsize=SEARCH_RG_QUEUE_MAXSIZE)

        def forward_proc_stdout(p):
            pid = getattr(p, 'pid', id(p))
            try:
                if p.stdout is None:
                    return
                for raw in p.stdout:
                    if pm.cancel_requested:
                        break
                    q.put((raw, pid))
            except Exception:
                pass
            finally:
                try:
                    s = getattr(p, 'stdout', None)
                    if s:
                        try:
                            s.close()
                        except Exception:
                            pass
                except Exception:
                    pass
                q.put((None, pid))

        def _register_proc(rp):
            """统一注册进程到管理列表，返回 pid 或 None"""
            try:
                all_rg_procs.append(rp)
                extra_procs_local.append(rp)
                pm.extra_procs.append(rp)
                return getattr(rp, 'pid', None)
            except Exception:
                return None

        # Helper: 处理单个文件（委托给 feed 模块）
        def handle_single_file(search_path_local, basename_label=None):
            nonlocal total_files
            # 本地包装：启动前向读取线程
            def start_forward_thread(p):
                try:
                    t = threading.Thread(target=forward_proc_stdout, args=(p,), daemon=True)
                    t.start()
                    forward_threads_local.append(t)
                except Exception:
                    pass
            try:
                inc = feed_handle_single_file(
                    search_path_local,
                    basename_label or os.path.basename(search_path_local),
                    rg_base,
                    keyword,
                    start_forward_thread,
                    _register_proc,
                    extra_procs_local
                )
            except Exception:
                inc = 0
            total_files += (inc or 0)

        # 主体：分两类处理（文件或目录）
        if os.path.isfile(search_path):
            handle_single_file(search_path, basename_label=os.path.basename(search_path))
        else:
            # 遍历目录，先收集分类文件列表
            csv_files = []
            text_files = []
            other_regular_files = []
            compressed_files = []
            archive_files = []
            excel_files = []
            for root, _, fns in os.walk(search_path):
                for fn in fns:
                    if include_glob_for_basename and os.path.basename(fn) != include_glob_for_basename:
                        continue
                    full = os.path.join(root, fn)
                    fn_lower = fn.lower()
                    if is_archive_multi_file(fn_lower):
                        archive_files.append(full)
                    elif is_single_file_compressed(fn_lower):
                        compressed_files.append(full)
                    elif is_excel_file(fn_lower):
                        excel_files.append(full)
                    elif is_csv_file(fn_lower):
                        csv_files.append(full)
                    elif fn_lower.endswith(tuple(TEXT_EXTS)):
                        text_files.append(full)
                    else:
                        other_regular_files.append(full)

            # 目录扫描不再预估总数，按 feed 返回逐步累加

            # 优先处理归档（尽可能流式）
            for full_archive in archive_files:
                try:
                    handle_single_file(full_archive)
                except Exception:
                    continue

            # CSV -> 文本 -> other
            for full in csv_files:
                try:
                    handle_single_file(full)
                except Exception:
                    continue
            for full in text_files:
                try:
                    handle_single_file(full)
                except Exception:
                    continue
            for full in other_regular_files:
                try:
                    handle_single_file(full)
                except Exception:
                    continue

            # 压缩文件（单文件压缩）
            for full in compressed_files:
                try:
                    handle_single_file(full)
                except Exception:
                    continue

            # Excel 文件
            for full in excel_files:
                try:
                    handle_single_file(full)
                except Exception:
                    continue

        # 启动后续处理
        total_procs = len(all_rg_procs)
        if total_procs == 0:
            try:
                emit_message_utf('没有可搜索的文件或内容。\n')
            except Exception:
                pass
            try:
                if scope == 'single':
                    close_export_stream(sanitize_keyword(keyword), scope='single')
                elif scope == 'all' and final_all:
                    close_export_stream(sanitize_keyword(keyword), scope='all')
            except Exception:
                pass
            try:
                pm.extra_procs.clear()
            except Exception:
                pass
            try:
                pm._proc_label_map.clear()
            except Exception:
                pass
            # 在关闭导出流与清理完成后再释放 Busy 标志
            pm.proc = None
            return "Started"

        # 标记主进程（用于 /cancel 检测）
        try:
            pm.proc = all_rg_procs[0]
        except Exception:
            pm.proc = all_rg_procs[-1]

        request_start_ns = time.perf_counter_ns()

        # 启动后台输出线程
        try:
            t_loop = threading.Thread(
                target=run_output_loop,
                args=(
                    q,
                    total_files,
                    len(all_rg_procs),
                    scope,
                    keyword,
                    context_before,
                    context_after,
                    count_only,
                    request_start_ns,
                    extra_procs_local,
                    forward_threads_local,
                    final_all,
                ),
                daemon=True,
            )
            t_loop.start()
        except Exception as e:
            try:
                if get_socketio():
                    get_socketio().emit('message', {'message': f'Start failed: {e}\n'})
            except Exception:
                pass
            # 清理
            for p in extra_procs_local:
                try:
                    pm.terminate_proc(p)
                except Exception:
                    pass
            try:
                pm.cleanup_temp_dirs()
            except Exception:
                pass
            try:
                pm._proc_label_map.clear()
            except Exception:
                pass
            try:
                for t in forward_threads_local:
                    try:
                        t.join(timeout=0.5)
                    except Exception:
                        pass
            except Exception:
                pass
            # 先终止并清理额外进程，避免遗留数据竞争导出流
            try:
                for p in extra_procs_local:
                    try:
                        if p.poll() is None:
                            try:
                                pm.terminate_proc(p)
                            except Exception:
                                pass
                    except Exception:
                        pass
            except Exception:
                pass
            extra_procs_local = []
            try:
                pm.extra_procs.clear()
            except Exception:
                pass
            try:
                if scope == 'single':
                    close_export_stream(sanitize_keyword(keyword), scope='single')
                elif scope == 'all' and final_all:
                    close_export_stream(sanitize_keyword(keyword), scope='all')
            except Exception:
                pass
            # 保证导出流关闭完毕后再释放 Busy 标志
            pm.proc = None
            return "Error"

        return "Started"

    except Exception as e:
        try:
            if get_socketio():
                get_socketio().emit('message', {'message': f'Start failed: {e}\n'})
        except Exception:
            pass
        for p in extra_procs_local:
            try:
                pm.terminate_proc(p)
            except Exception:
                pass
        try:
            pm.cleanup_temp_dirs()
        except Exception:
            pass
        try:
            pm._proc_label_map.clear()
        except Exception:
            pass
        try:
            for t in forward_threads_local:
                try:
                    t.join(timeout=0.5)
                except Exception:
                    pass
        except Exception:
            pass
        # 先终止并清理额外进程，避免遗留数据竞争导出流
        try:
            for p in extra_procs_local:
                try:
                    if p.poll() is None:
                        try:
                            pm.terminate_proc(p)
                        except Exception:
                            pass
                except Exception:
                    pass
        except Exception:
            pass
        extra_procs_local = []
        try:
            pm.extra_procs.clear()
        except Exception:
            pass
        try:
            if scope == 'single':
                close_export_stream(sanitize_keyword(keyword), scope='single')
            elif scope == 'all' and final_all:
                close_export_stream(sanitize_keyword(keyword), scope='all')
        except Exception:
            pass
        # 保证导出流关闭完毕后再释放 Busy 标志
        pm.proc = None
        return "Error"
