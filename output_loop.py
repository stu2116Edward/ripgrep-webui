# -*- coding: utf-8 -*-
"""
输出解析与导出模块
- 解析 ripgrep 的 JSON 输出，管理上下文与进度，并写入导出流
- 与 search_engine 解耦为独立函数，便于复用与维护
"""

import time
import os
import json
import gc

from utils import (
    get_app, emit_message_utf, emit_progress_ex, sanitize_keyword, classify_file_type,
    trim_process_memory, aggressive_memory_reclaim, drop_file_cache_path,
)
from config import (
    NON_PREVIEW_LIGHT_TRIM_INTERVAL_MS,
    NON_PREVIEW_AGGRESSIVE_TRIM_INTERVAL_MS,
    SCAN_FILE_CACHE_DROP_INTERVAL_MS,
    SEARCH_FINAL_RECLAIM_ENABLED,
    FINAL_AGGRESSIVE_RECLAIM_REPEATS,
    FINAL_AGGRESSIVE_RECLAIM_SLEEP_MS,
    SEARCH_FINAL_DROP_EXPORT_CACHE_ENABLED,
)
from export_manager import append_export_text, close_export_stream, get_exports_dir, get_latest_export_filename
import process_manager as pm


def run_output_loop(
    q,
    total_files: int,
    total_procs: int,
    scope: str,
    keyword: str,
    context_before: int,
    context_after: int,
    count_only: bool,
    request_start_ns: int,
    extra_procs_local: list,
    forward_threads_local: list,
    final_all: bool,
):
    """
    解析 ripgrep 的 JSON 输出并进行上下文/进度/导出管理。

    参数说明：
    - q: 前向线程写入的队列，元素为 (raw_line_bytes, owner_pid)
    - total_files: 总文件数（用于进度）
    - total_procs: rg 进程总数（用于判断 EOF 完结）
    - scope: 导出范围 'single' 或 'all'
    - keyword: 检索关键字（用于导出文件命名）
    - context_before/context_after: 匹配前后上下文行数
    - count_only: 仅统计匹配数而不输出预览
    - request_start_ns: 请求起始时间（perf_counter_ns）
    - extra_procs_local: 附加子进程列表（用于清理）
    - forward_threads_local: 前向读取线程列表（用于等待结束）
    - final_all: 'all' 范围在该批次是否最终关闭导出
    """

    safe_kw = sanitize_keyword(keyword)
    match_count = 0
    files_done = 0

    try:
        with get_app().app_context():
            context_before_n = context_before if context_before and context_before > 0 else 0
            context_after_n = context_after if context_after and context_after > 0 else 0

            before_lines = []
            after_lines = []
            block_main = None
            block_ready = False
            after_emit_count = 0
            block_match_count = 0
            first_block = True
            search_start_ns = {}
            last_progress_tick_ns = 0
            # 非预览模式下的周期性轻量内存修剪节流
            last_trim_tick_ns = 0
            # 非预览模式下的周期性“增强回收”节流（含 drop_caches 尝试），避免过于频繁
            last_aggressive_tick_ns = 0
            # 针对当前正在检索的文件，按 owner 记录页面缓存丢弃的节流时间
            last_cache_drop_ns_map = {}
            owner_current_label = {}
            owner_has_output = {}

            try:
                elapsed_ms_total = int((time.perf_counter_ns() - request_start_ns) / 1_000_000)
            except Exception:
                elapsed_ms_total = 0
            emit_progress_ex(matches=match_count, files_total=total_files, files_done=files_done, elapsed_ms=elapsed_ms_total)

            eof_set = set()
            while True:
                if pm.cancel_requested:
                    break
                try:
                    raw_item, owner = q.get(timeout=0.05)
                except Exception:
                    if pm.cancel_requested:
                        break
                    # 仅当所有 RG 读取线程已发出 EOF、所有附加进程结束且队列为空时再退出
                    all_extra_ended = True
                    for p in extra_procs_local:
                        try:
                            if p.poll() is None:
                                all_extra_ended = False
                                break
                        except Exception:
                            pass
                    rg_all_eof = (len(eof_set) >= total_procs)
                    if rg_all_eof and all_extra_ended and q.empty():
                        break
                    # 周期性发送进度
                    try:
                        now_ns = time.perf_counter_ns()
                        if last_progress_tick_ns == 0 or (now_ns - last_progress_tick_ns) >= 200_000_000:
                            elapsed_ms_total = int((now_ns - request_start_ns) / 1_000_000)
                            emit_progress_ex(matches=match_count, files_total=total_files, files_done=files_done, elapsed_ms=elapsed_ms_total)
                            last_progress_tick_ns = now_ns
                        # 在非预览（仅计数）模式中，根据配置节流执行轻量与增强回收，避免内存攀升
                        if count_only:
                            light_trim_interval_ns = max(0, int(NON_PREVIEW_LIGHT_TRIM_INTERVAL_MS)) * 1_000_000
                            if light_trim_interval_ns > 0 and (last_trim_tick_ns == 0 or (now_ns - last_trim_tick_ns) >= light_trim_interval_ns):
                                try:
                                    gc.collect()
                                except Exception:
                                    pass
                                try:
                                    trim_process_memory()
                                except Exception:
                                    pass
                                last_trim_tick_ns = now_ns
                            # 周期性进行一次增强回收（包含 drop_caches 尝试），在容器中更有效
                            aggressive_interval_ns = max(0, int(NON_PREVIEW_AGGRESSIVE_TRIM_INTERVAL_MS)) * 1_000_000
                            if aggressive_interval_ns > 0 and (last_aggressive_tick_ns == 0 or (now_ns - last_aggressive_tick_ns) >= aggressive_interval_ns):
                                try:
                                    aggressive_memory_reclaim()
                                except Exception:
                                    pass
                                last_aggressive_tick_ns = now_ns
                            # 定期丢弃当前检索中文件的页面缓存，抑制大文件 page cache
                            try:
                                for owner_id, label_text in list(owner_current_label.items()):
                                    if not label_text:
                                        continue
                                    last_ns = last_cache_drop_ns_map.get(owner_id, 0)
                                    drop_interval_ns = max(0, int(SCAN_FILE_CACHE_DROP_INTERVAL_MS)) * 1_000_000
                                    if drop_interval_ns > 0 and (last_ns == 0 or (now_ns - last_ns) >= drop_interval_ns):
                                        try:
                                            drop_file_cache_path(label_text)
                                        except Exception:
                                            pass
                                        last_cache_drop_ns_map[owner_id] = now_ns
                            except Exception:
                                pass
                    except Exception:
                        pass
                    continue

                if raw_item is None:
                    eof_set.add(owner)
                    if len(eof_set) >= total_procs:
                        break
                    else:
                        continue

                try:
                    line = raw_item.decode('utf-8', errors='replace')
                except Exception:
                    continue
                if not line.strip():
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue

                typ = obj.get('type')
                if typ == 'begin':
                    if pm.cancel_requested:
                        break
                    if 'data' in obj and 'path' not in obj.get('data', {}) and owner in pm._proc_label_map:
                        try:
                            obj['data']['path'] = {'text': pm._proc_label_map.get(owner)}
                        except Exception:
                            pass
                    try:
                        search_start_ns[owner] = time.perf_counter_ns()
                    except Exception:
                        pass
                    try:
                        data_path = (obj.get('data') or {}).get('path') or {}
                        label_text = data_path.get('text') if isinstance(data_path, dict) else None
                    except Exception:
                        label_text = None
                    if not label_text:
                        label_text = pm._proc_label_map.get(owner)
                    if label_text:
                        owner_current_label[owner] = label_text
                        try:
                            if str(label_text).strip().lower() != '<stdin>':
                                append_export_text(safe_kw, f"[{label_text}]\n", scope=scope)
                        except Exception:
                            pass
                    owner_has_output[owner] = False
                    before_lines = []
                    after_lines = []
                    block_main = None
                    block_ready = False
                    block_match_count = 0

                elif typ == 'context':
                    if pm.cancel_requested:
                        break
                    data = obj.get('data', {})
                    line_text = (data.get('lines', {}).get('text', '') or '').strip()
                    if not block_ready:
                        before_lines.append(line_text)
                        if len(before_lines) > context_before_n:
                            before_lines.pop(0)
                    else:
                        append_export_text(safe_kw, line_text + '\n', scope=scope)
                        if not count_only:
                            emit_message_utf(line_text + '\n')
                        owner_has_output[owner] = True
                        after_emit_count += 1
                        if after_emit_count >= context_after_n:
                            if not count_only:
                                match_count += 1
                                try:
                                    elapsed_ms_total = int((time.perf_counter_ns() - request_start_ns) / 1_000_000)
                                except Exception:
                                    elapsed_ms_total = 0
                                emit_progress_ex(matches=match_count, files_total=total_files, files_done=files_done, elapsed_ms=elapsed_ms_total)
                            # 完成当前块并重置状态，下一块按“非首块”处理插入空行
                            first_block = False
                            try:
                                before_lines.append(block_main)
                                if len(before_lines) > context_before_n:
                                    before_lines.pop(0)
                            except Exception:
                                pass
                            block_ready = False
                            block_main = None
                            after_emit_count = 0
                            after_lines = []
                            block_match_count = 0

                elif typ == 'match':
                    if pm.cancel_requested:
                        break
                    data = obj.get('data', {})
                    line_text = (data.get('lines', {}).get('text', '') or '').strip()
                    if not block_ready:
                        if count_only:
                            match_count += 1
                            try:
                                elapsed_ms_total = int((time.perf_counter_ns() - request_start_ns) / 1_000_000)
                            except Exception:
                                elapsed_ms_total = 0
                            emit_progress_ex(matches=match_count, files_total=total_files, files_done=files_done, elapsed_ms=elapsed_ms_total)
                            # 写入与预览一致的分隔与前后文，并开启 block 以便 after 上下文写入
                            try:
                                if not first_block:
                                    append_export_text(safe_kw, '\n', scope=scope)
                                for t in before_lines:
                                    append_export_text(safe_kw, t + '\n', scope=scope)
                                append_export_text(safe_kw, line_text + '\n', scope=scope)
                                owner_has_output[owner] = True
                                block_main = line_text
                                block_ready = True
                                after_emit_count = 0
                                block_match_count = 1
                                after_lines = []
                            except Exception:
                                pass
                            # 首次块写入完成后再标记，避免在开头插入空行
                            first_block = False
                            if context_after_n == 0:
                                block_ready = False
                                block_main = None
                                before_lines = []
                                after_emit_count = 0
                                after_lines = []
                                block_match_count = 0
                        else:
                            if not first_block:
                                try:
                                    append_export_text(safe_kw, '\n', scope=scope)
                                    emit_message_utf('\n')
                                    owner_has_output[owner] = True
                                except Exception:
                                    pass
                            for t in before_lines:
                                append_export_text(safe_kw, t + '\n', scope=scope)
                                emit_message_utf(t + '\n')
                                owner_has_output[owner] = True
                            append_export_text(safe_kw, line_text + '\n', scope=scope)
                            emit_message_utf(line_text + '\n')
                            owner_has_output[owner] = True
                            block_main = line_text
                            block_ready = True
                            after_emit_count = 0
                            block_match_count = 1
                            after_lines = []
                            if context_after_n == 0:
                                match_count += 1
                                try:
                                    elapsed_ms_total = int((time.perf_counter_ns() - request_start_ns) / 1_000_000)
                                except Exception:
                                    elapsed_ms_total = 0
                                emit_progress_ex(matches=match_count, files_total=total_files, files_done=files_done, elapsed_ms=elapsed_ms_total)
                                first_block = False
                                block_ready = False
                                block_main = None
                                before_lines = []
                                after_emit_count = 0
                                after_lines = []
                                block_match_count = 0
                    else:
                        try:
                            while after_emit_count < context_after_n:
                                append_export_text(safe_kw, '\n', scope=scope)
                                if not count_only:
                                    emit_message_utf('\n')
                                owner_has_output[owner] = True
                                after_emit_count += 1
                        except Exception:
                            pass
                        # 无论是否仅计数模式，此处都在“补齐上一块 after 上下文”后统计上一块匹配
                        match_count += 1
                        try:
                            elapsed_ms_total = int((time.perf_counter_ns() - request_start_ns) / 1_000_000)
                        except Exception:
                            elapsed_ms_total = 0
                        emit_progress_ex(matches=match_count, files_total=total_files, files_done=files_done, elapsed_ms=elapsed_ms_total)
                        first_block = False

                        for i in range(context_before_n):
                            t = before_lines[i] if i < len(before_lines) else ''
                            append_export_text(safe_kw, t + '\n', scope=scope)
                            if not count_only:
                                emit_message_utf(t + '\n')
                            owner_has_output[owner] = True
                        append_export_text(safe_kw, line_text + '\n', scope=scope)
                        if not count_only:
                            emit_message_utf(line_text + '\n')
                        owner_has_output[owner] = True
                        block_main = line_text
                        block_ready = True
                        after_emit_count = 0
                        block_match_count = 1
                        after_lines = []

                if typ == 'end':
                    # 结束当前文件：完成 after 缓冲并统计
                    if block_ready:
                        try:
                            while after_emit_count < context_after_n:
                                append_export_text(safe_kw, '\n', scope=scope)
                                if not count_only:
                                    emit_message_utf('\n')
                                owner_has_output[owner] = True
                                after_emit_count += 1
                        except Exception:
                            pass
                        if not count_only:
                            match_count += 1
                            try:
                                elapsed_ms_total = int((time.perf_counter_ns() - request_start_ns) / 1_000_000)
                            except Exception:
                                elapsed_ms_total = 0
                            emit_progress_ex(matches=match_count, files_total=total_files, files_done=files_done, elapsed_ms=elapsed_ms_total)
                        first_block = False
                        block_ready = False
                        block_main = None
                        before_lines = []
                        after_lines = []
                        after_emit_count = 0
                        block_match_count = 0

                    files_done += 1
                    try:
                        elapsed_ms_total = int((time.perf_counter_ns() - request_start_ns) / 1_000_000)
                    except Exception:
                        elapsed_ms_total = 0
                    emit_progress_ex(matches=match_count, files_total=total_files, files_done=files_done, elapsed_ms=elapsed_ms_total)

                    try:
                        if owner_has_output.get(owner):
                            append_export_text(safe_kw, '\n', scope=scope)
                            if not count_only:
                                emit_message_utf('\n')
                            owner_has_output[owner] = False
                    except Exception:
                        pass

                    try:
                        ns = search_start_ns.pop(owner, None)
                        if ns is not None:
                            elapsed_ms = int((time.perf_counter_ns() - ns) / 1_000_000)
                        else:
                            elapsed_ms = 0
                        label_text = owner_current_label.get(owner) or pm._proc_label_map.get(owner)
                        ft = classify_file_type(label_text.lower()) if label_text else None
                        emit_progress_ex(
                            phase='search_end',
                            file_type=ft,
                            elapsed_ms=elapsed_ms,
                            files_total=total_files,
                            files_done=files_done,
                            matches=match_count,
                            label=label_text,
                        )
                    except Exception:
                        pass

                    # 每个文件检索完成后主动回收局部状态并进行内存修剪
                    try:
                        # 清理与该 owner 相关的临时映射，避免集合无限增长
                        try:
                            owner_current_label.pop(owner, None)
                        except Exception:
                            pass
                        try:
                            last_cache_drop_ns_map.pop(owner, None)
                        except Exception:
                            pass
                        try:
                            owner_has_output.pop(owner, None)
                        except Exception:
                            pass
                        try:
                            pm._proc_label_map.pop(owner, None)
                        except Exception:
                            pass
                        # 释放可能较大的上下文缓存
                        try:
                            before_lines = []
                            after_lines = []
                            block_main = None
                            block_ready = False
                            block_match_count = 0
                            after_emit_count = 0
                        except Exception:
                            pass
                        # 触发一次垃圾回收与进程内存修剪（轻量）
                        try:
                            gc.collect()
                        except Exception:
                            pass
                        try:
                            trim_process_memory()
                        except Exception:
                            pass
                    except Exception:
                        pass

            # 循环结束后发送最终进度
            try:
                elapsed_ms_total = int((time.perf_counter_ns() - request_start_ns) / 1_000_000)
            except Exception:
                elapsed_ms_total = 0
            emit_progress_ex(matches=match_count, files_total=total_files, files_done=files_done, elapsed_ms=elapsed_ms_total)
    finally:
        # 最终清理：临时目录、导出流、主进程标记
        try:
            pm.cleanup_temp_dirs()
        except Exception:
            pass
        try:
            pm._proc_label_map.clear()
        except Exception:
            pass
        # 清理可能累积的全局附加进程引用
        try:
            pm.extra_procs.clear()
        except Exception:
            pass
        # 等待前向读取线程结束：更稳健地等待直至退出
        try:
            for t in forward_threads_local:
                try:
                    total_wait = 0.0
                    while getattr(t, 'is_alive', lambda: False)() and total_wait < 2.0:
                        try:
                            t.join(timeout=0.1)
                        except Exception:
                            pass
                        total_wait += 0.1
                except Exception:
                    pass
        except Exception:
            pass
        # 所有前向线程均结束后再关闭导出流
        try:
            if scope == 'single':
                close_export_stream(sanitize_keyword(keyword), scope='single')
            elif scope == 'all' and final_all:
                close_export_stream(sanitize_keyword(keyword), scope='all')
        except Exception:
            pass
        # 在导出流关闭后尝试丢弃导出文件的页面缓存（可配置），降低容器 page cache
        try:
            if SEARCH_FINAL_DROP_EXPORT_CACHE_ENABLED:
                exports_dir = get_exports_dir()
                if scope == 'single':
                    fn = get_latest_export_filename(sanitize_keyword(keyword), scope='single')
                    if fn:
                        drop_file_cache_path(os.path.join(exports_dir, fn))
                elif scope == 'all' and final_all:
                    fn = get_latest_export_filename(sanitize_keyword(keyword), scope='all')
                    if fn:
                        drop_file_cache_path(os.path.join(exports_dir, fn))
        except Exception:
            pass
        # 在关闭导出后再终止额外进程
        try:
            for p in extra_procs_local:
                try:
                    if p.poll() is None:
                        try:
                            pm._terminate_proc(p)
                        except Exception:
                            pass
                except Exception:
                    pass
        except Exception:
            pass

        # 所有资源清理完毕后再释放 Busy 信号
        pm.proc = None

        # 主动排空内部队列
        try:
            while not q.empty():
                try:
                    q.get_nowait()
                except Exception:
                    break
        except Exception:
            pass

        # 清理本地大对象引用，帮助垃圾回收尽快回收
        try:
            try:
                forward_threads_local[:] = []
            except Exception:
                pass
            try:
                extra_procs_local[:] = []
            except Exception:
                pass
        except Exception:
            pass

        try:
            gc.collect()
        except Exception:
            pass
        try:
            trim_process_memory()
        except Exception:
            pass
        # 容器环境下执行更强的回收尝试：按配置重复与休眠
        try:
            if SEARCH_FINAL_RECLAIM_ENABLED:
                repeats = max(1, int(FINAL_AGGRESSIVE_RECLAIM_REPEATS))
                sleep_ms = max(0, int(FINAL_AGGRESSIVE_RECLAIM_SLEEP_MS))
                for _ in range(repeats):
                    try:
                        aggressive_memory_reclaim()
                    except Exception:
                        pass
                    try:
                        if sleep_ms > 0:
                            time.sleep(sleep_ms / 1000.0)
                    except Exception:
                        pass
                    try:
                        gc.collect()
                    except Exception:
                        pass
                    try:
                        trim_process_memory()
                    except Exception:
                        pass
        except Exception:
            pass

        try:
            elapsed_ms_total = int((time.perf_counter_ns() - request_start_ns) / 1_000_000)
        except Exception:
            elapsed_ms_total = 0
        try:
            emit_progress_ex(matches=match_count, files_total=total_files, files_done=files_done, elapsed_ms=elapsed_ms_total)
        except Exception:
            pass

        # 可配置的休眠已在上面的循环中进行；此处无需固定等待

        # 再次进行一次轻量修剪，确保完成后工作集回落到基线
        try:
            gc.collect()
        except Exception:
            pass
        try:
            trim_process_memory()
        except Exception:
            pass

        try:
            elapsed_ms_total = int((time.perf_counter_ns() - request_start_ns) / 1_000_000)
        except Exception:
            elapsed_ms_total = 0
        try:
            emit_progress_ex(matches=match_count, files_total=total_files, files_done=files_done, elapsed_ms=elapsed_ms_total)
        except Exception:
            pass

        try:
            emit_message_utf('Done\n')
        except Exception:
            pass
