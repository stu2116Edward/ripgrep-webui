# -*- coding: utf-8 -*-
"""
搜索逻辑模块
- 负责 /search 请求的整个管道：构建 rg 命令、按类型流式喂入、解析 rg JSON 输出、实时发送进度与导出
- 保持原有接口与行为，增加可靠性与可读性
"""

import os
import io
import time
import json
import queue
import shutil
import signal
import threading
import subprocess
import tempfile
import gc

from config import DEFAULT_DATA_DIR, EXCEL_EXTS, CSV_EXTS, TEXT_EXTS, SEARCH_RG_QUEUE_MAXSIZE
from utils import (
    get_app, get_socketio, emit_message_utf, emit_progress_ex, has_cmd,
    sanitize_keyword, classify_file_type, strip_single_compress_ext, popen_creationflags,
    is_single_file_compressed, is_archive_multi_file, is_excel_file, is_csv_file,
    trim_process_memory
)
from export_manager import start_export_stream, append_export_text, close_export_stream
from file_handlers import (
    start_rg_and_feed_python_stream, python_decompress_feed, build_decompress_command,
    list_7z_members, safe_extract_tar, safe_extract_zip, stream_excel_to_writer,
    stream_excel_bytes_to_writer, stream_csv_fileobj_to_writer,
    copy_fileobj_chunked, spool_stream_to_temp_then_stream_excel,
    try_py7zr_extract, try_rarfile_extract
)
import process_manager as pm

# 缓存 ripgrep 是否支持 --label 参数（None 表示尚未检测）
_RG_SUPPORTS_LABEL = None
_RG_LOCK = threading.Lock()


def check_rg_supports_label():
    """检测系统中 rg 的帮助输出里是否包含 --label，缓存结果。"""
    global _RG_SUPPORTS_LABEL
    with _RG_LOCK:
        if _RG_SUPPORTS_LABEL is not None:
            return _RG_SUPPORTS_LABEL
        try:
            p = subprocess.run(['rg', '--help'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)
            out = p.stdout.decode('utf-8', errors='replace')
            _RG_SUPPORTS_LABEL = ('--label' in out)
        except Exception:
            _RG_SUPPORTS_LABEL = False
        return _RG_SUPPORTS_LABEL


def start_search(keyword: str, context_before: int, context_after: int, file: str, scope_override: str = None, reset_all: bool = False, final_all: bool = False, count_only: bool = False):
    """
    启动搜索：复用原始流程与细节，返回字符串状态（'Started' 或错误字符串）。
    """
    # 只有在没有搜索正在进行时才继续
    if pm.proc is not None:
        try:
            emit_message_utf('Busy\n')
        except Exception:
            pass
        return "Busy"

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
                    close_export_stream(safe_kw_init)
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

        def start_rg_for_path(path, stdin_pipe=None, label=None, exclude_patterns=None, python_stream_feed=None):
            """启动 rg：支持从 stdin 流式喂入或直接传路径"""
            cmd = rg_base.copy()
            if exclude_patterns:
                for pat in exclude_patterns:
                    cmd += ['--glob', f'!{pat}']
            supports_label = check_rg_supports_label()
            if path == '-':
                cmd += ['-a']
            if label and supports_label:
                if path == '-':
                    cmd += ['--label', label, '--', keyword, '-']
                else:
                    cmd += ['--label', label, '--', keyword, path]
            else:
                if path == '-':
                    cmd += ['--', keyword, '-']
                else:
                    cmd += ['--', keyword, path]

            if python_stream_feed:
                rg_proc = start_rg_and_feed_python_stream(cmd, python_stream_feed)
            else:
                rg_proc = subprocess.Popen(
                    cmd,
                    stdin=stdin_pipe,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    shell=False,
                    preexec_fn=os.setpgrp if hasattr(os, "setpgrp") else None,
                    creationflags=popen_creationflags()
                )

            # 若 rg 不支持 --label，保存 pid->label 映射
            if label and not supports_label:
                try:
                    pid = getattr(rg_proc, 'pid', None)
                    if pid is not None:
                        pm._proc_label_map[pid] = label
                except Exception:
                    pass

            # 启动前向线程
            try:
                t = threading.Thread(target=forward_proc_stdout, args=(rg_proc,), daemon=True)
                t.start()
                forward_threads_local.append(t)
            except Exception:
                pass

            _register_proc(rg_proc)
            return rg_proc

        def schedule_temp_dir_cleanup_for_proc(proc, temp_dir):
            def _cleanup(p=proc, d=temp_dir):
                try:
                    if hasattr(p, 'wait'):
                        p.wait()
                except Exception:
                    pass
                try:
                    shutil.rmtree(d)
                except Exception:
                    pass
                try:
                    if d in pm.temp_dirs:
                        pm.temp_dirs.remove(d)
                except Exception:
                    pass
                try:
                    gc.collect()
                except Exception:
                    pass
            try:
                t = threading.Thread(target=_cleanup, daemon=True)
                t.start()
            except Exception:
                try:
                    if proc and getattr(proc, 'poll', lambda: None)() is not None:
                        try:
                            shutil.rmtree(temp_dir)
                        except Exception:
                            pass
                        try:
                            if temp_dir in pm.temp_dirs:
                                pm.temp_dirs.remove(temp_dir)
                        except Exception:
                            pass
                except Exception:
                    pass

        # Helper: 处理单个文件（包括压缩/归档/Excel）
        def handle_single_file(search_path_local, basename_label=None):
            nonlocal total_files
            file_lower = os.path.basename(search_path_local).lower()
            rel_label = basename_label or os.path.basename(search_path_local)

            # 单文件压缩：.gz/.bz2/.xz/.lz4 等
            if is_single_file_compressed(file_lower):
                label = rel_label
                inner_lower = strip_single_compress_ext(file_lower)
                # 优先使用外部解压器
                cmd = build_decompress_command(file_lower, search_path_local)
                if cmd:
                    try:
                        decompress_proc = subprocess.Popen(
                            cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.DEVNULL,
                            shell=False,
                            preexec_fn=os.setpgrp if hasattr(os, "setpgrp") else None,
                            creationflags=popen_creationflags()
                        )
                        extra_procs_local.append(decompress_proc)
                        pm.extra_procs.append(decompress_proc)
                        if is_excel_file(inner_lower):
                            def feed_fn(w, proc=decompress_proc, name=inner_lower, lb=label):
                                def _cb(done, total, elapsed_ms):
                                    emit_progress_ex(phase='decompress', file_type='compressed',
                                                     elapsed_ms=elapsed_ms, bytes_done=done, bytes_total=total,
                                                     label=lb)
                                try:
                                    spool_stream_to_temp_then_stream_excel(name, proc.stdout, w)
                                finally:
                                    try:
                                        proc.stdout.close()
                                    except Exception:
                                        pass
                                    try:
                                        proc.wait()
                                    except Exception:
                                        pass
                                    try:
                                        trim_process_memory()
                                    except Exception:
                                        pass
                            start_rg_for_path('-', label=label, python_stream_feed=feed_fn)
                        else:
                            def feed_fn(w, proc=decompress_proc, lb=label, orig=search_path_local):
                                size_c = None
                                try:
                                    size_c = os.path.getsize(orig)
                                except Exception:
                                    size_c = None
                                def _cb(done, total, elapsed_ms):
                                    emit_progress_ex(phase='decompress', file_type='compressed',
                                                     elapsed_ms=elapsed_ms, bytes_done=done, bytes_total=total,
                                                     label=lb)
                                try:
                                    copy_fileobj_chunked(proc.stdout, w, progress_cb=_cb, bytes_total=size_c)
                                finally:
                                    try:
                                        proc.stdout.close()
                                    except Exception:
                                        pass
                                    try:
                                        proc.wait()
                                    except Exception:
                                        pass
                                    try:
                                        trim_process_memory()
                                    except Exception:
                                        pass
                            start_rg_for_path('-', label=label, python_stream_feed=feed_fn)
                    except Exception:
                        # 回退到 Python 解压
                        def feed_fn(w, p=search_path_local, e=file_lower):
                            python_decompress_feed(p, e, w)
                        start_rg_for_path('-', label=label, python_stream_feed=feed_fn)
                else:
                    # Python 解压
                    def feed_fn(w, p=search_path_local, e=file_lower):
                        python_decompress_feed(p, e, w)
                    start_rg_for_path('-', label=label, python_stream_feed=feed_fn)
                total_files += 1
                return

            # tar 系列归档流式处理
            if file_lower.endswith(('.tar.gz', '.tgz', '.tar.bz2', '.tbz2', '.tar.xz', '.txz', '.tar')):
                try:
                    import tarfile
                    if file_lower.endswith(('.tar.gz', '.tgz')):
                        tar_mode = 'r:gz'
                    elif file_lower.endswith(('.tar.bz2', '.tbz2')):
                        tar_mode = 'r:bz2'
                    elif file_lower.endswith(('.tar.xz', '.txz')):
                        tar_mode = 'r:xz'
                    else:
                        tar_mode = 'r'
                    
                    with tarfile.open(search_path_local, mode=tar_mode) as tar:
                        for member in tar.getmembers():
                            if member.isfile():
                                total_files += 1
                                label = f"{os.path.basename(search_path_local)}/{member.name}"
                                member_name = member.name
                                def feed_fn(w, p=search_path_local, mode=tar_mode, name=member_name, lb=label):
                                    import tarfile as _tarfile
                                    try:
                                        with _tarfile.open(p, mode=mode) as t2:
                                            f2 = t2.extractfile(name)
                                            if not f2:
                                                return
                                            lower = name.lower()
                                            def _cb(done, total, elapsed_ms):
                                                emit_progress_ex(
                                                    phase='decompress', file_type='archive',
                                                    elapsed_ms=elapsed_ms, bytes_done=done, bytes_total=total,
                                                    label=lb
                                                )
                                            if is_excel_file(lower):
                                                spool_stream_to_temp_then_stream_excel(lower, f2, w)
                                            elif is_csv_file(lower):
                                                stream_csv_fileobj_to_writer(f2, w, progress_cb=_cb)
                                            else:
                                                copy_fileobj_chunked(f2, w, progress_cb=_cb)
                                    except Exception:
                                        pass
                                start_rg_for_path('-', label=label, python_stream_feed=feed_fn)
                except Exception as e:
                    # tar 流式失败，回退到解包临时目录
                    temp_dir_for_archive = tempfile.mkdtemp(prefix='rg_archive_')
                    pm.temp_dirs.append(temp_dir_for_archive)
                    try:
                        import tarfile
                        with tarfile.open(search_path_local, 'r:*') as tf:
                            safe_extract_tar(tf, temp_dir_for_archive)
                        cnt = 0
                        for _, _, fns in os.walk(temp_dir_for_archive):
                            for _ in fns:
                                cnt += 1
                        total_files += cnt if cnt > 0 else 1
                        _p = start_rg_for_path(temp_dir_for_archive)
                        schedule_temp_dir_cleanup_for_proc(_p, temp_dir_for_archive)
                    except Exception:
                        try:
                            shutil.rmtree(temp_dir_for_archive)
                        except Exception:
                            pass
                        pm.temp_dirs.remove(temp_dir_for_archive)
                return

            # 归档（zip/rar/7z 等） -> 先尝试流式成员处理，再回退到解包临时目录
            if is_archive_multi_file(file_lower):
                # 处理 zip/jar/war
                if file_lower.endswith(('.zip', '.jar', '.war')):
                    try:
                        import zipfile
                        with zipfile.ZipFile(search_path_local, 'r') as zf:
                            infos = zf.infolist()
                            for info in infos:
                                if info.is_dir():
                                    continue
                                name = info.filename
                                size = info.file_size
                                label = (os.path.basename(search_path_local) + '/' + name)
                                def feed_fn(w, p=search_path_local, nm=name, lb=label, sz=size):
                                    import zipfile as _zipfile
                                    try:
                                        with _zipfile.ZipFile(p, 'r') as zf2:
                                            with zf2.open(nm, 'r') as member_f:
                                                lower = nm.lower()
                                                def _cb(done, total, elapsed_ms):
                                                    emit_progress_ex(phase='decompress', file_type='archive',
                                                                     elapsed_ms=elapsed_ms, bytes_done=done, bytes_total=total,
                                                                     label=lb)
                                                if is_excel_file(lower):
                                                    spool_stream_to_temp_then_stream_excel(lower, member_f, w)
                                                elif is_csv_file(lower):
                                                    stream_csv_fileobj_to_writer(member_f, w, progress_cb=_cb, bytes_total=sz)
                                                else:
                                                    copy_fileobj_chunked(member_f, w, progress_cb=_cb, bytes_total=sz)
                                    except Exception:
                                        pass
                                try:
                                    start_rg_for_path('-', label=label, python_stream_feed=feed_fn)
                                    total_files += 1
                                except Exception:
                                    pass
                    except Exception:
                        # zip 流式失败，回退到解包临时目录
                        temp_dir_for_archive = tempfile.mkdtemp(prefix='rg_archive_')
                        pm.temp_dirs.append(temp_dir_for_archive)
                        try:
                            import zipfile
                            with zipfile.ZipFile(search_path_local, 'r') as zf:
                                safe_extract_zip(zf, temp_dir_for_archive)
                            cnt = 0
                            for _, _, fns in os.walk(temp_dir_for_archive):
                                for _ in fns:
                                    cnt += 1
                            total_files += cnt if cnt > 0 else 1
                            _p = start_rg_for_path(temp_dir_for_archive)
                            schedule_temp_dir_cleanup_for_proc(_p, temp_dir_for_archive)
                        except Exception:
                            try:
                                shutil.rmtree(temp_dir_for_archive)
                            except Exception:
                                pass
                            pm.temp_dirs.remove(temp_dir_for_archive)
                # rar
                elif file_lower.endswith('.rar'):
                    streamed_local = False
                    try:
                        import rarfile
                        rf = rarfile.RarFile(search_path_local)
                        members = rf.infolist()
                        for mi in members:
                            if mi.isdir():
                                continue
                            name = mi.filename
                            label = os.path.basename(search_path_local) + '/' + name
                            def feed_fn(w, p=search_path_local, mi_local=mi, nm=name, lb=label):
                                import rarfile as _rarfile
                                try:
                                    with _rarfile.RarFile(p) as rf2:
                                        with rf2.open(mi_local) as member_f:
                                            lower = nm.lower()
                                            size = getattr(mi_local, 'file_size', None)
                                            def _cb(done, total, elapsed_ms):
                                                emit_progress_ex(phase='decompress', file_type='archive',
                                                                 elapsed_ms=elapsed_ms, bytes_done=done, bytes_total=total,
                                                                 label=lb)
                                            if is_excel_file(lower):
                                                spool_stream_to_temp_then_stream_excel(lower, member_f, w)
                                            elif is_csv_file(lower):
                                                stream_csv_fileobj_to_writer(member_f, w, progress_cb=_cb, bytes_total=size)
                                            else:
                                                copy_fileobj_chunked(member_f, w, progress_cb=_cb, bytes_total=size)
                                except Exception:
                                    pass
                            try:
                                start_rg_for_path('-', label=label, python_stream_feed=feed_fn)
                                total_files += 1
                            except Exception:
                                pass
                        streamed_local = True
                    except Exception:
                        streamed_local = False
                    
                    if not streamed_local and has_cmd('7z'):
                        try:
                            members = list_7z_members(search_path_local)
                            for m in members:
                                name = m.get('name')
                                size = m.get('size')
                                label = os.path.basename(search_path_local) + '/' + name
                                def feed_fn(w, nm=name, lb=label, sz=size):
                                    try:
                                        dec_cmd = ['7z', 'e', '-so', search_path_local, nm]
                                        dec_proc = subprocess.Popen(
                                            dec_cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                            shell=False, preexec_fn=os.setpgrp if hasattr(os, "setpgrp") else None,
                                            creationflags=popen_creationflags()
                                        )
                                        extra_procs_local.append(dec_proc)
                                        pm.extra_procs.append(dec_proc)
                                        lower = nm.lower()
                                        def _cb(done, total, elapsed_ms):
                                            emit_progress_ex(phase='decompress', file_type='archive',
                                                             elapsed_ms=elapsed_ms, bytes_done=done, bytes_total=total,
                                                             label=lb)
                                        if is_excel_file(lower):
                                            spool_stream_to_temp_then_stream_excel(lower, dec_proc.stdout, w)
                                        elif is_csv_file(lower):
                                            stream_csv_fileobj_to_writer(dec_proc.stdout, w, progress_cb=_cb, bytes_total=sz)
                                        else:
                                            copy_fileobj_chunked(dec_proc.stdout, w, progress_cb=_cb, bytes_total=sz)
                                        try:
                                            dec_proc.stdout.close()
                                        except Exception:
                                            pass
                                        try:
                                            dec_proc.wait()
                                        except Exception:
                                            pass
                                        try:
                                            trim_process_memory()
                                        except Exception:
                                            pass
                                    except Exception:
                                        pass
                                try:
                                    start_rg_for_path('-', label=label, python_stream_feed=feed_fn)
                                    total_files += 1
                                except Exception:
                                    pass
                            streamed_local = True
                        except Exception:
                            streamed_local = False

                    if not streamed_local:
                        # rar 流式失败，回退到解包临时目录
                        temp_dir_for_archive = tempfile.mkdtemp(prefix='rg_archive_')
                        pm.temp_dirs.append(temp_dir_for_archive)
                        extracted_ok = False
                        try:
                            ok, _ = try_rarfile_extract(search_path_local, temp_dir_for_archive)
                            if ok:
                                extracted_ok = True
                        except Exception:
                            extracted_ok = False

                        if not extracted_ok and has_cmd('7z'):
                            try:
                                extract_cmd = ['7z', 'x', '-y', search_path_local, f'-o{temp_dir_for_archive}']
                                extract_proc = subprocess.Popen(
                                    extract_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=False,
                                    preexec_fn=os.setpgrp if hasattr(os, "setpgrp") else None,
                                    creationflags=popen_creationflags()
                                )
                                extra_procs_local.append(extract_proc)
                                pm.extra_procs.append(extract_proc)
                                extract_proc.wait()
                                try:
                                    trim_process_memory()
                                except Exception:
                                    pass
                                if extract_proc.returncode == 0:
                                    extracted_ok = True
                            except Exception:
                                extracted_ok = False

                        if extracted_ok:
                            cnt = 0
                            for _, _, fns in os.walk(temp_dir_for_archive):
                                for _ in fns:
                                    cnt += 1
                            total_files += cnt if cnt > 0 else 1
                            _p = start_rg_for_path(temp_dir_for_archive)
                            schedule_temp_dir_cleanup_for_proc(_p, temp_dir_for_archive)
                        else:
                            try:
                                shutil.rmtree(temp_dir_for_archive)
                            except Exception:
                                pass
                            pm.temp_dirs.remove(temp_dir_for_archive)
                # 7z
                elif file_lower.endswith('.7z'):
                    streamed_local = False
                    if has_cmd('7z'):
                        try:
                            members = list_7z_members(search_path_local)
                            for m in members:
                                name = m.get('name')
                                size = m.get('size')
                                label = os.path.basename(search_path_local) + '/' + name
                                def feed_fn(w, nm=name, lb=label, sz=size):
                                    try:
                                        dec_cmd = ['7z', 'e', '-so', search_path_local, nm]
                                        dec_proc = subprocess.Popen(
                                            dec_cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                            shell=False, preexec_fn=os.setpgrp if hasattr(os, "setpgrp") else None,
                                            creationflags=popen_creationflags()
                                        )
                                        extra_procs_local.append(dec_proc)
                                        pm.extra_procs.append(dec_proc)
                                        lower = nm.lower()
                                        def _cb(done, total, elapsed_ms):
                                            emit_progress_ex(phase='decompress', file_type='archive',
                                                             elapsed_ms=elapsed_ms, bytes_done=done, bytes_total=total,
                                                             label=lb)
                                        if is_excel_file(lower):
                                            spool_stream_to_temp_then_stream_excel(lower, dec_proc.stdout, w)
                                        elif is_csv_file(lower):
                                            stream_csv_fileobj_to_writer(dec_proc.stdout, w, progress_cb=_cb, bytes_total=sz)
                                        else:
                                            copy_fileobj_chunked(dec_proc.stdout, w, progress_cb=_cb, bytes_total=sz)
                                        try:
                                            dec_proc.stdout.close()
                                        except Exception:
                                            pass
                                        try:
                                            dec_proc.wait()
                                        except Exception:
                                            pass
                                    except Exception:
                                        pass
                                try:
                                    start_rg_for_path('-', label=label, python_stream_feed=feed_fn)
                                    total_files += 1
                                except Exception:
                                    pass
                            streamed_local = True
                        except Exception:
                            streamed_local = False
                    
                    if not streamed_local:
                        # 避免使用 py7zr 将成员完整读入内存导致峰值增长；
                        # 在无法使用系统 7z 流式的情况下，回退到解包到临时目录的方式。
                        streamed_local = False

                    if not streamed_local:
                        # 7z 流式失败，回退到解包临时目录
                        temp_dir_for_archive = tempfile.mkdtemp(prefix='rg_archive_')
                        pm.temp_dirs.append(temp_dir_for_archive)
                        extracted_ok = False
                        try:
                            ok, _ = try_py7zr_extract(search_path_local, temp_dir_for_archive)
                            if ok:
                                extracted_ok = True
                        except Exception:
                            extracted_ok = False

                        if not extracted_ok and has_cmd('7z'):
                            try:
                                extract_cmd = ['7z', 'x', '-y', search_path_local, f'-o{temp_dir_for_archive}']
                                extract_proc = subprocess.Popen(
                                    extract_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=False,
                                    preexec_fn=os.setpgrp if hasattr(os, "setpgrp") else None,
                                    creationflags=popen_creationflags()
                                )
                                extra_procs_local.append(extract_proc)
                                pm.extra_procs.append(extract_proc)
                                extract_proc.wait()
                                if extract_proc.returncode == 0:
                                    extracted_ok = True
                            except Exception:
                                extracted_ok = False

                        if extracted_ok:
                            cnt = 0
                            for _, _, fns in os.walk(temp_dir_for_archive):
                                for _ in fns:
                                    cnt += 1
                            total_files += cnt if cnt > 0 else 1
                            _p = start_rg_for_path(temp_dir_for_archive)
                            schedule_temp_dir_cleanup_for_proc(_p, temp_dir_for_archive)
                        else:
                            try:
                                shutil.rmtree(temp_dir_for_archive)
                            except Exception:
                                pass
                            pm.temp_dirs.remove(temp_dir_for_archive)
                return

            # Excel 单文件：转换为文本后通过 stdin 喂入 rg
            if is_excel_file(file_lower):
                rel_label = rel_label
                def feed_fn(w, p=search_path_local):
                    try:
                        stream_excel_to_writer(p, w)
                    except Exception:
                        pass
                start_rg_for_path('-', label=rel_label, python_stream_feed=feed_fn)
                total_files += 1
                return

            # 常规文本/CSV/其他：直接作为 stdin 流式写入
            rel_label = rel_label
            def feed_fn(w, p=search_path_local, lb=rel_label, fl=file_lower):
                size_c = None
                try:
                    size_c = os.path.getsize(p)
                except Exception:
                    size_c = None
                ft = classify_file_type(fl)
                def _cb(done, total, elapsed_ms, ft_local=ft, lb_local=lb):
                    emit_progress_ex(phase='scan', file_type=ft_local,
                                     elapsed_ms=elapsed_ms, bytes_done=done, bytes_total=total,
                                     label=lb_local)
                try:
                    with open(p, 'rb') as f:
                        if is_csv_file(fl):
                            stream_csv_fileobj_to_writer(f, w, progress_cb=_cb, bytes_total=size_c)
                        else:
                            copy_fileobj_chunked(f, w, chunk_size=64 * 1024, progress_cb=_cb, bytes_total=size_c)
                except Exception:
                    pass
            start_rg_for_path('-', label=rel_label, python_stream_feed=feed_fn)
            total_files += 1

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

            total_files = (len(csv_files) + len(text_files) + len(other_regular_files)
                           + len(compressed_files) + len(excel_files) + len(archive_files))

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
            pm.proc = None
            try:
                if scope == 'single' or (scope == 'all' and final_all):
                    close_export_stream(sanitize_keyword(keyword))
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
            return "Started"

        # 标记主进程（用于 /cancel 检测）
        try:
            pm.proc = all_rg_procs[0]
        except Exception:
            pm.proc = all_rg_procs[-1]

        request_start_ns = time.perf_counter_ns()

        def get_output_loop():
            safe_kw = sanitize_keyword(keyword)
            match_count = 0
            files_done = 0
            try:
                with get_app().app_context():
                    context_before_n = context_before if context_before > 0 else 0
                    context_after_n = context_after if context_after > 0 else 0
                    before_lines = []
                    after_lines = []
                    block_main = None
                    block_ready = False
                    after_emit_count = 0
                    block_match_count = 0
                    first_block = True
                    search_start_ns = {}
                    last_progress_tick_ns = 0
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
                        except queue.Empty:
                            if pm.cancel_requested:
                                break
                            all_ended = True
                            for p in extra_procs_local:
                                if p.poll() is None:
                                    all_ended = False
                                    break
                            if all_ended and q.empty():
                                break
                            # 周期性发送进度
                            try:
                                now_ns = time.perf_counter_ns()
                                if last_progress_tick_ns == 0 or (now_ns - last_progress_tick_ns) >= 200_000_000:
                                    elapsed_ms_total = int((now_ns - request_start_ns) / 1_000_000)
                                    emit_progress_ex(matches=match_count, files_total=total_files, files_done=files_done, elapsed_ms=elapsed_ms_total)
                                    last_progress_tick_ns = now_ns
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
                                    # 无论是否为计数模式，都完成当前块并重置状态，确保下一块按“非首块”处理插入空行
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
                                    label=label_text
                                )
                            except Exception:
                                pass

                            # 每个文件检索完成后主动回收局部状态并进行内存修剪，降低长跑内存占用
                            try:
                                # 清理与该 owner 相关的临时映射，避免集合无限增长
                                try:
                                    owner_current_label.pop(owner, None)
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
                                # 触发一次垃圾回收与进程内存修剪（轻量，确保大型对象尽快回收并释放工作集）
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
                # 最终清理：额外进程、临时目录、导出流、主进程标记
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
                try:
                    for d in list(pm.temp_dirs):
                        try:
                            shutil.rmtree(d)
                        except Exception:
                            pass
                    pm.temp_dirs.clear()
                except Exception:
                    pass
                try:
                    pm._proc_label_map.clear()
                except Exception:
                    pass
                pm.proc = None
                try:
                    if (scope == 'single' or (scope == 'all' and final_all)):
                        close_export_stream(sanitize_keyword(keyword))
                except Exception:
                    pass
                # 清理可能累积的全局附加进程引用
                try:
                    pm.extra_procs.clear()
                except Exception:
                    pass
                # 等待前向读取线程结束
                try:
                    for t in forward_threads_local:
                        try:
                            t.join(timeout=0.5)
                        except Exception:
                            pass
                except Exception:
                    pass

                # 主动排空内部队列，避免残留大对象占用内存
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
                    try:
                        owner_current_label.clear()
                    except Exception:
                        pass
                    try:
                        owner_has_output.clear()
                    except Exception:
                        pass
                    try:
                        search_start_ns.clear()
                    except Exception:
                        pass
                    try:
                        before_lines.clear()
                        after_lines.clear()
                    except Exception:
                        pass
                except Exception:
                    pass

                # 触发垃圾回收与进程内存修剪，确保在“完成”消息前尽量释放内存
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
                    time.sleep(0.5)
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

        # 启动后台输出线程
        try:
            t_loop = threading.Thread(target=get_output_loop, daemon=True)
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
                    pm._terminate_proc(p)
                except Exception:
                    pass
            for d in pm.temp_dirs:
                try:
                    shutil.rmtree(d)
                except Exception:
                    pass
            try:
                pm.temp_dirs.clear()
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
            extra_procs_local = []
            pm.proc = None
            try:
                pm.extra_procs.clear()
            except Exception:
                pass
            try:
                if scope == 'single' or (scope == 'all' and final_all):
                    close_export_stream(sanitize_keyword(keyword))
            except Exception:
                pass
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
                pm._terminate_proc(p)
            except Exception:
                pass
        for d in pm.temp_dirs:
            try:
                shutil.rmtree(d)
            except Exception:
                pass
        try:
            pm.temp_dirs.clear()
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
        extra_procs_local = []
        pm.proc = None
        try:
            pm.extra_procs.clear()
        except Exception:
            pass
        try:
            if scope == 'single' or (scope == 'all' and final_all):
                close_export_stream(sanitize_keyword(keyword))
        except Exception:
            pass
        return "Error"
