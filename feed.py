# -*- coding: utf-8 -*-
"""
流式喂入模块
- 负责不同文件类型（文本、CSV、Excel、单文件压缩、归档）的内容转换与流式喂入 ripgrep
- 提供 start_rg_for_path 与 handle_single_file 等 API，被搜索框架调用
"""

import os
import shutil
import tempfile
import subprocess
import threading

from utils import (
    emit_progress_ex, classify_file_type, strip_single_compress_ext, popen_creationflags,
    is_single_file_compressed, is_archive_multi_file, is_excel_file, is_csv_file, has_cmd, trim_process_memory
)
from file_handlers import (
    start_rg_and_feed_python_stream, python_decompress_feed, build_decompress_command,
    list_7z_members, safe_extract_tar, safe_extract_zip, stream_excel_to_writer,
    stream_csv_fileobj_to_writer, copy_fileobj_chunked, spool_stream_to_temp_then_stream_excel,
    try_py7zr_extract, try_rarfile_extract
)
import process_manager as pm

_RG_SUPPORTS_LABEL = None
_RG_LOCK = threading.Lock()


def check_rg_supports_label():
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


def start_rg_for_path(rg_base, keyword, path, stdin_pipe=None, label=None, exclude_patterns=None, python_stream_feed=None, register_proc=None):
    cmd = list(rg_base) if isinstance(rg_base, list) else [str(x) for x in rg_base]
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

    try:
        if callable(register_proc):
            register_proc(rg_proc)
    except Exception:
        pass

    return rg_proc




def handle_single_file(search_path_local, basename_label, rg_base, keyword, start_forward_thread, register_proc, extra_procs_local):
    files_inc = 0
    file_lower = os.path.basename(search_path_local).lower()
    rel_label = basename_label or os.path.basename(search_path_local)

    # 单文件压缩：.gz/.bz2/.xz/.lz4 等
    if is_single_file_compressed(file_lower):
        label = rel_label
        inner_lower = strip_single_compress_ext(file_lower)
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
                if isinstance(extra_procs_local, list):
                    extra_procs_local.append(decompress_proc)
                pm.extra_procs.append(decompress_proc)
                if is_excel_file(inner_lower):
                    def feed_fn(w, proc=decompress_proc, name=inner_lower, lb=label):
                        def _cb(done, total, elapsed_ms):
                            emit_progress_ex(phase='decompress', file_type='compressed',
                                             elapsed_ms=elapsed_ms, bytes_done=done, bytes_total=total, label=lb)
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
                    rg_proc = start_rg_for_path(rg_base, keyword, '-', label=label, python_stream_feed=feed_fn, register_proc=register_proc)
                    try:
                        start_forward_thread(rg_proc)
                    except Exception:
                        pass
                else:
                    def feed_fn(w, proc=decompress_proc, lb=label, orig=search_path_local):
                        size_c = None
                        try:
                            size_c = os.path.getsize(orig)
                        except Exception:
                            size_c = None
                        def _cb(done, total, elapsed_ms):
                            emit_progress_ex(phase='decompress', file_type='compressed',
                                             elapsed_ms=elapsed_ms, bytes_done=done, bytes_total=total, label=lb)
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
                    rg_proc = start_rg_for_path(rg_base, keyword, '-', label=label, python_stream_feed=feed_fn, register_proc=register_proc)
                    try:
                        start_forward_thread(rg_proc)
                    except Exception:
                        pass
            except Exception:
                def feed_fn(w, p=search_path_local, e=file_lower):
                    python_decompress_feed(p, e, w)
                rg_proc = start_rg_for_path(rg_base, keyword, '-', label=label, python_stream_feed=feed_fn, register_proc=register_proc)
                try:
                    start_forward_thread(rg_proc)
                except Exception:
                    pass
        else:
            def feed_fn(w, p=search_path_local, e=file_lower):
                python_decompress_feed(p, e, w)
            rg_proc = start_rg_for_path(rg_base, keyword, '-', label=label, python_stream_feed=feed_fn, register_proc=register_proc)
            try:
                start_forward_thread(rg_proc)
            except Exception:
                pass
        files_inc += 1
        return files_inc

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
                        files_inc += 1
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
                                        emit_progress_ex(phase='decompress', file_type='archive',
                                                         elapsed_ms=elapsed_ms, bytes_done=done, bytes_total=total, label=lb)
                                    if is_excel_file(lower):
                                        spool_stream_to_temp_then_stream_excel(lower, f2, w)
                                    elif is_csv_file(lower):
                                        stream_csv_fileobj_to_writer(f2, w, progress_cb=_cb)
                                    else:
                                        copy_fileobj_chunked(f2, w, progress_cb=_cb)
                            except Exception:
                                pass
                        rg_proc = start_rg_for_path(rg_base, keyword, '-', label=label, python_stream_feed=feed_fn, register_proc=register_proc)
                        try:
                            start_forward_thread(rg_proc)
                        except Exception:
                            pass
        except Exception:
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
                files_inc += cnt if cnt > 0 else 1
                _p = start_rg_for_path(rg_base, keyword, temp_dir_for_archive, register_proc=register_proc)
                pm.schedule_temp_dir_cleanup_for_proc(_p, temp_dir_for_archive)
                try:
                    start_forward_thread(_p)
                except Exception:
                    pass
            except Exception:
                try:
                    shutil.rmtree(temp_dir_for_archive)
                except Exception:
                    pass
                pm.temp_dirs.remove(temp_dir_for_archive)
        return files_inc

    # 归档（zip/rar/7z 等）
    if is_archive_multi_file(file_lower):
        # zip/jar/war
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
                                                             elapsed_ms=elapsed_ms, bytes_done=done, bytes_total=total, label=lb)
                                        if is_excel_file(lower):
                                            spool_stream_to_temp_then_stream_excel(lower, member_f, w)
                                        elif is_csv_file(lower):
                                            stream_csv_fileobj_to_writer(member_f, w, progress_cb=_cb, bytes_total=sz)
                                        else:
                                            copy_fileobj_chunked(member_f, w, progress_cb=_cb, bytes_total=sz)
                            except Exception:
                                pass
                        try:
                            rg_proc = start_rg_for_path(rg_base, keyword, '-', label=label, python_stream_feed=feed_fn, register_proc=register_proc)
                            try:
                                start_forward_thread(rg_proc)
                            except Exception:
                                pass
                            files_inc += 1
                        except Exception:
                            pass
            except Exception:
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
                    files_inc += cnt if cnt > 0 else 1
                    _p = start_rg_for_path(rg_base, keyword, temp_dir_for_archive, register_proc=register_proc)
                    pm.schedule_temp_dir_cleanup_for_proc(_p, temp_dir_for_archive)
                    try:
                        start_forward_thread(_p)
                    except Exception:
                        pass
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
                                                         elapsed_ms=elapsed_ms, bytes_done=done, bytes_total=total, label=lb)
                                    if is_excel_file(lower):
                                        spool_stream_to_temp_then_stream_excel(lower, member_f, w)
                                    elif is_csv_file(lower):
                                        stream_csv_fileobj_to_writer(member_f, w, progress_cb=_cb, bytes_total=size)
                                    else:
                                        copy_fileobj_chunked(member_f, w, progress_cb=_cb, bytes_total=size)
                        except Exception:
                            pass
                    try:
                        rg_proc = start_rg_for_path(rg_base, keyword, '-', label=label, python_stream_feed=feed_fn, register_proc=register_proc)
                        try:
                            start_forward_thread(rg_proc)
                        except Exception:
                            pass
                        files_inc += 1
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
                                if isinstance(extra_procs_local, list):
                                    extra_procs_local.append(dec_proc)
                                pm.extra_procs.append(dec_proc)
                                lower = nm.lower()
                                def _cb(done, total, elapsed_ms):
                                    emit_progress_ex(phase='decompress', file_type='archive',
                                                     elapsed_ms=elapsed_ms, bytes_done=done, bytes_total=total, label=lb)
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
                            rg_proc = start_rg_for_path(rg_base, keyword, '-', label=label, python_stream_feed=feed_fn, register_proc=register_proc)
                            try:
                                start_forward_thread(rg_proc)
                            except Exception:
                                pass
                            files_inc += 1
                        except Exception:
                            pass
                    streamed_local = True
                except Exception:
                    streamed_local = False

            if not streamed_local:
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
                        if isinstance(extra_procs_local, list):
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
                    files_inc += cnt if cnt > 0 else 1
                    _p = start_rg_for_path(rg_base, keyword, temp_dir_for_archive, register_proc=register_proc)
                    pm.schedule_temp_dir_cleanup_for_proc(_p, temp_dir_for_archive)
                    try:
                        start_forward_thread(_p)
                    except Exception:
                        pass
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
                                if isinstance(extra_procs_local, list):
                                    extra_procs_local.append(dec_proc)
                                pm.extra_procs.append(dec_proc)
                                lower = nm.lower()
                                def _cb(done, total, elapsed_ms):
                                    emit_progress_ex(phase='decompress', file_type='archive',
                                                     elapsed_ms=elapsed_ms, bytes_done=done, bytes_total=total, label=lb)
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
                            rg_proc = start_rg_for_path(rg_base, keyword, '-', label=label, python_stream_feed=feed_fn, register_proc=register_proc)
                            try:
                                start_forward_thread(rg_proc)
                            except Exception:
                                pass
                            files_inc += 1
                        except Exception:
                            pass
                    streamed_local = True
                except Exception:
                    streamed_local = False

            if not streamed_local:
                streamed_local = False

            if not streamed_local:
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
                        if isinstance(extra_procs_local, list):
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
                    files_inc += cnt if cnt > 0 else 1
                    _p = start_rg_for_path(rg_base, keyword, temp_dir_for_archive, register_proc=register_proc)
                    pm.schedule_temp_dir_cleanup_for_proc(_p, temp_dir_for_archive)
                    try:
                        start_forward_thread(_p)
                    except Exception:
                        pass
                else:
                    try:
                        shutil.rmtree(temp_dir_for_archive)
                    except Exception:
                        pass
                    pm.temp_dirs.remove(temp_dir_for_archive)
        return files_inc

    # Excel 单文件：转换为文本后通过 stdin 喂入 rg
    if is_excel_file(file_lower):
        def feed_fn(w, p=search_path_local):
            try:
                stream_excel_to_writer(p, w)
            except Exception:
                pass
        rg_proc = start_rg_for_path(rg_base, keyword, '-', label=rel_label, python_stream_feed=feed_fn, register_proc=register_proc)
        try:
            start_forward_thread(rg_proc)
        except Exception:
            pass
        files_inc += 1
        return files_inc

    # 常规文本/CSV/其他
    def feed_fn(w, p=search_path_local, lb=rel_label, fl=file_lower):
        size_c = None
        try:
            size_c = os.path.getsize(p)
        except Exception:
            size_c = None
        ft = classify_file_type(fl)
        def _cb(done, total, elapsed_ms, ft_local=ft, lb_local=lb):
            emit_progress_ex(phase='scan', file_type=ft_local,
                             elapsed_ms=elapsed_ms, bytes_done=done, bytes_total=total, label=lb_local)
        try:
            with open(p, 'rb') as f:
                if is_csv_file(fl):
                    stream_csv_fileobj_to_writer(f, w, progress_cb=_cb, bytes_total=size_c)
                else:
                    copy_fileobj_chunked(f, w, chunk_size=64 * 1024, progress_cb=_cb, bytes_total=size_c)
        except Exception:
            pass
    rg_proc = start_rg_for_path(rg_base, keyword, '-', label=rel_label, python_stream_feed=feed_fn, register_proc=register_proc)
    try:
        start_forward_thread(rg_proc)
    except Exception:
        pass
    files_inc += 1
    return files_inc