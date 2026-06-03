# -*- coding: utf-8 -*-
"""
压缩文件处理器
- 负责解压/解包、单一压缩文件流式处理、归档成员提取等
- 提供将流式数据喂入 rg 的辅助方法
"""

import os
import io
import time
import shutil
import tempfile
import threading
import subprocess

from config import STREAM_CHUNK_SIZE
from utils import (
    has_cmd, popen_creationflags, emit_message_utf, emit_progress_ex,
    is_csv_file, drop_file_cache_range, set_file_access_noreuse_fd,
)
import process as pm


def try_py7zr_extract(archive_path, dest):
    """如果可行，尝试使用 py7zr 解压 7z（以及许多其他格式）。"""
    try:
        import py7zr
    except Exception:
        return False, "py7zr not available"
    try:
        with py7zr.SevenZipFile(archive_path, mode='r') as z:
            z.extractall(path=dest)
        return True, ""
    except Exception as e:
        return False, str(e)


def try_rarfile_extract(archive_path, dest):
    """如果可用，尝试使用 Python 的 rarfile 库来解压 rar 文件。"""
    try:
        import rarfile
    except Exception:
        return False, "rarfile not available"
    try:
        rf = rarfile.RarFile(archive_path)
        rf.extractall(dest)
        return True, ""
    except Exception as e:
        return False, str(e)


def start_rg_and_feed_python_stream(rg_cmd, feed_fn):
    """
    启动 rg 进程并用 feed_fn 向其 stdin 写入解压或转换后的内容。
    feed_fn 接受一个可写二进制文件对象。
    """
    rg_proc = subprocess.Popen(
        rg_cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        shell=False,
        preexec_fn=os.setpgrp if hasattr(os, "setpgrp") else None,
        creationflags=popen_creationflags()
    )

    def writer_thread():
        try:
            with rg_proc.stdin:
                if not pm.cancel_requested:
                    feed_fn(rg_proc.stdin)
        except Exception:
            try:
                rg_proc.stdin.close()
            except Exception:
                pass

    t = threading.Thread(target=writer_thread, daemon=True)
    t.start()
    return rg_proc


def python_decompress_feed(path, ext, out_stream):
    """解压单一压缩文件并写入到 out_stream（支持 gzip/bz2/lzma/lz4）。"""
    ext = (ext or '').lower()
    try:
        chunk_size = STREAM_CHUNK_SIZE
        if ext.endswith('.gz'):
            import gzip
            with gzip.open(path, 'rb') as f:
                while True:
                    if pm.cancel_requested:
                        break
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    out_stream.write(chunk)
        elif ext.endswith('.bz2'):
            import bz2
            with bz2.open(path, 'rb') as f:
                while True:
                    if pm.cancel_requested:
                        break
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    out_stream.write(chunk)
        elif ext.endswith('.xz') or ext.endswith('.txz') or ext.endswith('.lzma'):
            import lzma
            with lzma.open(path, 'rb') as f:
                while True:
                    if pm.cancel_requested:
                        break
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    out_stream.write(chunk)
        elif ext.endswith('.lz4'):
            try:
                import lz4.frame as lz4frame
                with open(path, 'rb') as raw:
                    decompressor = lz4frame.LZ4FrameDecompressor()
                    while True:
                        if pm.cancel_requested:
                            break
                        chunk = raw.read(chunk_size)
                        if not chunk:
                            break
                        out = decompressor.decompress(chunk)
                        if out:
                            out_stream.write(out)
            except Exception:
                raise
        else:
            raise RuntimeError("Unsupported python decompressor for ext: " + ext)
    except Exception:
        raise


def build_decompress_command(path_lower, real_path):
    """根据后缀构建外部解压命令（若系统支持对应工具）。"""
    path_lower = (path_lower or '').lower()
    if path_lower.endswith('.gz'):
        if has_cmd('gzip'):
            return ['gzip', '-dc', real_path]
        else:
            return None
    if path_lower.endswith('.bz2'):
        if has_cmd('bzip2'):
            return ['bzip2', '-dc', real_path]
        else:
            return None
    if path_lower.endswith('.xz') or path_lower.endswith('.txz'):
        if has_cmd('xz'):
            return ['xz', '-dc', real_path]
        else:
            return None
    if path_lower.endswith('.lz4'):
        if has_cmd('lz4'):
            return ['lz4', '-dc', real_path]
        else:
            return None
    if path_lower.endswith('.lzma'):
        if has_cmd('lzma'):
            return ['lzma', '-dc', real_path]
        else:
            return None
    if path_lower.endswith(('.7z', '.rar')):
        if has_cmd('7z'):
            return ['7z', 'x', '-so', real_path]
    return None


def list_7z_members(archive_path):
    """逐行解析 7z 列表输出，按需产生成员信息（name, size）。"""
    if not has_cmd('7z'):
        return []
    try:
        proc = subprocess.Popen(
            ['7z', 'l', '-slt', archive_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            shell=False,
            creationflags=popen_creationflags()
        )
        current_path = None
        current_type = None
        current_size = None
        # 逐行读取，避免一次性加载到内存
        for raw_line in iter(proc.stdout.readline, b''):
            s = raw_line.decode('utf-8', errors='replace').strip()
            if not s:
                if current_path and (not current_type or current_type.lower() == 'file'):
                    yield {'name': current_path, 'size': (int(current_size) if current_size and current_size.isdigit() else None)}
                current_path = None
                current_type = None
                current_size = None
                continue
            if s.startswith('Path = '):
                current_path = s[7:]
            elif s.startswith('Type = '):
                current_type = s[7:]
            elif s.startswith('Size = '):
                current_size = s[7:]
        # 文件末尾可能没有空行分隔，处理最后一个条目
        if current_path and (not current_type or current_type.lower() == 'file'):
            yield {'name': current_path, 'size': (int(current_size) if current_size and current_size.isdigit() else None)}
        proc.wait()
    except Exception:
        # 出错时返回空可迭代对象
        return []


def safe_extract_tar(tar, path):
    """安全地解包 tar，防止路径穿越。"""
    import tarfile
    for member in tar.getmembers():
        member_path = os.path.join(path, member.name)
        abs_dest = os.path.abspath(path)
        abs_target = os.path.abspath(member_path)
        if not abs_target.startswith(abs_dest + os.sep) and abs_target != abs_dest:
            raise Exception("Attempted Path Traversal in Tar File")
    tar.extractall(path)


def safe_extract_zip(zipf, path):
    """安全地解包 zip，防止路径穿越。"""
    import zipfile
    for member in zipf.namelist():
        member_path = os.path.join(path, member)
        abs_dest = os.path.abspath(path)
        abs_target = os.path.abspath(member_path)
        if not abs_target.startswith(abs_dest + os.sep) and abs_target != abs_dest:
            raise Exception("Attempted Path Traversal in Zip File")
    zipf.extractall(path)
