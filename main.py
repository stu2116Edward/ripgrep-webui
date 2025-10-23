from flask import Flask, render_template, request, send_from_directory, abort, jsonify
from flask_socketio import SocketIO, emit
import subprocess
import threading
import os
import signal
import time
import tempfile
import shutil
import queue
import io

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", path='/io')

# 全局当前正在运行的 ripgrep 进程（read loop 读取这些进程的 stdout）
proc = None
# 可能存在的额外进程（例如解压器/解包器/rg），搜索/取消时需要一起清理
extra_procs = []
# 临时目录列表（例如对 archive 的解包目录），结束时需要清理
temp_dirs = []

output_buffers = {}  # 关键字 -> 行内容列表

# 支持的压缩/归档后缀（用于目录扫描时识别并单独处理）
SINGLE_COMPRESSED_EXTS = ('.gz', '.bz2', '.xz', '.lz4', '.lzma')
ARCHIVE_EXTS = (
    '.zip', '.jar', '.war',
    '.tar', '.tar.gz', '.tgz', '.tar.bz2', '.tbz2', '.tar.xz', '.txz',
    '.7z', '.rar'   # 支持 .7z / .rar
)

# 支持的电子表格/文本后缀
EXCEL_EXTS = ('.xls', '.xlsx')
CSV_EXTS = ('.csv',)

# 缓存 ripgrep 是否支持 --label 参数（None 表示尚未检测）
_RG_SUPPORTS_LABEL = None
# 进程 pid -> 自定义 label 映射（当 rg 不支持 --label 时使用）
_proc_label_map = {}


def is_single_file_compressed(filename_lower):
    if filename_lower.endswith(('.tar.gz', '.tgz', '.tar.bz2', '.tbz2', '.tar.xz', '.txz')):
        return False
    return filename_lower.endswith(SINGLE_COMPRESSED_EXTS)


def is_archive_multi_file(filename_lower):
    return filename_lower.endswith(ARCHIVE_EXTS)


def is_excel_file(filename_lower):
    return filename_lower.endswith(EXCEL_EXTS)


def is_csv_file(filename_lower):
    return filename_lower.endswith(CSV_EXTS)


def strip_single_compress_ext(filename_lower):
    """去掉单一压缩扩展，返回内部真实扩展（小写，如 .xlsx/.csv），不匹配则返回空串"""
    for ce in SINGLE_COMPRESSED_EXTS:
        if filename_lower.endswith(ce):
            base = filename_lower[:-len(ce)]
            inner_ext = os.path.splitext(base)[1].lower()
            return inner_ext
    return ''


def has_cmd(name):
    """检查外部命令是否在 PATH 中"""
    return shutil.which(name) is not None


def check_rg_supports_label():
    """检测系统中 rg 的帮助输出里是否包含 --label，缓存结果"""
    global _RG_SUPPORTS_LABEL
    if _RG_SUPPORTS_LABEL is not None:
        return _RG_SUPPORTS_LABEL
    try:
        p = subprocess.run(['rg', '--help'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)
        out = p.stdout.decode('utf-8', errors='replace')
        _RG_SUPPORTS_LABEL = ('--label' in out)
    except Exception:
        _RG_SUPPORTS_LABEL = False
    return _RG_SUPPORTS_LABEL


def try_py7zr_extract(archive_path, dest):
    """Try to extract 7z (and many other formats) using py7zr if available."""
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
    """Try to extract rar using rarfile python lib if available."""
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
    启动 rg 进程并用 feed_fn 向其 stdin 写入解压或转换后的内容
    feed_fn 接受一个可写二进制文件对象
    """
    rg_proc = subprocess.Popen(
        rg_cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        shell=False,
        preexec_fn=os.setpgrp if hasattr(os, "setpgrp") else None
    )

    def writer_thread():
        try:
            with rg_proc.stdin:
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
    """解压单一压缩文件并写入到 out_stream（支持gzip,bz2,lzma,lz4）"""
    ext = ext.lower()
    try:
        if ext.endswith('.gz'):
            import gzip
            with gzip.open(path, 'rb') as f:
                shutil.copyfileobj(f, out_stream)
        elif ext.endswith('.bz2'):
            import bz2
            with bz2.open(path, 'rb') as f:
                shutil.copyfileobj(f, out_stream)
        elif ext.endswith('.xz') or ext.endswith('.txz') or ext.endswith('.lzma'):
            import lzma
            with lzma.open(path, 'rb') as f:
                shutil.copyfileobj(f, out_stream)
        elif ext.endswith('.lz4'):
            try:
                import lz4.frame as lz4frame
                with open(path, 'rb') as raw:
                    decompressor = lz4frame.LZ4FrameDecompressor()
                    while True:
                        chunk = raw.read(64 * 1024)
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
    path_lower = path_lower.lower()
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
    names = []
    if not has_cmd('7z'):
        return names
    try:
        p = subprocess.run(['7z', 'l', '-slt', archive_path], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)
        out_lines = p.stdout.decode('utf-8', errors='replace').splitlines()
        current_path = None
        current_type = None
        for line in out_lines:
            s = line.strip()
            if not s:
                if current_path and (not current_type or current_type.lower() == 'file'):
                    names.append(current_path)
                current_path = None
                current_type = None
                continue
            if s.startswith('Path = '):
                current_path = s[7:]
            elif s.startswith('Type = '):
                current_type = s[7:]
        if current_path and (not current_type or current_type.lower() == 'file'):
            names.append(current_path)
    except Exception:
        pass
    return names


def safe_extract_tar(tar, path):
    import tarfile
    for member in tar.getmembers():
        member_path = os.path.join(path, member.name)
        abs_dest = os.path.abspath(path)
        abs_target = os.path.abspath(member_path)
        if not abs_target.startswith(abs_dest + os.sep) and abs_target != abs_dest:
            raise Exception("Attempted Path Traversal in Tar File")
    tar.extractall(path)


def safe_extract_zip(zipf, path):
    import zipfile
    for member in zipf.namelist():
        member_path = os.path.join(path, member)
        abs_dest = os.path.abspath(path)
        abs_target = os.path.abspath(member_path)
        if not abs_target.startswith(abs_dest + os.sep) and abs_target != abs_dest:
            raise Exception("Attempted Path Traversal in Zip File")
    zipf.extractall(path)


# ===== 将 Excel 文件转换为纯文本流，写入 out_stream（binary writer） =====
def stream_excel_to_writer(path, out_stream):
    """
    将 xls/xlsx 文件内容以文本形式写到 out_stream（二进制写）。
    输出格式（便于 ripgrep 搜索）：
      # sheet: <sheetname>
      <cell1>\t<cell2>\t... \n
    优先使用 Python 库 openpyxl（xlsx）和 xlrd（xls）。若缺失，会尝试通知前端并跳过该文件。
    """
    path_lower = path.lower()
    try:
        if path_lower.endswith('.xlsx'):
            try:
                import openpyxl
            except Exception:
                socketio.emit('message', {'message': f'?? openpyxl not installed, cannot parse xlsx: {os.path.basename(path)}\n'})
                return
            # 以只读模式打开，data_only=True 获取公式计算值（若存在）
            wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
            for sheet in wb:
                try:
                    out_stream.write((f"# sheet: {sheet.title}\n").encode('utf-8'))
                except Exception:
                    pass
                # iter_rows(values_only=True) 返回每行的值元组
                for row in sheet.iter_rows(values_only=True):
                    try:
                        vals = []
                        for v in row:
                            if v is None:
                                vals.append('')
                            else:
                                vals.append(str(v))
                        line = '\t'.join(vals) + '\n'
                        out_stream.write(line.encode('utf-8'))
                    except Exception:
                        # 某些单元格可能编码成问题，使用 replace 处理
                        try:
                            safe_vals = [str(v) if v is not None else '' for v in row]
                            line = '\t'.join(safe_vals) + '\n'
                            out_stream.write(line.encode('utf-8', errors='replace'))
                        except Exception:
                            continue
            try:
                wb.close()
            except Exception:
                pass

        elif path_lower.endswith('.xls'):
            try:
                import xlrd
            except Exception:
                socketio.emit('message', {'message': f'?? xlrd not installed, cannot parse xls: {os.path.basename(path)}\n'})
                return
            wb = xlrd.open_workbook(path, on_demand=True)
            for si in range(wb.nsheets):
                sheet = wb.sheet_by_index(si)
                try:
                    out_stream.write((f"# sheet: {sheet.name}\n").encode('utf-8'))
                except Exception:
                    pass
                for r in range(sheet.nrows):
                    try:
                        row = sheet.row_values(r)
                        vals = [(str(c) if c is not None else '') for c in row]
                        line = '\t'.join(vals) + '\n'
                        out_stream.write(line.encode('utf-8'))
                    except Exception:
                        try:
                            out_stream.write(('\t'.join([str(c) for c in sheet.row_values(r)]) + '\n').encode('utf-8', errors='replace'))
                        except Exception:
                            continue
            try:
                wb.release_resources()
            except Exception:
                pass
        else:
            # 非支持格式
            socketio.emit('message', {'message': f'?? Unsupported excel format: {path}\n'})
            return
    except Exception as e:
        socketio.emit('message', {'message': f'?? Excel parse failed for {os.path.basename(path)}: {e}\n'})
        return

# 允许传入内存字节的 Excel 转换（供归档成员或解压输出使用）
def stream_excel_bytes_to_writer(name_lower, data_bytes, out_stream):
    try:
        if name_lower.endswith('.xlsx'):
            import openpyxl, io
            wb = openpyxl.load_workbook(io.BytesIO(data_bytes), read_only=True, data_only=True)
            for sheet in wb:
                try:
                    out_stream.write((f"# sheet: {sheet.title}\n").encode('utf-8'))
                except Exception:
                    pass
                for row in sheet.iter_rows(values_only=True):
                    try:
                        vals = [(str(c) if c is not None else '') for c in row]
                        out_stream.write(('\t'.join(vals) + '\n').encode('utf-8'))
                    except Exception:
                        try:
                            safe_vals = [str(v) if v is not None else '' for v in row]
                            out_stream.write(('\t'.join(safe_vals) + '\n').encode('utf-8', errors='replace'))
                        except Exception:
                            continue
            try:
                wb.close()
            except Exception:
                pass
        elif name_lower.endswith('.xls'):
            import xlrd
            wb = xlrd.open_workbook(file_contents=data_bytes, on_demand=True)
            for si in range(wb.nsheets):
                sheet = wb.sheet_by_index(si)
                try:
                    out_stream.write((f"# sheet: {sheet.name}\n").encode('utf-8'))
                except Exception:
                    pass
                for r in range(sheet.nrows):
                    try:
                        row = sheet.row_values(r)
                        vals = [(str(c) if c is not None else '') for c in row]
                        out_stream.write(('\t'.join(vals) + '\n').encode('utf-8'))
                    except Exception:
                        try:
                            out_stream.write(('\t'.join([str(c) for c in sheet.row_values(r)]) + '\n').encode('utf-8', errors='replace'))
                        except Exception:
                            continue
            try:
                wb.release_resources()
            except Exception:
                pass
    except Exception as e:
        socketio.emit('message', {'message': f'?? Excel parse failed (bytes) for {name_lower}: {e}\n'})
        return

# CSV 文件对象直接复制到输出（提供统一接口）
def stream_csv_fileobj_to_writer(fileobj, out_stream):
    try:
        last_byte = None
        while True:
            chunk = fileobj.read(64 * 1024)
            if not chunk:
                break
            out_stream.write(chunk)
            try:
                if chunk:
                    last_byte = chunk[-1]
            except Exception:
                pass
        if last_byte is not None and last_byte != 0x0A:
            try:
                out_stream.write(b'\n')
            except Exception:
                pass
    except Exception:
        # 退回一次性读写，并保证行尾换行
        try:
            data = fileobj.read()
            if data:
                out_stream.write(data)
                try:
                    if data[-1] != 0x0A:
                        out_stream.write(b'\n')
                except Exception:
                    pass
        except Exception:
            pass

# 统一的分块拷贝（避免一次性读入内存）
STREAM_CHUNK_SIZE = 256 * 1024  # 256KB

def copy_fileobj_chunked(src, dst, chunk_size: int = STREAM_CHUNK_SIZE):
    try:
        while True:
            chunk = src.read(chunk_size)
            if not chunk:
                break
            try:
                dst.write(chunk)
            except BrokenPipeError:
                break
    except Exception:
        # 尝试最后一次性拷贝，尽量保证输出完整
        try:
            data = src.read()
            if data:
                try:
                    dst.write(data)
                except Exception:
                    pass
        except Exception:
            pass
    try:
        if hasattr(dst, 'flush'):
            dst.flush()
    except Exception:
        pass

# 将输入流落盘为临时Excel文件后再流式转换（避免内存峰值）
import tempfile as _tempfile_mod

def spool_stream_to_temp_then_stream_excel(name_lower: str, in_stream, out_stream):
    ext = '.xlsx' if name_lower.endswith('.xlsx') else ('.xls' if name_lower.endswith('.xls') else '.xlsx')
    tmp = _tempfile_mod.NamedTemporaryFile(prefix='rg_excel_', suffix=ext, delete=False)
    tmp_path = tmp.name
    try:
        copy_fileobj_chunked(in_stream, tmp)
        try:
            tmp.flush()
        except Exception:
            pass
        try:
            tmp.close()
        except Exception:
            pass
        # 直接复用现有的按路径Excel流式转换
        stream_excel_to_writer(tmp_path, out_stream)
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass

# ===== 结束 Excel 转换函数 =====


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/search', methods=['POST'])
def search():
    global proc, extra_procs, temp_dirs, _proc_label_map
    if proc is not None:
        socketio.emit('message', {'message': '?????? Busy ??????\n'})
        return "Busy"

    data = request.json or {}
    keyword = data.get('keyword', '')
    if not keyword:
        abort(400)
    context_before = int(data.get('context_before', 0) or 0)
    context_after = int(data.get('context_after', 0) or 0)
    file = (data.get('file') or '').strip()

    # 基本 rg 参数
    rg_base = ['rg', '-uuu', '--smart-case', '--json']

    # 确认 rg 可用，避免运行时报错
    if not has_cmd('rg'):
        socketio.emit('message', {'message': 'ripgrep 未安装或不可用，请在系统 PATH 中提供 rg。'})
        return "rg not found"

    # 上下文参数
    if context_before and context_before > 0:
        rg_base += ['-B', str(context_before)]
    if context_after and context_after > 0:
        rg_base += ['-A', str(context_after)]

    # 默认搜索路径：优先 /data，如果不存在就使用项目目录
    data_dir = '/data'
    if not os.path.isdir(data_dir):
        data_dir = os.path.dirname(__file__)

    # 根据是否指定文件构建最终检索路径
    include_glob_for_basename = None
    data_dir_abs = os.path.abspath(data_dir)
    if file:
        candidate_norm = os.path.normpath(os.path.join(data_dir_abs, file))
        candidate_abs = os.path.abspath(candidate_norm)
        # 仅允许在 data_dir 内部
        if candidate_abs.startswith(data_dir_abs + os.sep) or candidate_abs == data_dir_abs:
            # 若解析后的路径存在（文件或目录），直接使用
            if os.path.exists(candidate_abs):
                search_path = candidate_abs
            else:
                # 如果仅是文件名（没有路径分隔），在整个 data_dir 下匹配该文件名
                base_only = (os.path.basename(file) == file) and ('/' not in file) and ('\\' not in file)
                if base_only:
                    include_glob_for_basename = os.path.basename(file)
                    search_path = data_dir_abs
                else:
                    # 回退：限制在 data_dir 内并使用 basename
                    search_path = os.path.join(data_dir_abs, os.path.basename(file))
        else:
            search_path = os.path.join(data_dir_abs, os.path.basename(file))
    else:
        search_path = data_dir_abs

    # 若设置了仅文件名过滤，使用 rg 的 glob 进行包含匹配
    if include_glob_for_basename:
        rg_base += ['--glob', f'**/{include_glob_for_basename}']

    # 将要运行并监听输出的 rg 进程列表（主 rg + 为每个压缩/归档额外启动的 rg）
    all_rg_procs = []
    # 附加的系统解压/解包进程（解压器、bsdtar/unzip/7z/rg），便于 cancel 清理
    extra_procs_local = []

    # 预先计算 total_files（尽量精确）
    total_files = 0
    try:
        # Helper: 启动 rg 并加入监听列表
        def start_rg_for_path(path, stdin_pipe=None, label=None, exclude_patterns=None, python_stream_feed=None):
            """
            启动 rg 以搜索 path 或从 stdin（使用 '-'）读取内容。
            当系统的 rg 不支持 --label 时，仍然接受 label 参数并把 label 存入 _proc_label_map 供后续使用。
            """
            cmd = rg_base.copy()
            if exclude_patterns:
                for pat in exclude_patterns:
                    cmd += ['--glob', f'!{pat}']
            supports_label = check_rg_supports_label()
            # 当通过 stdin 流式喂入时，确保将二进制当作文本处理（避免被跳过）
            if path == '-':
                cmd += ['-a']
            if label and supports_label:
                # rg 支持 --label：把 label 传给 rg（此时 rg 会在 JSON 的 begin/path 中显示 label）
                cmd += ['--label', label, '--', keyword, '-'] if path == '-' else ['--label', label, '--', keyword, path]
            else:
                # rg 不支持 --label：不传此参数，仍用 stdin 或 path 搜索
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
                    preexec_fn=os.setpgrp if hasattr(os, "setpgrp") else None
                )

            # 如果 rg 不支持 --label 且用户希望有 label，保存映射以便后续在解析时使用
            if label and not supports_label:
                try:
                    pid = rg_proc.pid
                    if pid is not None:
                        _proc_label_map[pid] = label
                except Exception:
                    pass

            all_rg_procs.append(rg_proc)
            return rg_proc

        # If user specified a single existing file -> handle as before but with python fallbacks
        if file and os.path.isfile(search_path):
            file_lower = file.lower()

            # === 支持 Excel 文件（xls/xlsx）直接流式检索 ===
            if is_excel_file(file_lower):
                total_files = 1
                safe_label = os.path.basename(file)
                # 使用 python 库将 excel 转为文本流并传给 rg
                def feed_fn(w, p=search_path):
                    stream_excel_to_writer(p, w)

                # 启动 rg（从 stdin 读取）
                try:
                    start_rg_for_path('-', label=safe_label, python_stream_feed=feed_fn)
                except Exception as e:
                    socketio.emit('message', {'message': f'?? Failed to start rg for excel: {e}\n'})
                    return "Excel stream failed"

            # === 处理单文件压缩：解压并按内部类型转换后流式喂入 rg ===
            elif is_single_file_compressed(file_lower):
                try:
                    rel_label = os.path.basename(file)
                    dec_cmd = build_decompress_command(file_lower, search_path)
                    inner_lower = strip_single_compress_ext(file_lower)
                    if dec_cmd:
                        decompressor_proc = subprocess.Popen(
                            dec_cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            shell=False,
                            preexec_fn=os.setpgrp if hasattr(os, "setpgrp") else None
                        )
                        extra_procs_local.append(decompressor_proc)
                        extra_procs.append(decompressor_proc)
                        if is_excel_file(inner_lower):
                            def feed_fn(w, proc=decompressor_proc, name=inner_lower):
                                data = proc.stdout.read() if proc.stdout else b''
                                stream_excel_bytes_to_writer(name, data, w)
                            start_rg_for_path('-', label=rel_label, python_stream_feed=feed_fn)
                        else:
                            start_rg_for_path('-', stdin_pipe=decompressor_proc.stdout, label=rel_label)
                    else:
                        # 使用 python 解压
                        if is_excel_file(inner_lower):
                            def feed_fn(w, p=search_path, e=file_lower, inner=inner_lower):
                                bio = io.BytesIO()
                                python_decompress_feed(p, e, bio)
                                data = bio.getvalue()
                                stream_excel_bytes_to_writer(inner, data, w)
                            start_rg_for_path('-', label=rel_label, python_stream_feed=feed_fn)
                        else:
                            def feed_fn(w, p=search_path, e=file_lower):
                                python_decompress_feed(p, e, w)
                            start_rg_for_path('-', label=rel_label, python_stream_feed=feed_fn)
                except Exception as e:
                    socketio.emit('message', {'message': f'?? Single-compressed stream failed: {e}\n'})
                    return "Single-compressed stream failed"

            # === 直接流式处理 tar 系列归档（gz/bz2/xz/无压缩） ===
            elif file_lower.endswith(('.tar.gz', '.tgz', '.tar.bz2', '.tbz2', '.tar.xz', '.txz', '.tar')):
                # 直接流式处理 tar 归档内容，无需落盘；为每个归档内文件启动一个 rg（stdin）
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
                    total_files = 0
                    with tarfile.open(search_path, mode=tar_mode) as tar:
                        for member in tar.getmembers():
                            if member.isfile():
                                total_files += 1
                                f = tar.extractfile(member)
                                if f:
                                    label = f"{os.path.basename(file)}/{member.name}"
                                    cmd = rg_base.copy()
                                    supports_label = check_rg_supports_label()
                                    if supports_label:
                                        cmd += ['--label', label, '--', keyword, '-']
                                    else:
                                        cmd += ['--', keyword, '-']
                                    # 启动 rg 并把归档内文件内容写入其 stdin
                                    try:
                                        rg_proc = subprocess.Popen(
                                            cmd,
                                            stdin=subprocess.PIPE,
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.STDOUT,
                                            shell=False,
                                            preexec_fn=os.setpgrp if hasattr(os, "setpgrp") else None
                                        )
                                        extra_procs_local.append(rg_proc)
                                        all_rg_procs.append(rg_proc)
                                        if not supports_label:
                                            try:
                                                _proc_label_map[rg_proc.pid] = label
                                            except Exception:
                                                pass
                                        # 将文件流写入 rg stdin（阻塞写入是可以的，因为这是独立进程）
                                        try:
                                            lower = member.name.lower()
                                            if is_excel_file(lower):
                                                data = f.read()
                                                stream_excel_bytes_to_writer(lower, data, rg_proc.stdin)
                                            elif is_csv_file(lower):
                                                stream_csv_fileobj_to_writer(f, rg_proc.stdin)
                                            else:
                                                shutil.copyfileobj(f, rg_proc.stdin)
                                        except Exception:
                                            pass
                                        try:
                                            rg_proc.stdin.close()
                                        except Exception:
                                            pass
                                    except Exception:
                                        # 如果启动 rg 失败，跳过该文件
                                        continue
                    # 如果归档为空，仍要保证至少一个进程以触发“无结果”逻辑
                    if total_files == 0:
                        total_files = 1
                        start_rg_for_path(search_path)
                except Exception as e:
                    socketio.emit('message', {'message': f'?? Tar stream failed: {e}\n'})
                    return "Tar stream failed"

            # 直接流式处理 zip/rar/7z 等归档（无外部 extractor 情况下的回退） ===
            elif is_archive_multi_file(file_lower):
                # 优先尝试 python 库的“逐文件流式读取并喂入 rg”方式，避免依赖外部 extractor
                streamed = False
                try:
                    # zip
                    if file_lower.endswith(('.zip', '.jar', '.war')):
                        import zipfile
                        with zipfile.ZipFile(search_path, 'r') as zf:
                            members = zf.namelist()
                            total_files = 0
                            for name in members:
                                # 跳过目录
                                if name.endswith('/'):
                                    continue
                                total_files += 1
                                label = f"{os.path.basename(file)}/{name}"
                                supports_label = check_rg_supports_label()
                                if supports_label:
                                    cmd = rg_base.copy() + ['-a', '--label', label, '--', keyword, '-']
                                else:
                                    cmd = rg_base.copy() + ['-a', '--', keyword, '-']
                                try:
                                    rg_proc = subprocess.Popen(
                                        cmd,
                                        stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT,
                                        shell=False,
                                        preexec_fn=os.setpgrp if hasattr(os, "setpgrp") else None
                                    )
                                    extra_procs_local.append(rg_proc)
                                    all_rg_procs.append(rg_proc)
                                    if not supports_label:
                                        try:
                                            _proc_label_map[rg_proc.pid] = label
                                        except Exception:
                                            pass
                                    with zf.open(name, 'r') as member_f:
                                        try:
                                            lower = name.lower()
                                            if is_excel_file(lower):
                                                spool_stream_to_temp_then_stream_excel(lower, member_f, rg_proc.stdin)
                                            elif is_csv_file(lower):
                                                stream_csv_fileobj_to_writer(member_f, rg_proc.stdin)
                                            else:
                                                copy_fileobj_chunked(member_f, rg_proc.stdin)
                                        except Exception:
                                            pass
                                    try:
                                        rg_proc.stdin.close()
                                    except Exception:
                                        pass
                                except Exception:
                                    continue
                            streamed = True
                    # rar
                    elif file_lower.endswith('.rar'):
                        streamed_local = False
                        # 首先尝试使用 rarfile 库进行逐成员流式处理
                        try:
                            import rarfile
                            rf = rarfile.RarFile(search_path)
                            members = rf.infolist()
                            total_files = 0
                            for mi in members:
                                if mi.isdir():
                                    continue
                                total_files += 1
                                name = mi.filename
                                label = f"{os.path.basename(file)}/{name}"
                                supports_label = check_rg_supports_label()
                                if supports_label:
                                    cmd = rg_base.copy() + ['-a', '--label', label, '--', keyword, '-']
                                else:
                                    cmd = rg_base.copy() + ['-a', '--', keyword, '-']
                                try:
                                    rg_proc = subprocess.Popen(
                                        cmd,
                                        stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT,
                                        shell=False,
                                        preexec_fn=os.setpgrp if hasattr(os, "setpgrp") else None
                                    )
                                    extra_procs_local.append(rg_proc)
                                    all_rg_procs.append(rg_proc)
                                    if not supports_label:
                                        try:
                                            _proc_label_map[rg_proc.pid] = label
                                        except Exception:
                                            pass
                                    with rf.open(mi) as member_f:
                                        try:
                                            lower = name.lower()
                                            if is_excel_file(lower):
                                                spool_stream_to_temp_then_stream_excel(lower, member_f, rg_proc.stdin)
                                            elif is_csv_file(lower):
                                                stream_csv_fileobj_to_writer(member_f, rg_proc.stdin)
                                            else:
                                                copy_fileobj_chunked(member_f, rg_proc.stdin)
                                        except Exception:
                                            pass
                                    try:
                                        rg_proc.stdin.close()
                                    except Exception:
                                        pass
                                except Exception:
                                    continue
                            streamed_local = True
                        except Exception:
                            streamed_local = False
                        # 回退：使用 7z 对 .rar 进行无落盘逐成员流式处理
                        if not streamed_local and has_cmd('7z'):
                            try:
                                names = list_7z_members(search_path)
                                total_files = 0
                                for name in names:
                                    total_files += 1
                                    label = f"{os.path.basename(file)}/{name}"
                                    supports_label = check_rg_supports_label()
                                    if supports_label:
                                        cmd = rg_base.copy() + ['-a', '--label', label, '--', keyword, '-']
                                    else:
                                        cmd = rg_base.copy() + ['-a', '--', keyword, '-']
                                    try:
                                        rg_proc = subprocess.Popen(
                                            cmd,
                                            stdin=subprocess.PIPE,
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.STDOUT,
                                            shell=False,
                                            preexec_fn=os.setpgrp if hasattr(os, "setpgrp") else None
                                        )
                                        extra_procs_local.append(rg_proc)
                                        all_rg_procs.append(rg_proc)
                                        if not supports_label:
                                            try:
                                                _proc_label_map[rg_proc.pid] = label
                                            except Exception:
                                                pass
                                        dec_proc = subprocess.Popen(
                                            ['7z', 'x', '-so', search_path, name],
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE,
                                            shell=False,
                                            preexec_fn=os.setpgrp if hasattr(os, "setpgrp") else None
                                        )
                                        extra_procs_local.append(dec_proc)
                                        try:
                                            lower = name.lower()
                                            if is_excel_file(lower):
                                                spool_stream_to_temp_then_stream_excel(lower, dec_proc.stdout, rg_proc.stdin)
                                            elif is_csv_file(lower):
                                                stream_csv_fileobj_to_writer(dec_proc.stdout, rg_proc.stdin)
                                            else:
                                                copy_fileobj_chunked(dec_proc.stdout, rg_proc.stdin)
                                        except Exception:
                                            pass
                                        try:
                                            rg_proc.stdin.close()
                                        except Exception:
                                            pass
                                        try:
                                            dec_proc.stdout.close()
                                        except Exception:
                                            pass
                                        try:
                                            dec_proc.wait()
                                        except Exception:
                                            pass
                                    except Exception:
                                        continue
                                streamed_local = True
                            except Exception:
                                streamed_local = False
                        streamed = streamed_local
                    # 7z 优先使用 7z CLI 逐成员流式；若不可用则回退 py7zr
                    elif file_lower.endswith('.7z'):
                        streamed_local = False
                        if has_cmd('7z'):
                            try:
                                names = list_7z_members(search_path)
                                total_files = 0
                                for name in names:
                                    total_files += 1
                                    label = f"{os.path.basename(file)}/{name}"
                                    supports_label = check_rg_supports_label()
                                    cmd = rg_base.copy() + (['-a', '--label', label, '--', keyword, '-'] if supports_label else ['-a', '--', keyword, '-'])
                                    try:
                                        rg_proc = subprocess.Popen(
                                            cmd,
                                            stdin=subprocess.PIPE,
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.STDOUT,
                                            shell=False,
                                            preexec_fn=os.setpgrp if hasattr(os, "setpgrp") else None
                                        )
                                        extra_procs_local.append(rg_proc)
                                        all_rg_procs.append(rg_proc)
                                        if not supports_label:
                                            try:
                                                _proc_label_map[rg_proc.pid] = label
                                            except Exception:
                                                pass
                                        dec_proc = subprocess.Popen(
                                            ['7z', 'x', '-so', search_path, name],
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE,
                                            shell=False,
                                            preexec_fn=os.setpgrp if hasattr(os, "setpgrp") else None
                                        )
                                        extra_procs_local.append(dec_proc)
                                        try:
                                            lower = name.lower()
                                            if is_excel_file(lower):
                                                spool_stream_to_temp_then_stream_excel(lower, dec_proc.stdout, rg_proc.stdin)
                                            elif is_csv_file(lower):
                                                stream_csv_fileobj_to_writer(dec_proc.stdout, rg_proc.stdin)
                                            else:
                                                copy_fileobj_chunked(dec_proc.stdout, rg_proc.stdin)
                                        except Exception:
                                            pass
                                        try:
                                            rg_proc.stdin.close()
                                        except Exception:
                                            pass
                                        try:
                                            dec_proc.stdout.close()
                                        except Exception:
                                            pass
                                        try:
                                            dec_proc.wait()
                                        except Exception:
                                            pass
                                    except Exception:
                                        continue
                                streamed_local = True
                            except Exception:
                                streamed_local = False
                        if not streamed_local:
                            try:
                                import py7zr
                                with py7zr.SevenZipFile(search_path, mode='r') as z:
                                    try:
                                        names = [n for n in z.getnames() if n]
                                    except Exception:
                                        try:
                                            names = list((z.readall() or {}).keys())
                                        except Exception:
                                            names = []
                                    total_files = 0
                                    for name in names:
                                        total_files += 1
                                        label = f"{os.path.basename(file)}/{name}"
                                        supports_label = check_rg_supports_label()
                                        cmd = rg_base.copy() + (['-a', '--label', label, '--', keyword, '-'] if supports_label else ['-a', '--', keyword, '-'])
                                        try:
                                            rg_proc = subprocess.Popen(
                                                cmd,
                                                stdin=subprocess.PIPE,
                                                stdout=subprocess.PIPE,
                                                stderr=subprocess.STDOUT,
                                                shell=False,
                                                preexec_fn=os.setpgrp if hasattr(os, "setpgrp") else None
                                            )
                                            extra_procs_local.append(rg_proc)
                                            all_rg_procs.append(rg_proc)
                                            if not supports_label:
                                                try:
                                                    _proc_label_map[rg_proc.pid] = label
                                                except Exception:
                                                    pass
                                            try:
                                                dm = z.read([name])
                                                obj = dm.get(name)
                                                if obj is None:
                                                    raise Exception('py7zr read returned None')
                                                lower = name.lower()
                                                if is_excel_file(lower):
                                                    # 先落盘再流式
                                                    bio = io.BytesIO(obj if isinstance(obj, (bytes, bytearray)) else (obj.getvalue() if hasattr(obj, 'getvalue') else obj.read()))
                                                    spool_stream_to_temp_then_stream_excel(lower, bio, rg_proc.stdin)
                                                elif is_csv_file(lower):
                                                    if isinstance(obj, (bytes, bytearray)):
                                                        data_bytes = bytes(obj)
                                                        rg_proc.stdin.write(data_bytes)
                                                        if len(data_bytes) == 0 or data_bytes[-1] != 0x0A:
                                                            rg_proc.stdin.write(b'\n')
                                                    else:
                                                        stream_csv_fileobj_to_writer(obj, rg_proc.stdin)
                                                else:
                                                    if isinstance(obj, (bytes, bytearray)):
                                                        bio = io.BytesIO(bytes(obj))
                                                        copy_fileobj_chunked(bio, rg_proc.stdin)
                                                    else:
                                                        copy_fileobj_chunked(obj, rg_proc.stdin)
                                            except Exception:
                                                pass
                                            try:
                                                rg_proc.stdin.close()
                                            except Exception:
                                                pass
                                        except Exception:
                                            continue
                                streamed_local = True
                            except Exception:
                                streamed_local = False
                        streamed = streamed_local
                except Exception:
                    streamed = False

                if not streamed:
                    # 若 python 库流式处理失败，再退回到原来的“解包到临时目录”的策略（如有 unzip/7z/bsdtar）
                    temp_dir_for_archive = tempfile.mkdtemp(prefix='rg_archive_')
                    temp_dirs.append(temp_dir_for_archive)
                    extracted_ok = False
                    try:
                        import tarfile, zipfile
                        if file_lower.endswith(('.zip', '.jar', '.war')):
                            with zipfile.ZipFile(search_path, 'r') as zf:
                                safe_extract_zip(zf, temp_dir_for_archive)
                            extracted_ok = True
                        else:
                            # tar/other handled by tarfile
                            with tarfile.open(search_path, 'r:*') as tf:
                                safe_extract_tar(tf, temp_dir_for_archive)
                            extracted_ok = True
                    except Exception:
                        extracted_ok = False

                    if not extracted_ok:
                        if file_lower.endswith('.7z'):
                            ok, msg = try_py7zr_extract(search_path, temp_dir_for_archive)
                            if ok:
                                extracted_ok = True
                        elif file_lower.endswith('.rar'):
                            ok, msg = try_rarfile_extract(search_path, temp_dir_for_archive)
                            if ok:
                                extracted_ok = True

                    if not extracted_ok:
                        try:
                            if file_lower.endswith(('.zip', '.jar', '.war')):
                                extract_cmd = ['unzip', '-qq', search_path, '-d', temp_dir_for_archive]
                            elif file_lower.endswith(('.7z', '.rar')):
                                if has_cmd('7z'):
                                    extract_cmd = ['7z', 'x', '-y', search_path, f'-o{temp_dir_for_archive}']
                                else:
                                    raise FileNotFoundError('7z not available')
                            else:
                                extract_cmd = ['bsdtar', '-xf', search_path, '-C', temp_dir_for_archive]
                            extract_proc = subprocess.Popen(
                                extract_cmd,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                shell=False,
                                preexec_fn=os.setpgrp if hasattr(os, "setpgrp") else None
                            )
                            extra_procs_local.append(extract_proc)
                            extra_procs.append(extract_proc)
                            out, err = extract_proc.communicate()
                            if extract_proc.returncode != 0:
                                msg = err.decode('utf-8', errors='replace') if err else 'Unknown extraction error'
                                socketio.emit('message', {'message': f'?? Extraction failed: {msg}\n'})
                                try:
                                    shutil.rmtree(temp_dir_for_archive)
                                except Exception:
                                    pass
                                temp_dirs.remove(temp_dir_for_archive)
                                return "Extraction failed"
                        except FileNotFoundError:
                            socketio.emit('message', {'message': f'?? Missing extractor on system for archive: {file}\n'})
                            try:
                                shutil.rmtree(temp_dir_for_archive)
                            except Exception:
                                pass
                            temp_dirs.remove(temp_dir_for_archive)
                            return "Missing extractor"

                    # 解包后按目录启动 rg（同时为 Excel 文件单独流式转换）
                    excel_files_list = []
                    non_excel_count = 0
                    for root, _, fns in os.walk(temp_dir_for_archive):
                        for fn in fns:
                            lower = fn.lower()
                            full = os.path.join(root, fn)
                            if is_excel_file(lower):
                                excel_files_list.append(full)
                            else:
                                non_excel_count += 1
                    total_files = (non_excel_count + len(excel_files_list)) if (non_excel_count + len(excel_files_list)) > 0 else 1
                    # 目录扫描时排除 Excel，避免重复
                    exclude_patterns = [f'**/*{ext}' for ext in EXCEL_EXTS]
                    start_rg_for_path(temp_dir_for_archive, exclude_patterns=exclude_patterns)
                    # 对每个 Excel 文件做流式转换并交给 rg
                    for full in excel_files_list:
                        # label 使用 归档名/归档内相对路径
                        rel_inside = os.path.relpath(full, temp_dir_for_archive)
                        label = f"{os.path.basename(file)}/{rel_inside}"
                        def feed_fn(w, p=full):
                            stream_excel_to_writer(p, w)
                        try:
                            start_rg_for_path('-', label=label, python_stream_feed=feed_fn)
                        except Exception:
                            continue

            else:
                # single normal file: count as 1
                total_files = 1
                start_rg_for_path(search_path)
        else:
            # Directory search (default) -> include compressed, archives and excel
            non_excluded_count = 0
            compressed_files = []
            archive_files = []
            excel_files = []
            # gather files first so we can count precisely
            for root, _, fns in os.walk(search_path):
                for fn in fns:
                    # 若启用了仅文件名过滤，则只统计匹配的文件
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
                    else:
                        non_excluded_count += 1
            total_files = non_excluded_count + len(compressed_files) + len(excel_files)
            # For archives, try streaming with python libs first; if streaming succeeds, it will start rg per member.
            for full_archive in archive_files:
                streamed = False
                try:
                    alc = full_archive.lower()
                    # zip streaming
                    if alc.endswith(('.zip', '.jar', '.war')):
                        try:
                            import zipfile
                            with zipfile.ZipFile(full_archive, 'r') as zf:
                                members = [n for n in zf.namelist() if not n.endswith('/')]
                                if members:
                                    for name in members:
                                        label = os.path.relpath(full_archive, data_dir) + '/' + name
                                        supports_label = check_rg_supports_label()
                                        if supports_label:
                                            cmd = rg_base.copy() + ['-a', '--label', label, '--', keyword, '-']
                                        else:
                                            cmd = rg_base.copy() + ['-a', '--', keyword, '-']
                                        try:
                                            rg_proc = subprocess.Popen(
                                                cmd,
                                                stdin=subprocess.PIPE,
                                                stdout=subprocess.PIPE,
                                                stderr=subprocess.STDOUT,
                                                shell=False,
                                                preexec_fn=os.setpgrp if hasattr(os, "setpgrp") else None
                                            )
                                            extra_procs_local.append(rg_proc)
                                            all_rg_procs.append(rg_proc)
                                            if not supports_label:
                                                _proc_label_map[rg_proc.pid] = label
                                            with zf.open(name, 'r') as member_f:
                                                try:
                                                    lower = name.lower()
                                                    if is_excel_file(lower):
                                                        spool_stream_to_temp_then_stream_excel(lower, member_f, rg_proc.stdin)
                                                    elif is_csv_file(lower):
                                                        stream_csv_fileobj_to_writer(member_f, rg_proc.stdin)
                                                    else:
                                                        copy_fileobj_chunked(member_f, rg_proc.stdin)
                                                except Exception:
                                                    pass
                                            try:
                                                rg_proc.stdin.close()
                                            except Exception:
                                                pass
                                            total_files += 1
                                        except Exception:
                                            continue
                                    streamed = True
                        except Exception:
                            streamed = False
                    # rar streaming
                    elif alc.endswith('.rar'):
                        streamed_local = False
                        # 优先尝试 rarfile 库进行逐成员流式处理
                        try:
                            import rarfile
                            rf = rarfile.RarFile(full_archive)
                            members = rf.infolist()
                            for mi in members:
                                if mi.isdir():
                                    continue
                                name = mi.filename
                                label = os.path.relpath(full_archive, data_dir) + '/' + name
                                supports_label = check_rg_supports_label()
                                if supports_label:
                                    cmd = rg_base.copy() + ['-a', '--label', label, '--', keyword, '-']
                                else:
                                    cmd = rg_base.copy() + ['-a', '--', keyword, '-']
                                try:
                                    rg_proc = subprocess.Popen(
                                        cmd,
                                        stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT,
                                        shell=False,
                                        preexec_fn=os.setpgrp if hasattr(os, "setpgrp") else None
                                    )
                                    extra_procs_local.append(rg_proc)
                                    all_rg_procs.append(rg_proc)
                                    if not supports_label:
                                        _proc_label_map[rg_proc.pid] = label
                                    with rf.open(mi) as member_f:
                                        try:
                                            lower = name.lower()
                                            if is_excel_file(lower):
                                                spool_stream_to_temp_then_stream_excel(lower, member_f, rg_proc.stdin)
                                            elif is_csv_file(lower):
                                                stream_csv_fileobj_to_writer(member_f, rg_proc.stdin)
                                            else:
                                                copy_fileobj_chunked(member_f, rg_proc.stdin)
                                        except Exception:
                                            pass
                                    try:
                                        rg_proc.stdin.close()
                                    except Exception:
                                        pass
                                    total_files += 1
                                except Exception:
                                    continue
                            streamed_local = True
                        except Exception:
                            streamed_local = False
                        # 回退：使用 7z 对 .rar 进行无落盘逐成员流式处理
                        if not streamed_local and has_cmd('7z'):
                            try:
                                names = list_7z_members(full_archive)
                                for name in names:
                                    label = os.path.relpath(full_archive, data_dir) + '/' + name
                                    supports_label = check_rg_supports_label()
                                    if supports_label:
                                        cmd = rg_base.copy() + ['-a', '--label', label, '--', keyword, '-']
                                    else:
                                        cmd = rg_base.copy() + ['-a', '--', keyword, '-']
                                    try:
                                        rg_proc = subprocess.Popen(
                                            cmd,
                                            stdin=subprocess.PIPE,
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.STDOUT,
                                            shell=False,
                                            preexec_fn=os.setpgrp if hasattr(os, "setpgrp") else None
                                        )
                                        extra_procs_local.append(rg_proc)
                                        all_rg_procs.append(rg_proc)
                                        if not supports_label:
                                            _proc_label_map[rg_proc.pid] = label
                                        dec_proc = subprocess.Popen(
                                            ['7z', 'x', '-so', full_archive, name],
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE,
                                            shell=False,
                                            preexec_fn=os.setpgrp if hasattr(os, "setpgrp") else None
                                        )
                                        extra_procs_local.append(dec_proc)
                                        try:
                                            lower = name.lower()
                                            if is_excel_file(lower):
                                                spool_stream_to_temp_then_stream_excel(lower, dec_proc.stdout, rg_proc.stdin)
                                            elif is_csv_file(lower):
                                                stream_csv_fileobj_to_writer(dec_proc.stdout, rg_proc.stdin)
                                            else:
                                                copy_fileobj_chunked(dec_proc.stdout, rg_proc.stdin)
                                        except Exception:
                                            pass
                                        try:
                                            rg_proc.stdin.close()
                                        except Exception:
                                            pass
                                        try:
                                            dec_proc.stdout.close()
                                        except Exception:
                                            pass
                                        try:
                                            dec_proc.wait()
                                        except Exception:
                                            pass
                                    except Exception:
                                        continue
                                streamed_local = True
                            except Exception:
                                streamed_local = False
                        streamed = streamed_local
                    # 7z 优先使用 7z CLI 逐成员流式；若不可用则回退 py7zr
                    elif alc.endswith('.7z'):
                        streamed_local = False
                        if has_cmd('7z'):
                            try:
                                names = list_7z_members(full_archive)
                                for name in names:
                                    label = os.path.relpath(full_archive, data_dir) + '/' + name
                                    supports_label = check_rg_supports_label()
                                    if supports_label:
                                        cmd = rg_base.copy() + ['-a', '--label', label, '--', keyword, '-']
                                    else:
                                        cmd = rg_base.copy() + ['-a', '--', keyword, '-']
                                    try:
                                        rg_proc = subprocess.Popen(
                                            cmd,
                                            stdin=subprocess.PIPE,
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.STDOUT,
                                            shell=False,
                                            preexec_fn=os.setpgrp if hasattr(os, "setpgrp") else None
                                        )
                                        extra_procs_local.append(rg_proc)
                                        all_rg_procs.append(rg_proc)
                                        if not supports_label:
                                            _proc_label_map[rg_proc.pid] = label
                                        dec_proc = subprocess.Popen(
                                            ['7z', 'x', '-so', full_archive, name],
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE,
                                            shell=False,
                                            preexec_fn=os.setpgrp if hasattr(os, "setpgrp") else None
                                        )
                                        extra_procs_local.append(dec_proc)
                                        try:
                                            lower = name.lower()
                                            if is_excel_file(lower):
                                                spool_stream_to_temp_then_stream_excel(lower, dec_proc.stdout, rg_proc.stdin)
                                            elif is_csv_file(lower):
                                                stream_csv_fileobj_to_writer(dec_proc.stdout, rg_proc.stdin)
                                            else:
                                                copy_fileobj_chunked(dec_proc.stdout, rg_proc.stdin)
                                        except Exception:
                                            pass
                                        try:
                                            rg_proc.stdin.close()
                                        except Exception:
                                            pass
                                        try:
                                            dec_proc.stdout.close()
                                        except Exception:
                                            pass
                                        try:
                                            dec_proc.wait()
                                        except Exception:
                                            pass
                                    except Exception:
                                        continue
                                streamed_local = True
                            except Exception:
                                streamed_local = False
                        if not streamed_local:
                            try:
                                import py7zr
                                with py7zr.SevenZipFile(full_archive, mode='r') as z:
                                    names = []
                                    try:
                                        names = z.getnames()
                                    except Exception:
                                        names = []
                                    for name in names:
                                        if not name:
                                            continue
                                        label = os.path.relpath(full_archive, data_dir) + '/' + name
                                        supports_label = check_rg_supports_label()
                                        if supports_label:
                                            cmd = rg_base.copy() + ['-a', '--label', label, '--', keyword, '-']
                                        else:
                                            cmd = rg_base.copy() + ['-a', '--', keyword, '-']
                                        try:
                                            rg_proc = subprocess.Popen(
                                                cmd,
                                                stdin=subprocess.PIPE,
                                                stdout=subprocess.PIPE,
                                                stderr=subprocess.STDOUT,
                                                shell=False,
                                                preexec_fn=os.setpgrp if hasattr(os, "setpgrp") else None
                                            )
                                            extra_procs_local.append(rg_proc)
                                            all_rg_procs.append(rg_proc)
                                            if not supports_label:
                                                _proc_label_map[rg_proc.pid] = label
                                            # Read single member data
                                            try:
                                                dm = z.read([name])
                                                obj = dm.get(name)
                                                if obj is None:
                                                    raise Exception('py7zr read returned None')
                                                lower = name.lower()
                                                if is_excel_file(lower):
                                                    bio = io.BytesIO(obj if isinstance(obj, (bytes, bytearray)) else (obj.getvalue() if hasattr(obj, 'getvalue') else obj.read()))
                                                    spool_stream_to_temp_then_stream_excel(lower, bio, rg_proc.stdin)
                                                elif is_csv_file(lower):
                                                    if isinstance(obj, (bytes, bytearray)):
                                                        data_bytes = bytes(obj)
                                                        rg_proc.stdin.write(data_bytes)
                                                        if len(data_bytes) == 0 or data_bytes[-1] != 0x0A:
                                                            try:
                                                                rg_proc.stdin.write(b'\n')
                                                            except Exception:
                                                                pass
                                                    else:
                                                        stream_csv_fileobj_to_writer(obj, rg_proc.stdin)
                                                else:
                                                    if isinstance(obj, (bytes, bytearray)):
                                                        bio = io.BytesIO(bytes(obj))
                                                        copy_fileobj_chunked(bio, rg_proc.stdin)
                                                    else:
                                                        copy_fileobj_chunked(obj, rg_proc.stdin)
                                            except Exception:
                                                pass
                                            try:
                                                rg_proc.stdin.close()
                                            except Exception:
                                                pass
                                            total_files += 1
                                        except Exception:
                                            continue
                                streamed_local = True
                            except Exception:
                                streamed_local = False
                        streamed = streamed_local
                except Exception:
                    streamed = False

                if streamed:
                    # 已由流式处理启动 rg，无需再 extract 到临时目录
                    continue

                # 若流式失败，回退到原有解包到 temp dir 的方式（仍然存在外部 extractor 的依赖）
                try:
                    temp_dir_for_archive = tempfile.mkdtemp(prefix='rg_archive_')
                    temp_dirs.append(temp_dir_for_archive)
                    extracted_ok = False
                    try:
                        import tarfile, zipfile
                        if full_archive.lower().endswith(('.zip', '.jar', '.war')):
                            with zipfile.ZipFile(full_archive, 'r') as zf:
                                safe_extract_zip(zf, temp_dir_for_archive)
                            extracted_ok = True
                        else:
                            with tarfile.open(full_archive, 'r:*') as tf:
                                safe_extract_tar(tf, temp_dir_for_archive)
                            extracted_ok = True
                    except Exception:
                        extracted_ok = False

                    if not extracted_ok:
                        if full_archive.lower().endswith('.7z'):
                            ok, msg = try_py7zr_extract(full_archive, temp_dir_for_archive)
                            if ok:
                                extracted_ok = True
                        elif full_archive.lower().endswith('.rar'):
                            ok, msg = try_rarfile_extract(full_archive, temp_dir_for_archive)
                            if ok:
                                extracted_ok = True

                    if not extracted_ok:
                        try:
                            if full_archive.lower().endswith(('.zip', '.jar', '.war')):
                                extract_cmd = ['unzip', '-qq', full_archive, '-d', temp_dir_for_archive]
                            elif full_archive.lower().endswith(('.7z', '.rar')):
                                if has_cmd('7z'):
                                    extract_cmd = ['7z', 'x', '-y', full_archive, f'-o{temp_dir_for_archive}']
                                else:
                                    raise FileNotFoundError('7z not available')
                            else:
                                extract_cmd = ['bsdtar', '-xf', full_archive, '-C', temp_dir_for_archive]
                            extract_proc = subprocess.Popen(
                                extract_cmd,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                shell=False,
                                preexec_fn=os.setpgrp if hasattr(os, "setpgrp") else None
                            )
                            extra_procs_local.append(extract_proc)
                            extra_procs.append(extract_proc)
                            out, err = extract_proc.communicate()
                            if extract_proc.returncode != 0:
                                try:
                                    shutil.rmtree(temp_dir_for_archive)
                                except Exception:
                                    pass
                                temp_dirs.remove(temp_dir_for_archive)
                                continue
                        except FileNotFoundError:
                            try:
                                shutil.rmtree(temp_dir_for_archive)
                            except Exception:
                                pass
                            temp_dirs.remove(temp_dir_for_archive)
                            continue
                    cnt = 0
                    for _, _, fns in os.walk(temp_dir_for_archive):
                        for _ in fns:
                            cnt += 1
                    total_files += cnt if cnt > 0 else 1
                    start_rg_for_path(temp_dir_for_archive)
                except Exception:
                    continue

            # start main rg for non-compressed files, excluding compressed/archives/excel to avoid duplicates
            exclude_patterns = []
            for ext in SINGLE_COMPRESSED_EXTS + ARCHIVE_EXTS + EXCEL_EXTS:
                exclude_patterns.append(f'**/*{ext}')
            start_rg_for_path(search_path, exclude_patterns=exclude_patterns)

            # start rg for each compressed file (streamed). Prefer external tool; if missing, use python stream feed
            for full in compressed_files:
                fn = os.path.basename(full)
                fn_lower = fn.lower()
                dec_cmd = build_decompress_command(fn_lower, full)
                rel_label = os.path.relpath(full, data_dir)
                if dec_cmd:
                    try:
                        decompressor_proc = subprocess.Popen(
                            dec_cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            shell=False,
                            preexec_fn=os.setpgrp if hasattr(os, "setpgrp") else None
                        )
                        extra_procs_local.append(decompressor_proc)
                        extra_procs.append(decompressor_proc)
                        inner_lower = strip_single_compress_ext(fn_lower)
                        if is_excel_file(inner_lower):
                            def feed_fn(w, proc=decompressor_proc, name=inner_lower):
                                data = proc.stdout.read() if proc.stdout else b''
                                stream_excel_bytes_to_writer(name, data, w)
                            start_rg_for_path('-', label=rel_label, python_stream_feed=feed_fn)
                        else:
                            start_rg_for_path('-', stdin_pipe=decompressor_proc.stdout, label=rel_label)
                    except FileNotFoundError:
                        inner_lower = strip_single_compress_ext(fn_lower)
                        if is_excel_file(inner_lower):
                            def feed_fn(w, p=full, e=fn_lower, inner=inner_lower):
                                bio = io.BytesIO()
                                python_decompress_feed(p, e, bio)
                                data = bio.getvalue()
                                stream_excel_bytes_to_writer(inner, data, w)
                            start_rg_for_path('-', label=rel_label, python_stream_feed=feed_fn)
                        else:
                            def feed_fn(w, p=full, e=fn_lower):
                                python_decompress_feed(p, e, w)
                            start_rg_for_path('-', label=rel_label, python_stream_feed=feed_fn)
                else:
                    inner_lower = strip_single_compress_ext(fn_lower)
                    if is_excel_file(inner_lower):
                        def feed_fn(w, p=full, e=fn_lower, inner=inner_lower):
                            bio = io.BytesIO()
                            python_decompress_feed(p, e, bio)
                            data = bio.getvalue()
                            stream_excel_bytes_to_writer(inner, data, w)
                        try:
                            start_rg_for_path('-', label=rel_label, python_stream_feed=feed_fn)
                        except Exception:
                            continue
                    else:
                        def feed_fn(w, p=full, e=fn_lower):
                            python_decompress_feed(p, e, w)
                        try:
                            start_rg_for_path('-', label=rel_label, python_stream_feed=feed_fn)
                        except Exception:
                            continue

            # 新增：对每个 excel 文件做流式转换并交给 rg（不落盘）
            for full in excel_files:
                fn = os.path.basename(full)
                rel_label = os.path.relpath(full, data_dir)
                # python feed：把 excel 转为文本写入 rg stdin
                def feed_fn(w, p=full):
                    stream_excel_to_writer(p, w)
                try:
                    start_rg_for_path('-', label=rel_label, python_stream_feed=feed_fn)
                except Exception:
                    continue

        # If no rg started, error
        if not all_rg_procs:
            socketio.emit('message', {'message': '?? No searchable files or rg failed to start\n'})
            return "No search procs"

        # 统一监听这些 rg 进程的 stdout：使用队列 + 每个进程独立线程转发 stdout -> 队列
        q = queue.Queue()
        total_procs = len(all_rg_procs)

        def forward_stdout(p):
            pid = getattr(p, 'pid', id(p))
            try:
                if p.stdout is None:
                    return
                for raw in p.stdout:
                    q.put((raw, pid))
            except Exception:
                pass
            finally:
                # 标识该进程的 EOF
                q.put((None, pid))

        for p in all_rg_procs:
            t = threading.Thread(target=forward_stdout, args=(p,), daemon=True)
            t.start()
            extra_procs_local.append(p)
            extra_procs.append(p)

        # 将第一个 rg 进程作为主引用（用于 cancel 等），但后台会管理所有进程
        proc = all_rg_procs[0]

    except Exception as e:
        socketio.emit('message', {'message': f'?? Start failed: {e}\n'})
        for p in extra_procs_local:
            try:
                os.killpg(os.getpgid(p.pid), signal.SIGTERM)
            except Exception:
                try:
                    p.terminate()
                except Exception:
                    pass
        for d in temp_dirs:
            try:
                shutil.rmtree(d)
            except Exception:
                pass
        extra_procs_local = []
        proc = None
        return "Error"

    # 读取与解析合并后的流（从队列读取）
    def get_output_loop():
        global proc, extra_procs, temp_dirs, _proc_label_map
        buf = []
        output_buffers[keyword] = buf
        # match_count 现在按实际输出的“内容区块”计数，保证与页面显示一致
        match_count = 0
        files_done = 0
        try:
            import json
            with app.app_context():
                context_before_n = context_before if context_before > 0 else 0
                context_after_n = context_after if context_after > 0 else 0
                before_lines = []
                after_lines = []
                block_main = None
                block_ready = False
                first_block = True

                # 发送初始匹配数和总文件数（files_total）
                socketio.emit('progress', {'matches': match_count, 'files_total': total_files, 'files_done': files_done})

                # loop: 从队列中读取行，直到所有 rg 进程发出 EOF 标记
                eof_set = set()
                while True:
                    try:
                        raw_item, owner = q.get(timeout=0.1)
                    except queue.Empty:
                        # 检查进程是否全部退出并且队列空 => 结束
                        all_ended = True
                        for p in extra_procs_local:
                            if p.poll() is None:
                                all_ended = False
                                break
                        if all_ended and q.empty():
                            break
                        continue

                    if raw_item is None:
                        eof_set.add(owner)
                        if len(eof_set) >= total_procs:
                            break
                        else:
                            continue

                    # 解析单行
                    try:
                        line = raw_item.decode('utf-8', errors='replace')
                    except Exception:
                        continue
                    if not line.strip():
                        continue
                    try:
                        obj = json.loads(line)
                    except Exception:
                        # 非 JSON 行（通常为外部解压/提示），忽略以保持纯净内容输出
                        continue

                    typ = obj.get('type')
                    # handle 'begin' to update files_done
                    if typ == 'begin':
                        # 如果 rg 不支持 --label，则尝试注入 label 到 obj 的 path 字段（仅本地使用）
                        if 'data' in obj and 'path' not in obj.get('data', {}) and owner in _proc_label_map:
                            try:
                                obj['data']['path'] = {'text': _proc_label_map.get(owner)}
                            except Exception:
                                pass
                        # 本次文件开始时不增加 files_done，保持以文件结束为准
                        # 重置本文件缓冲
                        before_lines = []
                        after_lines = []
                        block_main = None
                        block_ready = False
                    elif typ == 'context':
                        data = obj.get('data', {})
                        line_text = data.get('lines', {}).get('text', '')
                        line_text = (line_text or '').strip()
                        if block_main is None:
                            before_lines.append(line_text)
                            if len(before_lines) > context_before_n:
                                before_lines.pop(0)
                        else:
                            after_lines.append(line_text)
                    elif typ == 'match':
                        data = obj.get('data', {})
                        line_text = data.get('lines', {}).get('text', '')
                        line_text = (line_text or '').strip()
                        block_main = line_text
                        after_lines = []
                        block_ready = True
                    elif typ == 'end':
                        # Flush any pending block with padded empty lines at file boundary
                        if block_ready:
                            block = []
                            for i in range(context_before_n):
                                block.append(before_lines[i] if i < len(before_lines) else '')
                            block.append(block_main if block_main else '')
                            for i in range(context_after_n):
                                block.append(after_lines[i] if i < len(after_lines) else '')
                            out_block = '\n'.join(block)
                            if out_block.strip():
                                if not first_block:
                                    buf.append('')
                                    socketio.emit('message', {'message': '\n'})

                                buf.append(out_block)
                                socketio.emit('message', {'message': out_block + '\n'})
                                match_count += 1
                                socketio.emit('progress', {
                                    'matches': match_count,
                                    'files_total': total_files,
                                    'files_done': files_done
                                })
                                first_block = False
                            else:
                                socketio.emit('message', {'message': '\n'})
                            block_ready = False
                        # reset buffers at end of file
                        before_lines = []
                        after_lines = []
                        block_main = None
                        block_ready = False
                        # 文件结束：更新完成数并推送进度
                        files_done += 1
                        socketio.emit('progress', {
                            'matches': match_count,
                            'files_total': total_files,
                            'files_done': files_done
                        })

                    # 输出区块（每次命中后，等下文收集够了再输出）
                    if block_ready and (len(after_lines) >= context_after_n):
                        block = []
                        for i in range(context_before_n):
                            block.append(before_lines[i] if i < len(before_lines) else '')
                        block.append(block_main if block_main else '')
                        for i in range(context_after_n):
                            block.append(after_lines[i] if i < len(after_lines) else '')
                        out_block = '\n'.join(block)
                        # 只有非空区块计为匹配并输出
                        if out_block.strip():
                            if not first_block:
                                buf.append('')
                                socketio.emit('message', {'message': '\n'})
                            buf.append(out_block)
                            socketio.emit('message', {'message': out_block + '\n'})
                            match_count += 1
                            socketio.emit('progress', {
                                'matches': match_count,
                                'files_total': total_files,
                                'files_done': files_done
                            })
                            first_block = False
                        else:
                            # 纯空白块：把换行发给前端但不计为匹配
                            socketio.emit('message', {'message': '\n'})
                        block_ready = False
                        before_lines.append(block_main)
                        if len(before_lines) > context_before_n:
                            before_lines.pop(0)
                        block_main = None
                        after_lines = []

                # 处理最后一个可能未满足下文要求的区块（仍要输出）
                if block_ready:
                    block = []
                    for i in range(context_before_n):
                        block.append(before_lines[i] if i < len(before_lines) else '')
                    block.append(block_main if block_main else '')
                    for i in range(context_after_n):
                        block.append(after_lines[i] if i < len(after_lines) else '')
                    out_block = '\n'.join(block)
                    if out_block.strip():
                        if not first_block:
                            buf.append('')
                            socketio.emit('message', {'message': '\n'})
                        label = _proc_label_map.get(owner)
                        if label:
                            out_block = f"[{label}]\n" + out_block
                        buf.append(out_block)
                        socketio.emit('message', {'message': out_block + '\n'})
                        match_count += 1
                        socketio.emit('progress', {
                            'matches': match_count,
                            'files_total': total_files,
                            'files_done': files_done
                        })
                    else:
                        socketio.emit('message', {'message': '\n'})
        except Exception:
            # 兜底：管道可能已断，尝试将剩余 stdout 简单转发
            with app.app_context():
                try:
                    for p in extra_procs_local:
                        if p and p.stdout:
                            for raw in p.stdout:
                                text = raw.decode('utf-8', errors='replace')
                                # 补上 label（如果有）
                                label = _proc_label_map.get(getattr(p, 'pid', None))
                                if label:
                                    text = f"[{label}] {text}"
                                buf.append(text)
                                socketio.emit('message', {'message': text})
                except Exception:
                    pass
        finally:
            # 清理：等待所有 rg 及解压进程退出，终止额外进程并清理临时目录
            try:
                for p in extra_procs_local:
                    try:
                        p.wait(timeout=0.1)
                    except Exception:
                        pass
            except Exception:
                pass

            for p in list(extra_procs_local):
                try:
                    os.killpg(os.getpgid(p.pid), signal.SIGTERM)
                except Exception:
                    try:
                        p.terminate()
                    except Exception:
                        pass

            # 合并到全局 extra_procs 便于 cancel
            for p in extra_procs_local:
                if p not in extra_procs:
                    extra_procs.append(p)

            # 删除临时目录（若有）
            for d in list(temp_dirs):
                try:
                    shutil.rmtree(d)
                except Exception:
                    pass
            temp_dirs = []

            # 清理 pid->label 映射（避免内存泄漏）
            _proc_label_map = {}

            # 置空全局 proc（注意：proc 可能是第一个 rg）
            proc = None

            # 发送最终匹配数一次，确保前端能收到最终值
            socketio.emit('progress', {'matches': match_count, 'files_total': total_files, 'files_done': files_done})

            # 尝试保存导出
            try:
                save_export(keyword, buf)
            except Exception as e:
                with app.app_context():
                    socketio.emit('message', {'message': f'?? Save failed: {e}\n'})

            with app.app_context():
                socketio.emit('message', {'message': '?????? Done ??????\n'})

    # 启动后台读写线程
    thread = threading.Thread(target=get_output_loop, daemon=True)
    thread.start()
    # 立即通知前端搜索启动（前端会显示进度条）并发送初始 total_files
    socketio.emit('message', {'message': '?????? Started ??????\n'})
    socketio.emit('progress', {'files_total': total_files, 'files_done': 0, 'matches': 0})
    return "Started"


@app.route('/cancel', methods=['POST'])
def cancel():
    global proc, extra_procs, temp_dirs
    if proc:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except Exception:
            try:
                proc.terminate()
            except Exception:
                pass
    for p in list(extra_procs):
        try:
            os.killpg(os.getpgid(p.pid), signal.SIGTERM)
        except Exception:
            try:
                p.terminate()
            except Exception:
                pass
    extra_procs = []
    for d in list(temp_dirs):
        try:
            shutil.rmtree(d)
        except Exception:
            pass
    temp_dirs = []
    time.sleep(0.1)
    proc = None
    try:
        for kw, buf in list(output_buffers.items()):
            if buf:
                save_export(kw, buf)
    except Exception:
        pass
    socketio.emit('message', {'message': '??? Cancelled ???\n'})
    return "Cancelled"


def save_export(keyword, buf_lines):
    import datetime
    safe = ''.join(c for c in keyword if c.isalnum() or c in (' ', '_', '-')).strip()
    if not safe:
        safe = 'search'
    exports_dir = os.path.join(os.path.dirname(__file__), 'exports')
    os.makedirs(exports_dir, exist_ok=True)
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    filename = f"{safe}_{today}.txt"
    filepath = os.path.join(exports_dir, filename)
    if os.path.exists(filepath):
        ts = int(time.time())
        filename = f"{safe}_{today}_{ts}.txt"
        filepath = os.path.join(exports_dir, filename)
    content = '\n'.join(line.rstrip('\n') for line in buf_lines if line.strip() or line == '')
    if not content.endswith('\n'):
        content = content + '\n'
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    return filepath


@app.route('/download')
def download():
    keyword = request.args.get('keyword')
    if not keyword:
        abort(400)
    safe = ''.join(c for c in keyword if c.isalnum() or c in (' ', '_', '-')).strip()
    if not safe:
        safe = 'search'
    exports_dir = os.path.join(os.path.dirname(__file__), 'exports')
    candidates = []
    if os.path.isdir(exports_dir):
        for fn in os.listdir(exports_dir):
            if fn.startswith(safe):
                candidates.append(fn)
    if not candidates:
        abort(404)
    candidates.sort(key=lambda n: os.path.getmtime(os.path.join(exports_dir, n)), reverse=True)
    return send_from_directory(exports_dir, candidates[0], as_attachment=True)


@app.route('/files')
def list_files():
    data_dir = '/data'
    if not os.path.isdir(data_dir):
        data_dir = os.path.dirname(__file__)
    files = []
    try:
        for root, _, fns in os.walk(data_dir):
            for fn in fns:
                # 仅返回相对路径以便 UI 选择（包含压缩/归档文件）
                full = os.path.join(root, fn)
                rel = os.path.relpath(full, data_dir)
                files.append(rel)
    except Exception:
        files = []
    return jsonify(files)

if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5000)
