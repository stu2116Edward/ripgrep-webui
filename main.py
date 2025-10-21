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

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", path='/io')

# 全局当前正在运行的 ripgrep 进程（read loop 读取这些进程的 stdout）
proc = None
# 可能存在的额外进程（例如解压器/解包器），搜索/取消时需要一起清理
extra_procs = []
# 临时目录列表（例如对 archive 的解包目录），结束时需要清理
temp_dirs = []

output_buffers = {}  # 关键字 -> 行内容列表

# 支持的压缩/归档后缀（用于目录扫描时识别并单独处理）
SINGLE_COMPRESSED_EXTS = ('.gz', '.bz2', '.xz', '.lz4', '.lzma')
ARCHIVE_EXTS = ('.zip', '.jar', '.war', '.tar', '.tar.gz', '.tgz', '.tar.bz2', '.tbz2', '.tar.xz', '.txz')


def is_single_file_compressed(filename_lower):
    return filename_lower.endswith(SINGLE_COMPRESSED_EXTS)


def is_archive_multi_file(filename_lower):
    return filename_lower.endswith(ARCHIVE_EXTS)


def build_decompress_command(path_lower, real_path):
    if path_lower.endswith('.gz'):
        return ['gzip', '-dc', real_path]
    if path_lower.endswith('.bz2'):
        return ['bzip2', '-dc', real_path]
    if path_lower.endswith('.xz') or path_lower.endswith('.txz'):
        return ['xz', '-dc', real_path]
    if path_lower.endswith('.lz4'):
        return ['lz4', '-dc', real_path]
    if path_lower.endswith('.lzma'):
        return ['lzma', '-dc', real_path]
    return None


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


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/search', methods=['POST'])
def search():
    global proc, extra_procs, temp_dirs
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

    # 上下文参数
    if context_before and context_before > 0:
        rg_base += ['-B', str(context_before)]
    if context_after and context_after > 0:
        rg_base += ['-A', str(context_after)]

    # 默认搜索路径：优先 /data，如果不存在就使用项目目录（保证“所有文件”能工作）
    data_dir = '/data'
    if not os.path.isdir(data_dir):
        data_dir = os.path.dirname(__file__)

    # 根据是否指定文件构建最终检索路径
    if file:
        base = os.path.basename(file)
        if base:
            search_path = os.path.join(data_dir, base)
        else:
            search_path = data_dir
    else:
        search_path = data_dir

    # 将要运行并监听输出的 rg 进程列表（主 rg + 为每个压缩/归档额外启动的 rg）
    all_rg_procs = []
    # 附加的系统解压/解包进程（解压器、bsdtar/unzip），便于 cancel 清理
    extra_procs_local = []

    try:
        # Helper: 启动 rg 并加入监听列表
        def start_rg_for_path(path, stdin_pipe=None, label=None, exclude_patterns=None):
            cmd = rg_base.copy()
            if exclude_patterns:
                for pat in exclude_patterns:
                    cmd += ['--glob', f'!{pat}']
            if label:
                cmd += ['--label', label, '--', keyword, '-']
            else:
                cmd += ['--', keyword, path]
            rg_proc = subprocess.Popen(
                cmd,
                stdin=stdin_pipe,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                shell=False,
                preexec_fn=os.setpgrp if hasattr(os, "setpgrp") else None
            )
            all_rg_procs.append(rg_proc)
            return rg_proc

        # 如果 user 指定了单个文件并且文件存在：按之前逻辑处理（包括压缩/归档的特殊处理）
        if file and os.path.isfile(search_path):
            file_lower = file.lower()
            if is_single_file_compressed(file_lower):
                dec_cmd = build_decompress_command(file_lower, search_path)
                if dec_cmd is None:
                    socketio.emit('message', {'message': f'?? Unsupported compressed format for {file}\n'})
                    return "Unsupported format"
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
                except FileNotFoundError:
                    socketio.emit('message', {'message': f'?? Missing decompressor for {file}: {dec_cmd[0]}\n'})
                    return "Missing decompressor"
                safe_label = os.path.basename(file)
                rg_proc = start_rg_for_path('-', stdin_pipe=decompressor_proc.stdout, label=safe_label)
            elif is_archive_multi_file(file_lower):
                # extract to temp dir using python stdlib first, fallback to external extractor
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
                        with tarfile.open(search_path, 'r:*') as tf:
                            safe_extract_tar(tf, temp_dir_for_archive)
                        extracted_ok = True
                except Exception:
                    extracted_ok = False
                if not extracted_ok:
                    try:
                        if file_lower.endswith(('.zip', '.jar', '.war')):
                            extract_cmd = ['unzip', '-qq', search_path, '-d', temp_dir_for_archive]
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
                        socketio.emit('message', {'message': f'?? Missing extractor (bsdtar/unzip) on system\n'})
                        try:
                            shutil.rmtree(temp_dir_for_archive)
                        except Exception:
                            pass
                        temp_dirs.remove(temp_dir_for_archive)
                        return "Missing extractor"
                rg_proc = start_rg_for_path(temp_dir_for_archive)
            else:
                rg_proc = start_rg_for_path(search_path)
        else:
            # 搜索一个目录（默认）——需要包括压缩文件与归档
            # 1) 为主目录启动 rg，但排除识别到的压缩/归档扩展，避免重复或二次读取
            exclude_patterns = []
            for ext in SINGLE_COMPRESSED_EXTS + ARCHIVE_EXTS:
                exclude_patterns.append(f'**/*{ext}')
            main_rg = start_rg_for_path(search_path, exclude_patterns=exclude_patterns)

            # 2) 遍历目录，找到压缩/归档文件，为每个单独处理（流式或解包后再次 rg）
            for root, _, files in os.walk(search_path):
                for fn in files:
                    full = os.path.join(root, fn)
                    fn_lower = fn.lower()
                    rel_label = os.path.relpath(full, data_dir)
                    if is_single_file_compressed(fn_lower):
                        dec_cmd = build_decompress_command(fn_lower, full)
                        if dec_cmd is None:
                            # skip unsupported
                            continue
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
                        except FileNotFoundError:
                            # decompressor not available -> skip this file
                            continue
                        # 启动一个只对 stdin 进行匹配、带 label 的 rg，label 使用相对路径
                        start_rg_for_path('-', stdin_pipe=decompressor_proc.stdout, label=rel_label)
                    elif is_archive_multi_file(fn_lower):
                        # 为 archive 创建临时目录并尝试用 python 解包
                        temp_dir_for_archive = tempfile.mkdtemp(prefix='rg_archive_')
                        temp_dirs.append(temp_dir_for_archive)
                        extracted_ok = False
                        try:
                            import tarfile, zipfile
                            if fn_lower.endswith(('.zip', '.jar', '.war')):
                                with zipfile.ZipFile(full, 'r') as zf:
                                    safe_extract_zip(zf, temp_dir_for_archive)
                                extracted_ok = True
                            else:
                                with tarfile.open(full, 'r:*') as tf:
                                    safe_extract_tar(tf, temp_dir_for_archive)
                                extracted_ok = True
                        except Exception:
                            extracted_ok = False
                        if not extracted_ok:
                            try:
                                if fn_lower.endswith(('.zip', '.jar', '.war')):
                                    extract_cmd = ['unzip', '-qq', full, '-d', temp_dir_for_archive]
                                else:
                                    extract_cmd = ['bsdtar', '-xf', full, '-C', temp_dir_for_archive]
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
                        # 对解包的临时目录启动 rg
                        start_rg_for_path(temp_dir_for_archive)
                    else:
                        # 非压缩/非归档文件，主 rg 已覆盖
                        continue

        # 如果没有任何 rg 进程启动则报错
        if not all_rg_procs:
            socketio.emit('message', {'message': '?? No searchable files or rg failed to start\n'})
            return "No search procs"

        # 统一监听这些 rg 进程的 stdout：使用队列 + 每个进程独立线程转发 stdout -> 队列
        q = queue.Queue()
        proc_eof = set()
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
            # 加入 extra_procs 以便 cancel 时一并 kill（只包含外部解压/解包/rg）
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

    # -----------------------
    # 读取与解析合并后的流（从队列读取）
    # -----------------------
    def get_output_loop():
        global proc, extra_procs, temp_dirs
        buf = []
        output_buffers[keyword] = buf
        # match_count 现在按实际输出的“内容区块”计数，保证与页面显示一致
        match_count = 0
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

                # 发送初始匹配数（0）
                socketio.emit('progress', {'matches': match_count})

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
                        raw_text = line.strip()
                        if raw_text:
                            buf.append(raw_text)
                            socketio.emit('message', {'message': raw_text + '\n'})
                        continue

                    typ = obj.get('type')
                    if typ == 'begin':
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
                        # 注意：不在这里递增 match_count（因为尚未输出区块）
                    elif typ == 'end':
                        before_lines = []
                        after_lines = []
                        block_main = None
                        block_ready = False

                    # 输出区块（每次命中后，等下文收集够了再输出）
                    if block_ready and (len(after_lines) >= context_after_n):
                        block = []
                        for i in range(context_before_n):
                            block.append(before_lines[i] if i < len(before_lines) else '')
                        block.append(block_main if block_main else '')
                        for i in range(context_after_n):
                            block.append(after_lines[i] if i < len(after_lines) else '')
                        out_block = '\n'.join(block)
                        # 如果 out_block 非空白，则才视为一条有效显示结果并计数
                        if out_block.strip():
                            if not first_block:
                                buf.append('')
                                socketio.emit('message', {'message': '\n'})
                            buf.append(out_block)
                            socketio.emit('message', {'message': out_block + '\n'})
                            # 输出即为实际显示的一条结果块，递增计数并发送进度更新
                            match_count += 1
                            socketio.emit('progress', {'matches': match_count})
                            first_block = False
                        else:
                            # 纯空白块（可能全部为上下文空行），只发一个换行到前端但不计为匹配
                            socketio.emit('message', {'message': '\n'})
                        block_ready = False
                        # 把命中行也作为下一块的上文
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
                        buf.append(out_block)
                        socketio.emit('message', {'message': out_block + '\n'})
                        match_count += 1
                        socketio.emit('progress', {'matches': match_count})
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

            # 置空全局 proc（注意：proc 可能是第一个 rg）
            proc = None

            # 发送最终匹配数一次，确保前端能收到最终值
            socketio.emit('progress', {'matches': match_count})

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
    socketio.emit('message', {'message': '?????? Started ??????\n'})
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
