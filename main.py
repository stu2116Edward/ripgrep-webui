from flask import Flask, render_template, request
from flask_socketio import SocketIO, emit
import subprocess
import threading
import os
import signal
import time
from flask import send_from_directory, abort
from flask import jsonify

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", path='/io')

proc = None
output_buffers = {}  # 关键字 -> 行内容列表

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/search', methods=['POST'])
def search():
    global proc
    if proc is not None:
        socketio.emit('message', {'message': '?????? Busy ??????\n'})
        return "Busy"

    keyword = request.json['keyword']
    context_before = int(request.json.get('context_before', 0) or 0)
    context_after = int(request.json.get('context_after', 0) or 0)
    file = (request.json.get('file') or '').strip()

    # 构建 ripgrep 命令，支持上下文参数
    cmd = ['rg', '-uuu', '--smart-case', '--json']
    if context_before and context_before > 0:
        cmd += ['-B', str(context_before)]
    if context_after and context_after > 0:
        cmd += ['-A', str(context_after)]

    # 如果指定了文件，只允许文件名，防止路径穿越
    search_path = '/data'
    if file:
        base = os.path.basename(file)
        if base:
            search_path = os.path.join('/data', base)
    cmd += ['--', keyword, search_path]
    proc = subprocess.Popen(cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    def get_output():
        global proc
        buf = []
        output_buffers[keyword] = buf
        try:
            import json
            with app.app_context():
                # 新区块收集器
                context_before_n = context_before if context_before > 0 else 0
                context_after_n = context_after if context_after > 0 else 0
                before_lines = []
                after_lines = []
                block_main = None
                block_ready = False
                first_block = True
                # ripgrep 输出是流式的，需按顺序收集
                for raw in proc.stdout:
                    try:
                        line = raw.decode('utf-8', errors='replace')
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
                        # 新文件开始，清理残留
                        before_lines = []
                        after_lines = []
                        block_main = None
                        block_ready = False
                    elif typ == 'context':
                        data = obj.get('data', {})
                        line_text = data.get('lines', {}).get('text', '')
                        line_text = (line_text or '').strip()
                        if block_main is None:
                            # 收集上文
                            before_lines.append(line_text)
                            if len(before_lines) > context_before_n:
                                before_lines.pop(0)
                        else:
                            # 收集下文
                            after_lines.append(line_text)
                    elif typ == 'match':
                        # 命中，开始新块
                        data = obj.get('data', {})
                        line_text = data.get('lines', {}).get('text', '')
                        line_text = (line_text or '').strip()
                        block_main = line_text
                        after_lines = []
                        block_ready = True
                    elif typ == 'end':
                        # 文件结束，清理
                        before_lines = []
                        after_lines = []
                        block_main = None
                        block_ready = False
                    # 输出区块（每次命中后，等下文收集够了再输出）
                    if block_ready and (len(after_lines) >= context_after_n):
                        # 构造区块
                        block = []
                        # 上文
                        for i in range(context_before_n):
                            block.append(before_lines[i] if i < len(before_lines) else '')
                        # 主命中
                        block.append(block_main if block_main else '')
                        # 下文
                        for i in range(context_after_n):
                            block.append(after_lines[i] if i < len(after_lines) else '')
                        out_block = '\n'.join(block)
                        if not first_block:
                            buf.append('')
                            socketio.emit('message', {'message': '\n'})
                        buf.append(out_block)
                        socketio.emit('message', {'message': out_block + '\n'})
                        # 准备下一个区块
                        first_block = False
                        block_ready = False
                        # 上文滑动窗口：命中行也作为下一个区块的上文
                        before_lines.append(block_main)
                        if len(before_lines) > context_before_n:
                            before_lines.pop(0)
                        block_main = None
                        after_lines = []
                # 处理最后一个区块（如果下文不足也补齐）
                if block_ready:
                    block = []
                    for i in range(context_before_n):
                        block.append(before_lines[i] if i < len(before_lines) else '')
                    block.append(block_main if block_main else '')
                    for i in range(context_after_n):
                        block.append(after_lines[i] if i < len(after_lines) else '')
                    out_block = '\n'.join(block)
                    if not first_block:
                        buf.append('')
                        socketio.emit('message', {'message': '\n'})
                    buf.append(out_block)
                    socketio.emit('message', {'message': out_block + '\n'})
        except Exception:
            with app.app_context():
                for line in proc.stdout:
                    text = line.decode('utf-8')
                    buf.append(text)
                    socketio.emit('message', {'message': text})
        proc = None
        try:
            save_export(keyword, buf)
        except Exception as e:
            with app.app_context():
                socketio.emit('message', {'message': f'?? Save failed: {e}\n'})
        with app.app_context():
            socketio.emit('message', {'message': '?????? Done ??????\n'})

    thread = threading.Thread(target=get_output)
    thread.start()
    socketio.emit('message', {'message': '?????? Started ??????\n'})
    return "Started"

@app.route('/cancel', methods=['POST'])
def cancel():
    global proc
    if proc:
        try:
            # 终止进程组
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except Exception:
            try:
                proc.terminate()
            except Exception:
                pass
        # 等待线程刷新
        time.sleep(0.1)
        proc = None
        # 有缓冲内容时保存
        try:
            for kw, buf in list(output_buffers.items()):
                if buf:
                    save_export(kw, buf)
        except Exception:
            pass
    socketio.emit('message', {'message': '??? Cancelled ???\n'})
    return "Cancelled"


def save_export(keyword, buf_lines):
    """Save buffer lines to exports/<sanitized_keyword>.txt and return the file path."""
    import datetime
    safe = ''.join(c for c in keyword if c.isalnum() or c in (' ', '_', '-')).strip()
    if not safe:
        safe = 'search'
    exports_dir = os.path.join(os.path.dirname(__file__), 'exports')
    os.makedirs(exports_dir, exist_ok=True)
    # 文件名加上日期后缀
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    filename = f"{safe}_{today}.txt"
    filepath = os.path.join(exports_dir, filename)
    # 如果文件已存在，追加时间戳避免覆盖
    if os.path.exists(filepath):
        ts = int(time.time())
        filename = f"{safe}_{today}_{ts}.txt"
        filepath = os.path.join(exports_dir, filename)
    # 区块之间无空行，直接顺序拼接，页面显示什么导出就是什么
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
    # 优先精确文件名，否则选最新的前缀匹配
    candidates = []
    if os.path.isdir(exports_dir):
        for fn in os.listdir(exports_dir):
            if fn.startswith(safe):
                candidates.append(fn)
    if not candidates:
        abort(404)
    # 选最新的
    candidates.sort(key=lambda n: os.path.getmtime(os.path.join(exports_dir, n)), reverse=True)
    return send_from_directory(exports_dir, candidates[0], as_attachment=True)


@app.route('/files')
def list_files():
    # 列出 /data 下的文件，如果没有则列出项目目录下的文件
    data_dir = '/data'
    if not os.path.isdir(data_dir):
        data_dir = os.path.dirname(__file__)
    files = []
    try:
        for fn in os.listdir(data_dir):
            if os.path.isfile(os.path.join(data_dir, fn)):
                files.append(fn)
    except Exception:
        files = []
    return jsonify(files)

if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5000)
