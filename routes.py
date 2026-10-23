# -*- coding: utf-8 -*-
"""
路由模块
- 提供页面入口与静态资源映射
- 提供 /search /cancel /hot-reload /files /download 等后端接口
"""

import os
from datetime import datetime
from flask import (
    Blueprint, render_template, request, jsonify,
    send_from_directory, abort, current_app
)

from config import DEFAULT_DATA_DIR
from utils import emit_message_utf, sanitize_keyword, drop_file_cache_path
from search import start_search
from process import cancel as cancel_search
from process import trigger_hot_reload_async
from export import get_exports_dir, get_latest_export_filename


routes_bp = Blueprint('routes_bp', __name__)


@routes_bp.route('/')
def index():
    """主页：渲染模板"""
    return render_template('index.html')


@routes_bp.route('/css/<path:filename>')
def serve_css(filename):
    try:
        base = os.path.join(os.path.dirname(__file__), 'templates', 'css')
        return send_from_directory(base, filename)
    except Exception:
        abort(404)


@routes_bp.route('/js/<path:filename>')
def serve_js(filename):
    try:
        base = os.path.join(os.path.dirname(__file__), 'templates', 'js')
        return send_from_directory(base, filename)
    except Exception:
        abort(404)


@routes_bp.route('/exports/<path:filename>')
def serve_export_file(filename):
    """直接提供导出文件的下载（避免额外目录扫描与内存占用）。"""
    try:
        exports_dir = get_exports_dir()
        resp = send_from_directory(exports_dir, filename, as_attachment=True, conditional=True)
        # 在响应关闭后丢弃该导出文件的页面缓存，降低容器内存占用
        try:
            exports_abs = os.path.abspath(exports_dir)
            full_path = os.path.abspath(os.path.join(exports_dir, filename))
            if full_path.startswith(exports_abs + os.sep):
                resp.call_on_close(lambda: drop_file_cache_path(full_path))
        except Exception:
            pass
        return resp
    except Exception:
        abort(404)


@routes_bp.route('/files')
def list_files():
    """
    返回待检索的文件列表（相对路径）
    - 优先使用 /data 目录，不存在时回退到项目根目录
    """
    data_dir = DEFAULT_DATA_DIR
    if not os.path.isdir(data_dir):
        data_dir = os.path.dirname(__file__)
    files = []
    try:
        data_dir_abs = os.path.abspath(data_dir)
        for root, _, fns in os.walk(data_dir_abs):
            for fn in fns:
                full = os.path.join(root, fn)
                rel = os.path.relpath(full, data_dir_abs)
                files.append(rel)
    except Exception:
        files = []
    return jsonify(files)


@routes_bp.route('/search', methods=['POST'])
def route_search():
    """
    启动检索：
    - 参数：keyword, context_before, context_after, file
    """
    data = request.json or {}
    keyword = (data.get('keyword') or '').strip()
    if not keyword:
        return "Missing keyword", 400
    # 更稳健的数字解析，避免非法输入造成 500
    try:
        before = int(data.get('context_before', 0) or 0)
    except Exception:
        before = 0
    try:
        after = int(data.get('context_after', 0) or 0)
    except Exception:
        after = 0
    file = (data.get('file') or '').strip()
    scope = (data.get('scope') or '').strip()
    scope = (scope if scope in ('all', 'single') else None)
    reset_all = bool(data.get('reset_all') or False)
    final_all = bool(data.get('final_all') or False)

    status = start_search(keyword=keyword, context_before=before, context_after=after, file=file, scope_override=scope, reset_all=reset_all, final_all=final_all)

    if status == "Started":
        try:
            emit_message_utf('Started\n')
        except Exception:
            pass
        return "Started", 200
    elif status == "Busy":
        return "Busy", 200
    elif status == "rg not found":
        return "rg not found", 500
    else:
        return "Error", 500


@routes_bp.route('/search-count', methods=['POST'])
def route_search_count():
    """
    启动仅计数检索（非预览模式）：
    - 统计匹配次数并发送进度
    - 参数：keyword, file；可选 scope/reset_all/final_all
    """
    data = request.json or {}
    keyword = (data.get('keyword') or '').strip()
    if not keyword:
        return "Missing keyword", 400
    try:
        before = int(data.get('context_before', 0) or 0)
    except Exception:
        before = 0
    try:
        after = int(data.get('context_after', 0) or 0)
    except Exception:
        after = 0
    file = (data.get('file') or '').strip()
    scope = (data.get('scope') or '').strip()
    scope = (scope if scope in ('all', 'single') else None)
    reset_all = bool(data.get('reset_all') or False)
    final_all = bool(data.get('final_all') or False)

    status = start_search(keyword=keyword, context_before=before, context_after=after, file=file, scope_override=scope, reset_all=reset_all, final_all=final_all, count_only=True)

    if status == "Started":
        try:
            emit_message_utf('Started\n')
        except Exception:
            pass
        return "Started", 200
    elif status == "Busy":
        return "Busy", 200
    elif status == "rg not found":
        return "rg not found", 500
    else:
        return "Error", 500


@routes_bp.route('/cancel', methods=['POST'])
def route_cancel():
    """
    取消当前检索：委托 process_manager.cancel 完成
    """
    result, code = cancel_search()
    return jsonify(result), code


@routes_bp.route('/hot-reload', methods=['POST'])
def route_hot_reload():
    """
    触发热重载：清理当前搜索相关资源并尝试进程级重启
    """
    started = trigger_hot_reload_async()
    if started:
        return jsonify({"status": "restarting"}), 200
    else:
        return jsonify({"status": "restart_in_progress"}), 409


@routes_bp.route('/download')
def download():
    """
    按关键字与检索模式下载最新导出结果：
    - 参数：keyword（必填）、file（可选，用于区分单文件与全部文件：'__ALL__' 表示全部文件）
    - 可选：stamp（形如 MM-DD_ts），用于定位特定导出文件；不传则选择最新
    """
    keyword = request.args.get('keyword')
    if not keyword:
        return "Missing keyword", 400
    file_sel = (request.args.get('file') or '').strip()
    scope = 'all' if (file_sel == '__ALL__') else 'single'
    safe = sanitize_keyword(keyword)
    exports_dir = get_exports_dir()

    # 【直接使用内存记录】只认本次检索时创建的最新导出文件，去除目录扫描
    mem_fn = get_latest_export_filename(safe, scope=scope)
    if not mem_fn:
        abort(404)
    full_path = os.path.join(exports_dir, mem_fn)
    if not os.path.isfile(full_path):
        abort(404)

    # 如果提供了 stamp=MM-DD_ts，校验是否与当前内存记录一致
    stamp = (request.args.get('stamp') or '').strip()
    if stamp and len(stamp) >= 8:
        def _stamp_of(fn: str) -> str:
            try:
                prefix = f"{safe}__{scope}_"
                if not fn.startswith(prefix):
                    return ''
                rest = fn[len(prefix):]  # YYYY-MM-DD_<ts>.txt
                parts = rest.split('_')
                if len(parts) < 2:
                    return ''
                date = parts[0]
                ts_part = parts[1]
                ts = ts_part.split('.')[0]
                mmdd = date[5:] if len(date) >= 10 else ''
                return f"{mmdd}_{ts}" if mmdd and ts else ''
            except Exception:
                return ''
        if _stamp_of(mem_fn) != stamp:
            abort(404)

    resp = send_from_directory(exports_dir, mem_fn, as_attachment=True, conditional=True)
    # 在响应关闭后丢弃该导出文件的页面缓存，提升容器内存稳定性
    try:
        exports_abs = os.path.abspath(exports_dir)
        full_path = os.path.abspath(os.path.join(exports_dir, mem_fn))
        if full_path.startswith(exports_abs + os.sep):
            resp.call_on_close(lambda: drop_file_cache_path(full_path))
    except Exception:
        pass
    return resp


@routes_bp.route('/export-info')
def export_info():
    """
    返回某关键字与模式下最新导出文件的信息（用于构造下载链接）
    - 参数：keyword（必填）、file（可选，'__ALL__' 表示全部文件）
    返回：{"filename": str, "stamp": "MM-DD_ts", "download_url": str}
    """
    keyword = request.args.get('keyword')
    if not keyword:
        return "Missing keyword", 400
    file_sel = (request.args.get('file') or '').strip()
    scope = 'all' if (file_sel == '__ALL__') else 'single'
    safe = sanitize_keyword(keyword)
    exports_dir = get_exports_dir()

    # 【直接使用内存记录】只认本次检索时创建的最新导出文件，去除目录扫描
    mem_fn = get_latest_export_filename(safe, scope=scope)
    if not mem_fn or not os.path.isfile(os.path.join(exports_dir, mem_fn)):
        return jsonify({"exists": False}), 404

    # 提取 MM-DD_ts
    def _stamp_of(fn: str) -> str:
        try:
            prefix = f"{safe}__{scope}_"
            if not fn.startswith(prefix):
                return ''
            rest = fn[len(prefix):]
            parts = rest.split('_')
            if len(parts) < 2:
                return ''
            date = parts[0]
            ts_part = parts[1]
            ts = ts_part.split('.')[0]
            mmdd = date[5:] if len(date) >= 10 else ''
            return f"{mmdd}_{ts}" if mmdd and ts else ''
        except Exception:
            return ''
    stamp = _stamp_of(mem_fn)
    dl_url = f"/download?keyword={safe}&file={'__ALL__' if scope=='all' else ''}&stamp={stamp}" if stamp else f"/download?keyword={safe}&file={'__ALL__' if scope=='all' else ''}"
    return jsonify({
        "exists": True,
        "filename": mem_fn,
        "stamp": stamp,
        "download_url": dl_url
    })
