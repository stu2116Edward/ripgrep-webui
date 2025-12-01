# -*- coding: utf-8 -*-
"""
路由模块
- 提供页面入口与静态资源映射
- 提供 /search /cancel /hot-reload /files /download 等后端接口
"""

import os
from flask import (
    Blueprint, render_template, request, jsonify,
    send_from_directory, abort, current_app
)

from config import DEFAULT_DATA_DIR
from utils import emit_message_utf
from search_engine import start_search
from process_manager import cancel as cancel_search
from process_manager import trigger_hot_reload_async
from export_manager import get_exports_dir

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
    """
    keyword = request.args.get('keyword')
    if not keyword:
        return "Missing keyword", 400
    file_sel = (request.args.get('file') or '').strip()
    scope = 'all' if (file_sel == '__ALL__') else 'single'
    safe = ''.join(c for c in keyword if c.isalnum() or c in (' ', '_', '-')).strip()
    if not safe:
        safe = 'search'
    exports_dir = get_exports_dir()
    # 'all' 模式文件名已改为时间戳格式：<safe>__all_<YYYY-MM-DD>_<ts>.txt

    candidates = []
    try:
        if os.path.isdir(exports_dir):
            # 兼容旧命名：带 scope 的文件名前缀 <safe>__<scope>_
            prefix = f"{safe}__{scope}_"
            for fn in os.listdir(exports_dir):
                if fn.startswith(prefix):
                    candidates.append(fn)
            # 若未找到，回退到仅关键字前缀
            if not candidates:
                for fn in os.listdir(exports_dir):
                    if fn.startswith(safe):
                        candidates.append(fn)
    except Exception:
        candidates = []
    if not candidates:
        abort(404)
    candidates.sort(key=lambda n: os.path.getmtime(os.path.join(exports_dir, n)), reverse=True)
    return send_from_directory(exports_dir, candidates[0], as_attachment=True)
