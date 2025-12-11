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
from utils import emit_message_utf, sanitize_keyword
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


@routes_bp.route('/exports/<path:filename>')
def serve_export_file(filename):
    """直接提供导出文件的下载（避免额外目录扫描与内存占用）。"""
    try:
        exports_dir = get_exports_dir()
        return send_from_directory(exports_dir, filename, as_attachment=True, conditional=True)
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
    # 'all' 模式文件名已改为时间戳格式：<safe>__all_<YYYY-MM-DD>_<ts>.txt

    # 单次扫描选择最新文件，避免构建/排序大列表导致内存增长
    if not os.path.isdir(exports_dir):
        abort(404)

    # 如果提供了 stamp=MM-DD_ts，尝试精确匹配该文件
    stamp = (request.args.get('stamp') or '').strip()
    def _stamp_of(fn: str) -> str:
        try:
            prefix = f"{safe}__{scope}_"
            if not fn.startswith(prefix):
                return ''
            rest = fn[len(prefix):]  # YYYY-MM-DD_<ts>.txt
            parts = rest.split('_')
            if len(parts) < 2:
                return ''
            date = parts[0]  # YYYY-MM-DD
            ts_part = parts[1]
            ts = ts_part.split('.')[0]
            mmdd = date[5:] if len(date) >= 10 else ''
            return f"{mmdd}_{ts}" if mmdd and ts else ''
        except Exception:
            return ''

    chosen = None
    prefix = f"{safe}__{scope}_"
    best_prefix_fn = None
    best_prefix_mtime = -1.0
    best_any_fn = None
    best_any_mtime = -1.0
    try:
        for de in os.scandir(exports_dir):
            try:
                if not de.is_file():
                    continue
                fn = de.name
                # 优先匹配带 scope 的前缀
                if fn.startswith(prefix):
                    # 若指定了 stamp 且命中，则直接选择
                    if not chosen and stamp and len(stamp) >= 8:
                        try:
                            if _stamp_of(fn) == stamp:
                                chosen = fn
                                break
                        except Exception:
                            pass
                    # 否则记录最新的 mtime
                    try:
                        mt = de.stat().st_mtime
                        if mt > best_prefix_mtime:
                            best_prefix_mtime = mt
                            best_prefix_fn = fn
                    except Exception:
                        pass
                # 记录仅关键字前缀的最新项（作为回退）
                elif fn.startswith(safe):
                    try:
                        mt = de.stat().st_mtime
                        if mt > best_any_mtime:
                            best_any_mtime = mt
                            best_any_fn = fn
                    except Exception:
                        pass
            except Exception:
                continue
    except Exception:
        pass

    if not chosen:
        chosen = best_prefix_fn or best_any_fn
    if not chosen:
        abort(404)
    return send_from_directory(exports_dir, chosen, as_attachment=True, conditional=True)


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

    # 单次扫描选择最新文件，避免构建/排序大列表导致内存增长
    if not os.path.isdir(exports_dir):
        return jsonify({"exists": False}), 404
    prefix = f"{safe}__{scope}_"
    latest = None
    latest_mtime = -1.0
    try:
        for de in os.scandir(exports_dir):
            try:
                if not de.is_file():
                    continue
                fn = de.name
                if not fn.startswith(prefix):
                    continue
                try:
                    mt = de.stat().st_mtime
                    if mt > latest_mtime:
                        latest_mtime = mt
                        latest = fn
                except Exception:
                    pass
            except Exception:
                continue
    except Exception:
        pass
    if not latest:
        return jsonify({"exists": False}), 404

    # 提取 MM-DD_ts
    def _stamp_of(fn: str) -> str:
        try:
            prefix = f"{safe}__{scope}_"
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
    stamp = _stamp_of(latest)
    dl_url = f"/download?keyword={safe}&file={'__ALL__' if scope=='all' else ''}&stamp={stamp}" if stamp else f"/download?keyword={safe}&file={'__ALL__' if scope=='all' else ''}"
    direct_url = f"/exports/{latest}"
    return jsonify({
        "exists": True,
        "filename": latest,
        "stamp": stamp,
        "download_url": dl_url,
        "direct_download_url": direct_url
    })
