# -*- coding: utf-8 -*-
"""
应用主入口
- 创建 Flask 与 Socket.IO 实例
- 注册路由 Blueprint
- 注入 app/socketio 到工具模块以供各子模块使用
- 提供 Socket.IO 连接/断开事件
"""

# gevent 打补丁：加速与兼容（必须尽早调用）
from gevent import monkey
monkey.patch_all()

import os
import signal
import sys
import logging
from flask import Flask
from flask_socketio import SocketIO

# 配置日志：在控制台输出 INFO 级别及以上日志，便于后台观察运行状态
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


from config import (
    CORS_ALLOWED_ORIGINS,
    SOCKETIO_PATH,
    SOCKETIO_ASYNC_MODE,
    SOCKETIO_PING_TIMEOUT,
    SOCKETIO_PING_INTERVAL,
)
from utils import init_app, emit_message_utf
from process import trigger_hot_reload_async

from routes import routes_bp

# 创建 Flask 应用（模板目录默认即为 'templates'）
app = Flask(__name__, template_folder='templates')
# # 启用 X-Sendfile：由前置服务器（若支持）直接发送文件，避免 Python 进程内存占用
# try:
#     app.config['USE_X_SENDFILE'] = True
# except Exception:
#     pass

# 创建 Socket.IO（与前端保持一致的 path 与心跳参数）
socketio = SocketIO(
    app,
    cors_allowed_origins=CORS_ALLOWED_ORIGINS,
    path=SOCKETIO_PATH,
    async_mode=SOCKETIO_ASYNC_MODE,
    ping_timeout=SOCKETIO_PING_TIMEOUT,
    ping_interval=SOCKETIO_PING_INTERVAL,
)

# 注册路由 Blueprint
app.register_blueprint(routes_bp)
logger.info('Routes registered')

# 注入 app/socketio 上下文到工具模块，供各子模块统一取用
init_app(app, socketio)
logger.info('App and SocketIO context injected')


@socketio.on('connect')
def _on_connect():
    logger.info('Socket.IO client connected')
    # 简短确认信息；使用 try/except 保持稳定性
    try:
        emit_message_utf('Connected\n')
    except Exception:
        pass


@socketio.on('disconnect')
def _on_disconnect():
    logger.info('Socket.IO client disconnected')
    # 断连时通知并尝试触发热重载（页面刷新时有用）
    try:
        emit_message_utf('Disconnected\n')
    except Exception:
        pass

    try:
        enable = os.environ.get('HOT_RELOAD_ON_DISCONNECT', '')
        if str(enable).strip().lower() in ('1', 'true', 'yes', 'y', 'on'):
            logger.info('Hot reload triggered on disconnect')
            trigger_hot_reload_async()
    except Exception:
        pass


def _graceful_shutdown(signum, frame):
    logger.info(f'Received signal {signum}, initiating graceful shutdown')
    # 在容器或本地运行时，优雅地触发一次热重载/退出（便于 docker-compose 重启）
    try:
        trigger_hot_reload_async()
    except Exception:
        pass
    try:
        sys.exit(0)
    except Exception:
        os._exit(0)


# 捕获常见终止信号，以便在容器/本地时触发重载/退出
try:
    signal.signal(signal.SIGTERM, _graceful_shutdown)
    signal.signal(signal.SIGINT, _graceful_shutdown)
    logger.info('Signal handlers registered for SIGTERM and SIGINT')
except Exception:
    pass


if __name__ == '__main__':
    logger.info('Starting Socket.IO server on 0.0.0.0:5000 (debug mode)')
    socketio.run(app, host='0.0.0.0', port=5000, debug=True, use_reloader=False)
