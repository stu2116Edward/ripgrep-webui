# -*- coding: utf-8 -*-
"""
进程管理逻辑模块
- 管理 rg 主进程、额外子进程、临时目录与取消标志
- 提供取消与热重载的统一实现
"""

import os
import sys
import time
import shutil
import signal
import threading
import subprocess

from utils import emit_message_utf, emit_progress_ex
from export_manager import close_all_export_streams

# 全局状态
proc = None  # 当前主 rg 进程或代表主流程的进程
extra_procs = []  # 附加进程
temp_dirs = []  # 临时目录
cancel_requested = False  # 取消标志
_proc_label_map = {}  # pid -> label 映射（当系统 rg 不支持 --label 时使用）

# 热重载控制：避免并发重复触发
_restart_lock = threading.Lock()
_restart_in_progress = False


def _close_streams(p):
    """尝试关闭进程可能打开的流。"""
    try:
        if not p:
            return
        for attr in ('stdin', 'stdout', 'stderr'):
            try:
                s = getattr(p, attr, None)
                if s:
                    try:
                        s.close()
                    except Exception:
                        pass
            except Exception:
                pass
    except Exception:
        pass


def _terminate_proc(p, kill_group=True):
    """
    强制终止进程（跨平台，优先关闭管道，其次温和终止，最后强杀）。
    kill_group: 尝试终止进程组（Unix），以便杀掉子进程。
    """
    if not p:
        return
    try:
        _close_streams(p)
    except Exception:
        pass

    try:
        # 如果进程已经结束则直接返回
        if hasattr(p, 'poll') and p.poll() is not None:
            return

        if os.name != 'nt':
            # 先尝试发送 SIGTERM 给进程组
            try:
                if kill_group and hasattr(os, 'getpgid'):
                    os.killpg(os.getpgid(p.pid), signal.SIGTERM)
                else:
                    p.terminate()
            except Exception:
                try:
                    p.terminate()
                except Exception:
                    pass
        else:
            try:
                p.terminate()
            except Exception:
                pass

        # 等待短时间以便优雅退出
        try:
            p.wait(timeout=0.5)
        except Exception:
            pass

        # 最后强杀
        if hasattr(p, 'poll') and p.poll() is None:
            if os.name == 'nt':
                try:
                    subprocess.run(['taskkill', '/PID', str(p.pid), '/T', '/F'],
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=False)
                except Exception:
                    pass
            else:
                try:
                    if kill_group and hasattr(os, 'getpgid'):
                        os.killpg(os.getpgid(p.pid), signal.SIGKILL)
                    else:
                        p.kill()
                except Exception:
                    pass
    except Exception:
        pass
    finally:
        try:
            _close_streams(p)
        except Exception:
            pass


def cancel():
    """
    取消当前搜索，终止所有子进程并清理资源。返回 (result_dict, http_code) 以供路由直接返回。
    """
    global proc, extra_procs, temp_dirs, _proc_label_map, cancel_requested

    start_ns = time.perf_counter_ns()
    cancel_requested = True

    emit_message_utf('Cancelled\n')
    emit_progress_ex(phase='cancelled')

    # 终止额外进程
    for p in list(extra_procs):
        try:
            _terminate_proc(p)
        except Exception:
            pass
    extra_procs = []

    # 终止主进程
    try:
        _terminate_proc(proc)
    except Exception:
        pass

    # 清理临时目录
    for d in list(temp_dirs):
        try:
            shutil.rmtree(d)
        except Exception:
            pass
    temp_dirs = []

    # 关闭并清空导出流
    try:
        close_all_export_streams()
    except Exception:
        pass

    proc = None
    _proc_label_map = {}
    try:
        elapsed_ms_total = int((time.perf_counter_ns() - start_ns) / 1_000_000)
    except Exception:
        elapsed_ms_total = 0
    emit_progress_ex(phase='cancelled', elapsed_ms=elapsed_ms_total)

    return {"status": "cancelled"}, 200


def trigger_hot_reload_async():
    """
    触发热重载流程（异步）。返回 True 表示已启动，False 表示已有重载在进行中。
    """
    global proc, extra_procs, temp_dirs, _proc_label_map, cancel_requested, _restart_in_progress

    # 防止并发触发
    with _restart_lock:
        if _restart_in_progress:
            return False
        _restart_in_progress = True

    cancel_requested = True

    # 终止所有附加进程
    for p in list(extra_procs):
        try:
            _terminate_proc(p)
        except Exception:
            pass
    extra_procs = []

    # 终止主进程
    try:
        _terminate_proc(proc)
    except Exception:
        pass

    # 清理临时目录
    for d in list(temp_dirs):
        try:
            shutil.rmtree(d)
        except Exception:
            pass
    temp_dirs = []

    # 关闭导出流
    try:
        close_all_export_streams()
    except Exception:
        pass

    proc = None
    _proc_label_map = {}

    def _do_restart():
        # 短暂等待以让触发方继续返回
        try:
            time.sleep(0.2)
        except Exception:
            pass
        try:
            # 在容器环境内优先通过父进程/1号进程终止触发重启
            in_docker = False
            try:
                in_docker = os.path.exists('/.dockerenv') or bool(os.environ.get('IS_DOCKER') or os.environ.get('RUNNING_IN_DOCKER'))
            except Exception:
                in_docker = False

            if in_docker:
                try:
                    try:
                        os.kill(os.getppid(), signal.SIGTERM)
                    except Exception:
                        pass
                    try:
                        os.kill(1, signal.SIGTERM)
                    except Exception:
                        pass
                except Exception:
                    pass
                os._exit(0)

            # Gunicorn 环境：通知 master 重载并退出 worker
            if os.environ.get('GUNICORN_CMD_ARGS'):
                try:
                    os.kill(os.getppid(), signal.SIGHUP)
                except Exception:
                    pass
                os._exit(0)

            # 本地运行：exec 自身进行自重启（进程替换）
            try:
                os.execv(sys.executable, [sys.executable] + sys.argv)
            except Exception:
                os._exit(0)
        finally:
            # 如果任何路径失败，确保退出
            try:
                os._exit(0)
            except Exception:
                pass

    t = threading.Thread(target=_do_restart, daemon=True)
    t.start()
    return True