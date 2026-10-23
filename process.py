# -*- coding: utf-8 -*-
"""
进程管理模块
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
import gc

from utils import emit_message_utf, emit_progress_ex, trim_process_memory, aggressive_memory_reclaim
from export import close_all_export_streams, has_active_export_streams
from config import (
    IDLE_RECLAIM_ENABLED,
    IDLE_LIGHT_TRIM_INTERVAL_MS,
    IDLE_AGGRESSIVE_TRIM_INTERVAL_MS,
)

# 全局状态
proc = None  # 当前主 rg 进程或代表主流程的进程
extra_procs = []  # 附加进程
temp_dirs = []  # 临时目录
cancel_requested = False  # 取消标志
_proc_label_map = {}  # pid -> label 映射（当系统 rg 不支持 --label 时使用）

# 热重载控制：避免并发重复触发
_restart_lock = threading.Lock()
_restart_in_progress = False

# 检索启动串行化锁：防止并发进入 start_search
_search_lock = threading.Lock()

# 空闲内存回收线程控制
_idle_thread = None
_idle_thread_lock = threading.Lock()


def _idle_reclaimer_loop():
    """后台空闲内存回收线程：在完全空闲时周期性执行轻量与增强回收。
    条件：无主流程（proc 为 None）、无附加进程、无活跃导出写入线程、未处于取消流程。
    """
    last_light_ns = 0
    last_aggressive_ns = 0
    light_ns = max(0, int(IDLE_LIGHT_TRIM_INTERVAL_MS)) * 1_000_000
    aggressive_ns = max(0, int(IDLE_AGGRESSIVE_TRIM_INTERVAL_MS)) * 1_000_000
    while True:
        try:
            if not IDLE_RECLAIM_ENABLED:
                # 若禁用则短眠，避免忙等
                time.sleep(1.0)
                continue
            # 仅在完全空闲时尝试回收
            idle = (proc is None) and (not extra_procs) and (not cancel_requested)
            try:
                if idle:
                    # 若存在活跃的导出写入线程（含关闭中），视为非空闲
                    if has_active_export_streams():
                        idle = False
                else:
                    # 非空闲状态：重置计时器，避免立即触发回收
                    last_light_ns = time.perf_counter_ns()
                    last_aggressive_ns = last_light_ns
            except Exception:
                # 保守处理：出现异常时不做强回收
                idle = False

            now_ns = time.perf_counter_ns()
            if idle:
                # 轻量修剪（gc + 工作集压缩/trim），较为安全可频繁执行
                if light_ns > 0 and (last_light_ns == 0 or (now_ns - last_light_ns) >= light_ns):
                    try:
                        gc.collect()
                    except Exception:
                        pass
                    try:
                        trim_process_memory()
                    except Exception:
                        pass
                    last_light_ns = now_ns
                # 增强回收（可能尝试 drop_caches），间隔更长以避免影响系统缓存
                if aggressive_ns > 0 and (last_aggressive_ns == 0 or (now_ns - last_aggressive_ns) >= aggressive_ns):
                    try:
                        aggressive_memory_reclaim()
                    except Exception:
                        pass
                    last_aggressive_ns = now_ns
                # 空闲时短眠，避免忙等
                time.sleep(0.5)
            else:
                # 非空闲时更长时间休眠，降低负载
                time.sleep(1.0)
        except Exception:
            # 任何异常下继续循环，保持线程存活
            try:
                time.sleep(1.0)
            except Exception:
                pass


def _ensure_idle_reclaimer_started():
    """确保空闲回收线程已启动（一次性）。"""
    global _idle_thread
    try:
        with _idle_thread_lock:
            if _idle_thread is None:
                t = threading.Thread(target=_idle_reclaimer_loop, daemon=True)
                t.start()
                _idle_thread = t
    except Exception:
        pass

# 模块导入时启动空闲回收线程
_ensure_idle_reclaimer_started()


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


def terminate_proc(p, kill_group=True):
    """
    公开的进程终止包装：供其他模块调用，避免直接依赖私有函数。
    """
    try:
        _terminate_proc(p, kill_group=kill_group)
    except Exception:
        pass


def cleanup_temp_dirs():
    """
    统一清理并移除已登记的临时目录，供取消、热重载与检索收尾使用。
    """
    global temp_dirs
    try:
        for d in list(temp_dirs):
            try:
                shutil.rmtree(d)
            except Exception:
                pass
        temp_dirs = []
    except Exception:
        pass


def schedule_temp_dir_cleanup_for_proc(proc, temp_dir):
    """
    为进程退出后安排异步删除临时目录的任务。
    - 等待关联进程退出后删除目录并从登记列表移除。
    - 失败时在可确定进程已结束的情况下立即删除。
    """
    def _cleanup(p=proc, d=temp_dir):
        try:
            if hasattr(p, 'wait'):
                p.wait()
        except Exception:
            pass
        try:
            shutil.rmtree(d)
        except Exception:
            pass
        try:
            if d in temp_dirs:
                temp_dirs.remove(d)
        except Exception:
            pass
        try:
            gc.collect()
        except Exception:
            pass
    try:
        t = threading.Thread(target=_cleanup, daemon=True)
        t.start()
    except Exception:
        try:
            if proc and getattr(proc, 'poll', lambda: None)() is not None:
                try:
                    shutil.rmtree(temp_dir)
                except Exception:
                    pass
                try:
                    if temp_dir in temp_dirs:
                        temp_dirs.remove(temp_dir)
                except Exception:
                    pass
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

    # 清理临时目录（统一调用）
    try:
        cleanup_temp_dirs()
    except Exception:
        pass

    # 由搜索线程的 finally 统一负责关闭导出流与释放 Busy，避免尾部写入在取消时被抢占
    # 此处不主动关闭导出流，也不提前将 proc 置空，防止新检索在旧检索清理未完成时进入
    try:
        pass
    except Exception:
        pass

    # 保持 proc 非空以指示 Busy；实际置空由搜索线程完成
    _proc_label_map = {}
    # 在返回前主动进行垃圾回收与进程工作集修剪，尽量立刻归还内存
    try:
        gc.collect()
    except Exception:
        pass
    try:
        trim_process_memory()
    except Exception:
        pass
    try:
        elapsed_ms_total = int((time.perf_counter_ns() - start_ns) / 1_000_000)
    except Exception:
        elapsed_ms_total = 0
    emit_progress_ex(phase='cancelled', elapsed_ms=elapsed_ms_total)

    return {"status": "cancelled"}, 200


def trigger_hot_reload_async():
    """
    触发内部初始化（清理状态但不重启进程/容器）。
    返回 True 表示已执行清理，False 表示当前有活动搜索未执行。
    """
    global proc, extra_procs, temp_dirs, _proc_label_map, cancel_requested, _restart_in_progress

    # 防止并发触发
    with _restart_lock:
        # 若已有重载进行中，拒绝触发；若当前存在活动搜索，先取消并等待清理
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

    # 清理临时目录（统一调用）
    try:
        cleanup_temp_dirs()
    except Exception:
        pass

    # 关闭导出流
    try:
        close_all_export_streams()
    except Exception:
        pass

    proc = None
    _proc_label_map = {}

    # 重置标志，允许后续新搜索
    _restart_in_progress = False
    cancel_requested = False

    return True
