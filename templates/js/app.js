// === 连接与状态 ===
var socket = io({
    path: '/io',
    // 强制使用 WebSocket，避免某些环境下长轮询升级失败导致断连
    transports: ['websocket'],
    // 更稳健的重连与超时设置
    reconnection: true,
    reconnectionAttempts: 10,
    timeout: 20000
});

// 连接状态更新函数（含颜色映射与文案规范化）
function setConnectionStatus(text) {
    const el = document.getElementById('connectStatus');
    if (!el) return;
    // 统一文案：只保留“已连接 / 连接中 / 未连接”三种
    let t = (text || '').trim();
    if (t === '后台重载中...' || t === '连接异常') {
        t = '连接中';
    }
    el.textContent = t || '';
    // 颜色美化：按状态切换颜色类
    el.classList.remove('text-success', 'text-warning', 'text-info', 'text-muted');
    // 保持字号样式
    el.classList.add('small');
    if (t === '已连接') {
        el.classList.add('text-success');
    } else if (t === '连接中') {
        el.classList.add('text-warning');
    } else if (t === '未连接') {
        el.classList.add('text-muted');
    } else {
        // 默认颜色（未连接或未知）
        el.classList.add('text-muted');
    }
}

// 监听连接相关事件，更新状态文本
socket.on('connect', () => { setConnectionStatus('已连接'); try { fetchFileList(); } catch (e) {} });
socket.on('disconnect', () => setConnectionStatus('未连接'));
socket.on('connect_error', () => setConnectionStatus('连接中'));
socket.on('reconnect_attempt', () => setConnectionStatus('连接中'));
socket.on('reconnect', () => { setConnectionStatus('已连接'); try { fetchFileList(); } catch (e) {} });


// === 结果缓冲与渲染 ===
// 缓存常用 DOM 引用，减少重复查询
const resultEl = document.getElementById('result');
// 使用单一 Text 节点减少对已有大文本的复制，降低卡顿
let resultTextNode = null;
let MAX_RESULT_CHARS = 2000000; // 保留最近 ~2MB 文本，防止内存无限增长
let TRIM_AT_CHARS = 2500000;    // 超过阈值再裁剪，减少频繁复制
const FLUSH_CHUNK_SIZE = 65536;   // 以 64KB 分块追加，避免一次性大字符串阻塞

// 结果文本缓冲：批量 append，减少频繁的 DOM 触发
let resultBuffer = '';
let flushScheduled = false;

// === 预览设置（刷新页面或重启容器后生效） ===
const PREVIEW_KEY = 'preview_enabled';
// previewEnabled: 当前检索会话的实际预览状态；previewPref: 用户偏好，下一次提交时生效
let previewEnabled = true;
let previewPref = true;
try {
    const stored = localStorage.getItem(PREVIEW_KEY);
    previewPref = (stored === '0') ? false : true;
    // 页面首次加载时（尚未开始检索）使用偏好作为当前会话状态
    previewEnabled = previewPref;
} catch (e) {
    previewPref = true;
    previewEnabled = true;
}

function setupPreviewToggle() {
    const el = document.getElementById('previewToggle');
    if (!el) return;
    // 初始化为当前偏好状态
    try { el.checked = !!previewPref; } catch (e) {}
    // 切换时仅持久化偏好；不改变当前正在进行中的会话预览状态
    el.addEventListener('change', function(){
        previewPref = !!el.checked;
        try { localStorage.setItem(PREVIEW_KEY, previewPref ? '1' : '0'); } catch (e) {}
        // 注意：不即时应用到 previewEnabled；在下一次提交检索时应用
    });
}

// 将文本加入缓冲区
function enqueueResult(text) {
    // 始终以字符串形式累计
    const t = (typeof text === 'string') ? text : String(text || '');
    resultBuffer += t;
    scheduleFlush();
}
function scheduleFlush() {
    if (flushScheduled) return;
    flushScheduled = true;
    // 使用 rAF 在下一帧统一写入，提高渲染效率
    (window.requestAnimationFrame || setTimeout)(flushResultNow, 16);
}

// 立即将缓冲区内容写入结果区域（分块写入)
function flushResultNow() {
    try {
        if (!previewEnabled) {
            resultBuffer = '';
            flushScheduled = false;
            return;
        }
        if (resultBuffer && resultBuffer.length > 0) {
            if (resultTextNode && typeof resultTextNode.appendData === 'function') {
                // 分块写入，降低长字符串追加带来的阻塞
                let start = 0;
                const len = resultBuffer.length;
                while (start < len) {
                    const end = Math.min(start + FLUSH_CHUNK_SIZE, len);
                    resultTextNode.appendData(resultBuffer.slice(start, end));
                    start = end;
                }
                // 超过阈值时只保留最后 MAX_RESULT_CHARS 的内容
                if (resultTextNode.data && resultTextNode.data.length > TRIM_AT_CHARS) {
                    const d = resultTextNode.data;
                    resultTextNode.data = (MAX_RESULT_CHARS > 0) ? d.slice(-MAX_RESULT_CHARS) : '';
                }
            } else {
                resultEl.textContent = resultEl.textContent + resultBuffer;
                if (resultEl.textContent.length > TRIM_AT_CHARS) {
                    const d = resultEl.textContent;
                    resultEl.textContent = (MAX_RESULT_CHARS > 0) ? d.slice(-MAX_RESULT_CHARS) : '';
                }
            }
            resultBuffer = '';
        }
    } catch (e) {}
    flushScheduled = false;
}

// 在搜索完成或取消后触发一次主动压缩，避免长字符串残留占用内存
function compactResultIfLarge() {
    try {
        const res = document.getElementById('result');
        if (!res) return;
        // 先确保缓冲区已写入
        try { flushResultNow(); } catch (e) {}
        const current = (resultTextNode && resultTextNode.data) ? resultTextNode.data : (res.textContent || '');
        if (current && current.length > MAX_RESULT_CHARS) {
            const tail = (MAX_RESULT_CHARS > 0) ? current.slice(-MAX_RESULT_CHARS) : '';
            const t = document.createTextNode(tail);
            res.textContent = '';
            try { res.appendChild(t); } catch (e) {}
            resultTextNode = t;
        }
    } catch (e) {}
}

// 更激进的压缩：无论当前大小都重建 Text 节点，仅保留尾部 MAX_RESULT_CHARS
function aggressiveCompactResult() {
    try {
        const res = document.getElementById('result');
        if (!res) return;
        try { flushResultNow(); } catch (e) {}
        const current = (resultTextNode && resultTextNode.data) ? resultTextNode.data : (res.textContent || '');
        const tail = (MAX_RESULT_CHARS > 0) ? current.slice(-MAX_RESULT_CHARS) : '';
        const t = document.createTextNode(tail);
        res.textContent = '';
        try { res.appendChild(t); } catch (e) {}
        resultTextNode = t;
    } catch (e) {}
}

// 更彻底的前端内存回收：释放结果区所有文本与缓冲引用
function hardResetResults(tag) {
    try {
        // 先尝试刷新缓冲再清空，以免残留未写入的数据影响后续状态
        try { flushResultNow(); } catch (e) {}

        // 清空缓冲与写入计划
        try { resultBuffer = ''; } catch (e) {}
        try { flushScheduled = false; } catch (e) {}

        // 释放结果区域所有文本，断开旧 Text 节点引用，重新创建轻量节点用于后续增量写入
        const res = document.getElementById('result');
        if (res) {
            try { res.textContent = ''; } catch (e) {}
            try { resultTextNode = null; } catch (e) {}
            try {
                const t = document.createTextNode('');
                res.appendChild(t);
                resultTextNode = t;
            } catch (e) {}
        }

        // 重置与进度相关的轻量状态，避免后续 UI 抖动占用
        try { hasByteProgress = false; } catch (e) {}
        try { receivedChunks = 0; } catch (e) {}

        // 可选：记录一次内存状态，便于诊断
        try { reportMemory(tag || 'reset'); } catch (e) {}
    } catch (e) {}
}

// 轻量级内存报告：用于在导出或清空后观测是否回收
function reportMemory(tag) {
    try {
        // Chrome 专有 API：仅在支持时输出
        const p = (performance && performance.memory) ? performance.memory : null;
        if (p) {
            console.info(`[mem] ${tag}: used=${p.usedJSHeapSize}, total=${p.totalJSHeapSize}, limit=${p.jsHeapSizeLimit}`);
        }
    } catch (e) {}
}

// 根据检索模式动态调整尾部保留与裁剪阈值
function reconfigureRetentionForMode(isSearchAll) {
    // 若关闭预览，则不保留结果文本，最大限度降低内存
    if (!previewEnabled) {
        MAX_RESULT_CHARS = 0;
        TRIM_AT_CHARS = 0;
        return;
    }
    // 多文件模式下收敛到 1MB 尾部，降低累计文本占用；单文件保持 2MB
    MAX_RESULT_CHARS = isSearchAll ? 1000000 : 2000000;
    TRIM_AT_CHARS = Math.floor(MAX_RESULT_CHARS * 1.25);
}

// === 运行状态与分类 ===
let running = false;
let pendingSubmit = false;
let lastSubmitAt = 0;
const DEBOUNCE_MS = 600;
let receivedChunks = 0;
let matches = 0;
let files_total = 0;
let files_done = 0;
let progressHideTimeout = null;
let fileType = 'other';
let searchController = null;
const inflightControllers = new Set();
let wasCancelled = false;
let currentFileName = '';
// 标记是否收到过字节级进度，用于避免单文件的初始回退跳到高百分比
let hasByteProgress = false;
// 标记上一次检索是否为“全部文件”模式，用于导出逻辑选择
let lastSearchAll = false;
// 标记是否已点击“清空”按钮，用于导出按钮仅提示
let clearedAfterSearch = false;

// 根据文件扩展名粗略分类，用于调整进度条动画策略
function classifyFileType(nameLower) {
    const n = (nameLower || '').toLowerCase();
    if (/(\.zip|\.jar|\.war|\.rar|\.7z)$/i.test(n)) return 'archive';
    if (/(\.gz|\.bz2|\.xz|\.lz4|\.lzma)$/i.test(n)) return 'compressed';
    if (/\.xlsx$/i.test(n)) return 'excel';
    if (/\.xls$/i.test(n)) return 'excel';
    if (/\.csv$/i.test(n)) return 'csv';
    if (/(\.txt|\.log|\.json|\.xml|\.md|\.ini|\.yaml|\.yml)$/i.test(n)) return 'text';
    return 'other';
}

function showProgress() {
    document.getElementById('progressWrap').style.display = 'flex';
}
function hideProgress() {
    document.getElementById('progressWrap').style.display = 'none';
}

// 包装 fetch，自动跟踪并支持批量取消
function trackedFetch(url, options = {}, track = true) {
    const controller = new AbortController();
    const opts = Object.assign({}, options, { signal: controller.signal });
    if (track) inflightControllers.add(controller);
    return fetch(url, opts).finally(() => {
        if (track) inflightControllers.delete(controller);
    });
}

// 消所有被跟踪的请求
function cancelAllFetches() {
    inflightControllers.forEach(ctrl => {
        try { ctrl.abort(); } catch (e) {}
    });
    inflightControllers.clear();
}
// === 进度条与状态管理 ===
/**
 * 设置进度显示与状态
 * @param {'idle'|'running'|'done'|'cancelled'|'error'} state - 状态枚举
 * @param {{pct?:number,text?:string,file?:string,msg?:string,fileType?:string,preserveBarStyle?:boolean}} opts - 额外参数
 */
function setProgressState(state, opts = {}) {
    const bar = document.getElementById('progressBar');
    const badge = document.getElementById('progressBadge');
    const detail = document.getElementById('progressDetail');
    const percent = document.getElementById('progressPercent');
    const matchEl = document.getElementById('matchDisplay');
    const fileNameEl = document.getElementById('fileNameDisplay');

    if (state === 'idle') {
        bar.style.width = '0%';
        bar.className = 'progress-bar';
        bar.classList.remove('no-transition');
        badge.className = 'badge badge-secondary progress-status-badge';
        badge.textContent = '空闲';
        if (detail) detail.textContent = '';
        percent.textContent = '0%';
        fileNameEl.textContent = '';
        currentFileName = '';
        const sp = document.getElementById('loadingSpinner');
        if (sp) sp.style.display = 'none';
    } else if (state === 'running') {
        showProgress();
        let pct = typeof opts.pct === 'number' ? opts.pct : 0;
        pct = Math.max(0, Math.min(100, pct));
        // 在新一次运行开始时（pct 为 0）强制重置历史进度，避免继承旧值
        if (pct === 0) {
            bar.dataset.lastPct = '0';
            bar.style.width = '0%';
        }
        // 针对压缩/归档类型在运行阶段禁用过渡，减少抖动
        if (fileType === 'compressed' || fileType === 'archive') {
            bar.classList.add('no-transition');
        } else {
            bar.classList.remove('no-transition');
        }
        // 仅在最小变化达到 1% 时更新，降低重绘频率，同时保证进度单调递增
        const lastPct = parseFloat(bar.dataset.lastPct || '0');
        const targetPct = Math.max(lastPct, pct);
        if (Math.abs(targetPct - lastPct) >= 1) {
            bar.style.width = targetPct + '%';
            bar.dataset.lastPct = String(targetPct);
        }
        // 使用静态色块以降低绘制成本
        bar.className = 'progress-bar bg-info' + ((fileType === 'compressed' || fileType === 'archive') ? ' no-transition' : '');
        badge.className = 'badge badge-info progress-status-badge';
        badge.textContent = opts.text || '检索中';
        matchEl.textContent = `匹配：${matches} 条`;
        if (Math.abs(targetPct - lastPct) >= 1) {
            percent.textContent = targetPct + '%';
        }
        if (opts.file) {
            currentFileName = opts.file;
            fileNameEl.textContent = `正在检索：${opts.file}`;
        }
        const sp = document.getElementById('loadingSpinner');
        if (sp) sp.style.display = 'inline-block';
    } else if (state === 'done') {
        if (opts && (opts.fileType === 'compressed' || opts.fileType === 'archive')) {
            bar.classList.add('no-transition');
        } else {
            bar.classList.remove('no-transition');
        }
        bar.style.width = '100%';
        if (opts && opts.preserveBarStyle) {
            bar.classList.remove('bg-info');
            bar.classList.add('bg-success');
        } else {
            bar.className = 'progress-bar bg-success';
        }
        badge.className = 'badge badge-success progress-status-badge';
        badge.textContent = '完成';
        matchEl.textContent = `匹配：${matches} 条`;
        percent.textContent = '100%';
        const sp = document.getElementById('loadingSpinner');
        if (sp) sp.style.display = 'none';
    } else if (state === 'cancelled') {
        bar.style.width = '100%';
        bar.className = 'progress-bar bg-danger';
        badge.className = 'badge badge-danger progress-status-badge';
        badge.textContent = '已取消';
        matchEl.textContent = `匹配：${matches} 条`;
        percent.textContent = '已取消';
        fileNameEl.textContent = '';
        currentFileName = '';
        const sp = document.getElementById('loadingSpinner');
        if (sp) sp.style.display = 'none';
    } else if (state === 'error') {
        showProgress();
        bar.style.width = '100%';
        bar.className = 'progress-bar bg-warning';
        badge.className = 'badge badge-warning progress-status-badge';
        badge.textContent = '错误';
        if (detail) detail.textContent = opts.msg || '出现错误';
        percent.textContent = '错误';
        fileNameEl.textContent = '';
        currentFileName = '';
        const sp = document.getElementById('loadingSpinner');
        if (sp) sp.style.display = 'none';
    }
}

function disableControls(val) {
    const submitBtn = document.getElementById('submitBtn');
    if (submitBtn) submitBtn.disabled = val;
    const cancelBtn = document.getElementById('cancelBtn');
    if (cancelBtn) cancelBtn.disabled = false;
    const clearBtn = document.getElementById('clearBtn');
    if (clearBtn) clearBtn.disabled = false;
    const exportBtn = document.getElementById('exportBtn');
    if (exportBtn) exportBtn.disabled = false;
}

// === 文件列表加载/刷新 ===
function populateFileSelect(list) {
    const sel = document.getElementById('file');
    if (!sel) return;
    const prev = sel.value;
    // 重新构建选项，避免重复
    sel.innerHTML = '';
    const optAll = document.createElement('option');
    optAll.value = '__ALL__';
    optAll.textContent = '检索全部文件';
    sel.appendChild(optAll);
    if (Array.isArray(list) && list.length > 0) {
        list.forEach(fn => {
            const opt = document.createElement('option');
            opt.value = fn;
            opt.textContent = fn;
            sel.appendChild(opt);
        });
    }
    if (prev && Array.from(sel.options).some(o => o.value === prev)) {
        sel.value = prev;
    } else {
        sel.value = '__ALL__';
    }
}

function fetchFileList() {
    return trackedFetch('/files', {}, true)
        .then(r => r.ok ? r.json() : Promise.reject())
        .then(list => populateFileSelect(list))
        .catch(() => {});
}

// === 页面初始化 ===
window.addEventListener('DOMContentLoaded', function(){
    // 初始化结果区域为单一 Text 节点，降低后续写入的重排/复制成本
    try {
        resultEl.textContent = '';
        resultTextNode = document.createTextNode('');
        resultEl.appendChild(resultTextNode);
    } catch (e) {}

    // 初始化预览开关（设置在刷新后生效）
    try { setupPreviewToggle(); } catch (e) {}

    // 触发容器内进程热重载：每次页面打开或刷新时调用
    try {
        setConnectionStatus('连接中');
        trackedFetch('/hot-reload', { method: 'POST' }, true).catch(() => {});
    } catch (e) {}

    // 初次加载文件列表，同时在连接恢复时也会自动刷新
    fetchFileList();
    setProgressState('idle');
    const cancelBtnEl = document.getElementById('cancelBtn');
    cancelBtnEl.addEventListener('click', function(e){
        e.preventDefault();
        cancelSearch();
    });
});

// === Socket 消息处理 ===
socket.on('message', data => {
    if (!data || typeof data.message !== 'string') return;
    const text = data.message.replace(/\r?\n/g, '\n');
    const trimmed = text.trim();

    // 拦截后端推送的连接状态文案，改为在状态区显示
    if (trimmed === 'Connected') { setConnectionStatus('已连接'); return; }
    if (trimmed === 'Disconnected') { setConnectionStatus('未连接'); return; }

    if (text.includes('Cancelled')) {
        if (pendingSubmit) return;
        running = false;
        pendingSubmit = false;
        wasCancelled = true;
        hasByteProgress = false;
        setProgressState('cancelled');
        disableControls(false);
        document.getElementById('submitBtn').disabled = false;
        clearTimeout(progressHideTimeout);
        progressHideTimeout = setTimeout(() => hideProgress(), 1200);
        // 取消后执行硬重置，确保结果区内存彻底释放
        hardResetResults('cancelled');
        return;
    }

    if (text.includes('Started')) {
        if (!pendingSubmit) return;
        running = true;
        pendingSubmit = false;
        wasCancelled = false;
        receivedChunks = 0;
        hasByteProgress = false;
        matches = 0;
        files_total = 0;
        files_done = 0;
        document.getElementById('matchDisplay').textContent = `匹配：${matches} 条`;
        setProgressState('running', {pct:0, text: '开始检索'});
        disableControls(true);
        document.getElementById('cancelBtn').disabled = false;
        return;
    }

    if (text.includes('Busy')) {
        pendingSubmit = false;
        setProgressState('error', {msg: '服务器忙，请稍后再试'});
        disableControls(false);
        document.getElementById('submitBtn').disabled = false;
        clearTimeout(progressHideTimeout);
        progressHideTimeout = setTimeout(() => hideProgress(), 1200);
        return;
    }

    if (text.includes('Done')) {
        if (pendingSubmit) return;
        if (wasCancelled) return;
        running = false;
        pendingSubmit = false;
        hasByteProgress = false;
        setProgressState('done', {fileType, preserveBarStyle: (fileType === 'compressed')});
        disableControls(false);
        document.getElementById('submitBtn').disabled = false;
        clearTimeout(progressHideTimeout);
        progressHideTimeout = setTimeout(() => hideProgress(), 1200);
        // 若开启预览，保留已展示内容，仅进行压缩以降低占用；
        // 若关闭预览，则进行彻底重置以回收前端内存。
        if (previewEnabled) {
            try { aggressiveCompactResult(); } catch (e) {}
        } else {
            hardResetResults('done');
        }
        return;
    }

    if (/^\s*\?\?/.test(text)) return;

    if (text.trim().length > 0) {
        if (previewEnabled) enqueueResult(text);
        receivedChunks++;
    } else {
        if (previewEnabled) enqueueResult(text);
    }
});

let lastProgressAt = 0;
const PROGRESS_THROTTLE_MS = 180;

// === 进度事件处理 ===
socket.on('progress', data => {
    // 只在单文件检索时更新匹配数，多文件检索时由sendKeyword函数控制
    if (typeof data.matches === 'number' && document.getElementById('file').value !== '__ALL__') {
        matches = data.matches;
        document.getElementById('matchDisplay').textContent = `匹配：${matches} 条`;
    }
    if (typeof data.files_total === 'number') files_total = data.files_total;
    if (typeof data.files_done === 'number') files_done = data.files_done;
    if (data.file_type) fileType = data.file_type;
    if (data && data.phase === 'cancelled') {
        if (pendingSubmit) return;
        running = false;
        pendingSubmit = false;
        wasCancelled = true;
        setProgressState('cancelled');
        disableControls(false);
        clearTimeout(progressHideTimeout);
        progressHideTimeout = setTimeout(() => hideProgress(), 1200);
        return;
    }
    if (data && data.phase === 'error') {
        setProgressState('error', {msg: '出现错误'});
    }
    let pct = 0;
    if (typeof data.bytes_done === 'number') {
        hasByteProgress = true;
        const done = Math.max(0, data.bytes_done || 0);
        const total = Math.max(done, data.bytes_total || done);
        pct = total > 0 ? Math.floor((done / total) * 100) : 0;
    } else if (files_total && files_total > 0) {
        if (files_total === 1 && files_done === 0) {
            // 单文件尚未收到字节进度时，使用温和回退，最高不超过10%
            const fallbackGentle = Math.min(10, Math.floor(receivedChunks / 30));
            pct = Math.max(fallbackGentle, 0);
        } else {
            pct = Math.floor((files_done / files_total) * 100);
        }
    } else {
        // 未知总数时采用温和回退，避免过高跳跃
        pct = Math.min(60, Math.floor(receivedChunks / 30));
    }
    const reachedByteCompletion = (typeof data.bytes_done === 'number' && typeof data.bytes_total === 'number' && data.bytes_total > 0 && data.bytes_done >= data.bytes_total);
    const reachedFileCompletion = (files_total > 0 && files_done >= files_total);
    const reachedSearchEnd = (data && data.phase === 'search_end');
    // 在压缩/归档类型中，字节完成并不代表匹配统计完成，保持进度在95%，等待文件完成/搜索结束事件
    const bytesCompleteButNotFiles = reachedByteCompletion && !reachedFileCompletion && (fileType === 'compressed' || fileType === 'archive');
    if (running && (bytesCompleteButNotFiles || !(reachedByteCompletion || reachedFileCompletion))) {
        pct = Math.min(95, Math.max(0, pct));
    } else {
        pct = Math.min(100, Math.max(0, pct));
    }
    // 仅当文件完成或收到搜索结束事件时才标记完成，避免压缩文件过早结束
    if (reachedFileCompletion || reachedSearchEnd) {
        running = false;
        setProgressState('done', {fileType, preserveBarStyle: (fileType === 'compressed')});
        disableControls(false);
        document.getElementById('submitBtn').disabled = false;
        clearTimeout(progressHideTimeout);
        progressHideTimeout = setTimeout(() => hideProgress(), 1200);
        // 进度判断到达完成时也压缩一次结果，消除顺序差异的占用
        aggressiveCompactResult();
        return;
    }
    const now = Date.now();
    if (now - lastProgressAt >= PROGRESS_THROTTLE_MS) {
        lastProgressAt = now;
        const note = '检索中';
        setProgressState('running', {pct: pct, text: note});
    }
});


// === 检索（单文件/全部） ===
async function sendKeyword() {
    const kw = document.getElementById('keyword').value.trim();
    if (!kw) { alert('请输入关键词'); return; }

    const now = Date.now();
    if (running || pendingSubmit || (now - lastSubmitAt < DEBOUNCE_MS)) return;
    lastSubmitAt = now;
    pendingSubmit = true;
    wasCancelled = false;
    // 开始新检索时重置清空标记
    clearedAfterSearch = false;

    const before = parseInt(document.getElementById('context_before').value || '0', 10);
    const after  = parseInt(document.getElementById('context_after').value || '0', 10);
    const fileSel = document.getElementById('file');
    const file = fileSel.value;

    // 记录当前是否为“检索全部文件”模式
    lastSearchAll = (file === '__ALL__');
    // 在开始新检索时才应用预览偏好到当前会话
    previewEnabled = !!previewPref;
    // 按当前会话预览状态和模式调整保留参数
    reconfigureRetentionForMode(lastSearchAll);

    if (file === '__ALL__') {
        const list = await trackedFetch('/files').then(r => r.ok ? r.json() : Promise.reject()).catch(() => []);
        if (!list.length) { alert('目录中没有可检索文件'); pendingSubmit = false; return; }
        
        // 初始化累计匹配数
        let totalMatches = 0;
        matches = 0;
        document.getElementById('matchDisplay').textContent = `匹配：${matches} 条`;
        
        for (let i = 0; i < list.length; i++) {
            const f = list[i];
            if (wasCancelled) break;
            const isFirst = (i === 0);
            const isLast = (i === list.length - 1);
            const fileMatches = await singleSearch(kw, before, after, f, 'all', isFirst, isLast);
            if (typeof fileMatches === 'number') {
                totalMatches += fileMatches;
                matches = totalMatches;
                document.getElementById('matchDisplay').textContent = `匹配：${matches} 条`;
            }
            
            // 在进行下一个文件检索前添加1秒缓冲时间
            if (i < list.length - 1 && !wasCancelled) {
                await new Promise(resolve => setTimeout(resolve, 1000));
            }
        }
        pendingSubmit = false;
        return;
    }
    if (!file) { alert('请先选择文件'); pendingSubmit = false; return; }
    await singleSearch(kw, before, after, file);
}

/**
 * 执行单文件检索，并返回该文件匹配数
 * @param {string} kw - 关键词
 * @param {number} before - 上文行数
 * @param {number} after - 下文行数
 * @param {string} file - 文件路径/名称
 * @returns {Promise<number>} - 当前文件的匹配总数
 */
async function singleSearch(kw, before, after, file, scope, resetAll = false, finalAll = false) {
    fileType = classifyFileType(file.toLowerCase());
    receivedChunks = 0;
    let currentFileMatches = 0; // 当前文件的匹配数
    files_total = 0;
    files_done = 0;
    showProgress();
    setProgressState('running', {pct: 0, text: '检索中', file: file});
    document.getElementById('submitBtn').disabled = true;

    searchController = new AbortController();
    try {
        const payload = {keyword: kw, context_before: before, context_after: after, file};
        if (scope) payload.scope = scope;
        if (resetAll) payload.reset_all = true;
        if (finalAll) payload.final_all = true;
        const resp = await fetch('/search', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(payload),
            signal: searchController.signal
        });
        if (!resp.ok) throw new Error('search failed');
        
        // 等待搜索完成并获取匹配数
        await new Promise(resolve => {
            const onDone = () => { 
                socket.off('message', handler); 
                socket.off('progress', progressHandler);
                resolve(); 
            };
            const handler = (data) => {
                if (data && typeof data.message === 'string') {
                    const msg = data.message;
                    if (msg.includes('Done') || msg.includes('Cancelled') || msg.includes('Busy')) {
                        onDone();
                    }
                }
            };
            const progressHandler = (data) => {
                if (typeof data.matches === 'number') {
                    currentFileMatches = data.matches;
                }
            };
            socket.on('message', handler);
            socket.on('progress', progressHandler);
        });
        
        return currentFileMatches;
    } catch (e) {
        // 单条出错继续下一个
        return 0;
    }
}

// === 取消检索 ===
/**
 * 取消当前检索：终止请求、更新 UI 并通知后端
 */
function cancelSearch() {
    console.log('Canceling search...');
    
    // Abort any active fetch requests
    if (searchController) { 
        try { 
            searchController.abort(); 
        } catch(e){
            console.error('Error aborting search controller:', e);
        } 
        searchController = null; 
    }
    
    // Cancel all tracked fetch requests
    cancelAllFetches();
    
    // Update UI to show canceling state
    const badge = document.getElementById('progressBadge');
    if (badge) badge.textContent = '取消中...';
    const sp = document.getElementById('loadingSpinner');
    if (sp) sp.style.display = 'inline-block';
    
    // Send cancel request to backend
    fetch('/cancel', {method: 'POST'})
        .then(response => {
            console.log('Cancel request sent successfully');
        })
        .catch(error => {
            console.error('Error sending cancel request:', error);
        });
    
    // Update state
    running = false;
    pendingSubmit = false;
    wasCancelled = true;
    
    // Update UI
    setProgressState('cancelled');
    disableControls(false);
    const submitBtn = document.getElementById('submitBtn');
    if (submitBtn) submitBtn.disabled = false;
    
    // Hide progress after delay
    clearTimeout(progressHideTimeout);
    progressHideTimeout = setTimeout(() => { hideProgress(); }, 800);
}

// === 清空结果 ===
/**
 * 清空结果区域并重置缓冲与匹配数显示
 */
function clearResult() {
    try {
        // 重置缓冲与计划的刷新，避免残留数据影响下一次渲染
        try { resultBuffer = ''; } catch (e) {}
        try { flushScheduled = false; } catch (e) {}

        // 重新创建结果区域的 Text 节点，避免清空后旧节点被移除导致后续内容不可见
        const res = document.getElementById('result');
        if (res) {
            // 清空现有内容并附加一个新的 Text 节点供后续增量写入
            res.textContent = '';
            try {
                resultTextNode = document.createTextNode('');
                res.appendChild(resultTextNode);
            } catch (e) {
                // 兜底：如果创建 Text 节点失败，至少保证区域为空
                res.textContent = '';
            }
        }

        // 重置匹配数显示
        matches = 0;
        document.getElementById('matchDisplay').textContent = `匹配：${matches} 条`;
        // 记录已点击清空，用于导出按钮提示
        clearedAfterSearch = true;
    } catch (e) {
        console.warn('clearResult error:', e);
    }
}

// === 导出结果 ===
function exportResult() {
    const kwRaw = document.getElementById('keyword').value || '';
    const kw = kwRaw.trim().replace(/[^\w\u4e00-\u9fa5\-_ ]/g, '');
    const fileSel = document.getElementById('file');
    const fileVal = fileSel ? (fileSel.value || '') : '';

    // 全文件模式：下载后端按时间戳命名的最新文件 <keyword>__all_<YYYY-MM-DD>_<ts>.txt
    // 单文件模式：保持原路由参数以下载对应文件的最新导出
    const url = '/download?keyword=' + encodeURIComponent(kw || 'search') +
                '&file=' + encodeURIComponent(fileVal || '');

    const a = document.createElement('a');
    a.href = url;
    a.target = '_blank';
    a.rel = 'noopener';
    a.setAttribute('download', '');
    document.body.appendChild(a); a.click(); a.remove();

    // 可选提示：如果用户之前点击过“清空”，提示当前导出的是后台文件
    if (clearedAfterSearch) {
        try { console.info('提示：已清空，导出的是后端文件。'); } catch (e) {}
    }

    // 导出完成后主动进行一次压缩，以便释放前端大文本的内存占用
    try { compactResultIfLarge(); } catch (e) {}
    try { reportMemory('after_export'); } catch (e) {}
}

window.addEventListener('beforeunload', () => {
    try { cancelSearch(); } catch (e) {}
    try {
        if (navigator.sendBeacon) {
            const data = new Blob(['1'], { type: 'text/plain' });
            navigator.sendBeacon('/hot-reload', data);
        }
    } catch (e) {}
});
