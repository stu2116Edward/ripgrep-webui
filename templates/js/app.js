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

// 连接状态更新函数
function setConnectionStatus(text) {
    const el = document.getElementById('connectStatus');
    if (el) el.textContent = text || '';
}

// 监听连接相关事件，更新状态文本
socket.on('connect', () => setConnectionStatus('已连接'));
socket.on('disconnect', () => setConnectionStatus('未连接'));
socket.on('connect_error', () => setConnectionStatus('连接异常'));
socket.on('reconnect_attempt', () => setConnectionStatus('重连中...'));
socket.on('reconnect', () => setConnectionStatus('已连接'));


// === 结果缓冲与渲染 ===
// 缓存常用 DOM 引用，减少重复查询
const resultEl = document.getElementById('result');
// 使用单一 Text 节点减少对已有大文本的复制，降低卡顿
let resultTextNode = null;
const MAX_RESULT_CHARS = 2000000; // 保留最近 ~2MB 文本，防止内存无限增长
const TRIM_AT_CHARS = 2500000;    // 超过阈值再裁剪，减少频繁复制
const FLUSH_CHUNK_SIZE = 65536;   // 以 64KB 分块追加，避免一次性大字符串阻塞

// 结果文本缓冲：批量 append，减少频繁的 DOM 触发
let resultBuffer = '';
let flushScheduled = false;

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
                    resultTextNode.data = resultTextNode.data.slice(-MAX_RESULT_CHARS);
                }
            } else {
                // 兜底：如果 Text 节点不可用，退回 textContent 方式
                resultEl.textContent = resultEl.textContent + resultBuffer;
                if (resultEl.textContent.length > TRIM_AT_CHARS) {
                    resultEl.textContent = resultEl.textContent.slice(-MAX_RESULT_CHARS);
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
            const tail = current.slice(-MAX_RESULT_CHARS);
            // 替换为新的 Text 节点，有助于释放旧字符串的内存占用
            const t = document.createTextNode(tail);
            res.textContent = '';
            try { res.appendChild(t); } catch (e) {}
            resultTextNode = t;
        }
    } catch (e) {}
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

// === 页面初始化 ===
window.addEventListener('DOMContentLoaded', function(){
    // 初始化结果区域为单一 Text 节点，降低后续写入的重排/复制成本
    try {
        resultEl.textContent = '';
        resultTextNode = document.createTextNode('');
        resultEl.appendChild(resultTextNode);
    } catch (e) {}

    // 触发容器内进程热重载：每次页面打开或刷新时调用
    try {
        setConnectionStatus('后台重载中...');
        trackedFetch('/hot-reload', { method: 'POST' }, true).catch(() => {});
    } catch (e) {}

    trackedFetch('/files', {}, true)
    .then(r => r.ok ? r.json() : Promise.reject())
    .then(list => {
        const sel = document.getElementById('file');
        if (Array.isArray(list) && list.length > 0) {
            list.forEach(fn => {
                const opt = document.createElement('option');
                opt.value = fn; opt.textContent = fn;
                sel.appendChild(opt);
            });
        }
    }).catch(() => {});
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
        // 取消后主动压缩一次结果，保证后续内存稳定
        compactResultIfLarge();
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
        // 完成后主动压缩一次结果，保证后续内存稳定
        compactResultIfLarge();
        return;
    }

    if (/^\s*\?\?/.test(text)) return;

    if (text.trim().length > 0) {
        enqueueResult(text);
        receivedChunks++;
    } else {
        enqueueResult(text);
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
    if (running && !(reachedByteCompletion || reachedFileCompletion)) {
        pct = Math.min(99, Math.max(0, pct));
    } else {
        pct = Math.min(100, Math.max(0, pct));
    }
    if (reachedFileCompletion || ((fileType === 'compressed' || fileType === 'archive') && reachedByteCompletion)) {
        running = false;
        setProgressState('done', {fileType, preserveBarStyle: (fileType === 'compressed')});
        disableControls(false);
        document.getElementById('submitBtn').disabled = false;
        clearTimeout(progressHideTimeout);
        progressHideTimeout = setTimeout(() => hideProgress(), 1200);
        // 进度判断到达完成时也压缩一次结果，消除顺序差异的占用
        compactResultIfLarge();
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

    const before = parseInt(document.getElementById('context_before').value || '0', 10);
    const after  = parseInt(document.getElementById('context_after').value || '0', 10);
    const fileSel = document.getElementById('file');
    const file = fileSel.value;

    if (file === '__ALL__') {
        const list = await trackedFetch('/files').then(r => r.ok ? r.json() : Promise.reject()).catch(() => []);
        if (!list.length) { alert('目录中没有可检索文件'); pendingSubmit = false; return; }
        
        // 初始化累计匹配数
        let totalMatches = 0;
        matches = 0;
        document.getElementById('matchDisplay').textContent = `匹配：${matches} 条`;
        
        for (const f of list) {
            if (wasCancelled) break;
            const fileMatches = await singleSearch(kw, before, after, f);
            if (typeof fileMatches === 'number') {
                totalMatches += fileMatches;
                matches = totalMatches;
                document.getElementById('matchDisplay').textContent = `匹配：${matches} 条`;
            }
            
            // 在进行下一个文件检索前添加1秒缓冲时间
            if (list.indexOf(f) < list.length - 1 && !wasCancelled) {
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
async function singleSearch(kw, before, after, file) {
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
        const resp = await fetch('/search', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({keyword: kw, context_before: before, context_after: after, file}),
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
                if (data.message && (data.message.includes('Done') || data.message.includes('Cancelled'))) onDone();
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
    } catch (e) {
        console.warn('clearResult error:', e);
    }
}

// === 导出结果 ===
/**
 * 将结果导出为本地 .txt 文件（过滤取消提示行）
 */
function exportResult() {
    const content = document.getElementById('result').textContent
                      .split('\n')
                      .filter(line => !line.includes('Cancelled'))
                      .join('\n');
    if (!content.trim()) { alert('检索结果为空，无需导出！'); return; }
    const kw = document.getElementById('keyword').value.trim().replace(/[^\w\u4e00-\u9fa5\-_ ]/g, '');
    const fn = `${kw || 'search'}_${new Date().toISOString().slice(0,10)}_${Date.now()}.txt`;
    const blob = new Blob([content], {type: 'text/plain'});
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = fn;
    document.body.appendChild(a); a.click(); a.remove();
    setTimeout(() => URL.revokeObjectURL(a.href), 1000);
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