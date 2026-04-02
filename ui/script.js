const API = '/api';
let state = { chats: {}, currentChatId: null, messages: [], docs: [], isThinking: false };

/* ── HTTP ── */
async function apiFetch(method, path, body, isFormData = false) {
  const opts = { method, headers: {} };
  if (body) {
    if (isFormData) { opts.body = body; }
    else { opts.headers['Content-Type'] = 'application/json'; opts.body = JSON.stringify(body); }
  }
  const res = await fetch(API + path, opts);
  if (!res.ok) {
    let msg = `HTTP ${res.status}`;
    try { const j = await res.json(); msg = j.error || msg; } catch (_) { }
    throw new Error(msg);
  }
  const ct = res.headers.get('content-type') || '';
  return ct.includes('application/json') ? res.json() : res;
}

/* ── Toast ── */
function toast(msg, type = 'info') {
  const el = document.createElement('div');
  el.className = 'toast' + (type === 'error' ? ' error' : type === 'success' ? ' success' : '');
  el.textContent = msg;
  document.getElementById('toast-container').appendChild(el);
  setTimeout(() => el.remove(), 3200);
}

/* ── Utils ── */
function escHtml(s) {
  return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}
function renderMarkdown(text) {
  if (!text) return '';
  let h = text
    .replace(/```(\w*)\n([\s\S]*?)```/g, (_, l, c) => `<pre><code class="lang-${l}">${escHtml(c.trim())}</code></pre>`)
    .replace(/`([^`]+)`/g, (_, c) => `<code>${escHtml(c)}</code>`)
    .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
    .replace(/^### (.+)$/gm, '<h3>$1</h3>')
    .replace(/^## (.+)$/gm, '<h2>$1</h2>')
    .replace(/^# (.+)$/gm, '<h1>$1</h1>')
    .replace(/\n\n+/g, '</p><p>').replace(/\n/g, '<br>');
  return `<p>${h}</p>`;
}
function autoResize(ta) { ta.style.height = 'auto'; ta.style.height = Math.min(ta.scrollHeight, 180) + 'px'; }
function scrollToBottom() { const a = document.getElementById('chat-area'); a.scrollTop = a.scrollHeight; }
function updateSendButton() {
  const i = document.getElementById('msg-input'), s = document.getElementById('send-btn');
  s.disabled = !(i.value.trim() && state.currentChatId && !state.isThinking);
}

/* ── Views ── */
function setView(mode) {
  const empty = document.getElementById('empty-state'), chat = document.getElementById('chat-area');
  if (mode === 'chat') { empty.style.display = 'none'; chat.style.display = 'block'; }
  else { empty.style.display = 'flex'; chat.style.display = 'none'; }
  updateSendButton();
}
function updateTopbar() {
  const n = document.getElementById('topbar-name'), i = document.getElementById('topbar-id');
  if (!state.currentChatId) { n.textContent = 'Smart Agent'; i.textContent = 'no conversation selected'; return; }
  const m = state.chats[state.currentChatId];
  n.textContent = m?.name || 'Untitled'; i.textContent = '#' + state.currentChatId.slice(0, 8);
}

/* ── Messages ── */
function appendMessage(role, content) {
  const c = document.getElementById('messages'), row = document.createElement('div');
  row.className = `msg-row ${role}`;
  if (role === 'assistant') {
    row.innerHTML = `<img src="/assets/icon.png" class="msg-avatar" alt="S"><div class="msg-content">${renderMarkdown(content)}</div>`;
  } else {
    row.innerHTML = `<div class="msg-content">${escHtml(content)}</div>`;
  }
  c.appendChild(row); scrollToBottom();
}
function showThinking() {
  const row = document.createElement('div'); row.id = 'thinking-indicator'; row.className = 'msg-row assistant thinking-row';
  row.innerHTML = `<img src="/assets/icon.png" class="msg-avatar" alt="S"><div class="msg-content"><div class="thinking-dots"><span></span><span></span><span></span></div></div>`;
  document.getElementById('messages').appendChild(row); scrollToBottom();
}
function hideThinking() { const e = document.getElementById('thinking-indicator'); if (e) e.remove(); }
function renderMessages() {
  document.getElementById('messages').innerHTML = '';
  state.messages.forEach(m => appendMessage(m.role, m.content)); scrollToBottom();
}

/* ── Conv list ── */
function renderConvList() {
  const list = document.getElementById('conv-list'); list.innerHTML = '';
  const ids = Object.keys(state.chats);
  if (!ids.length) { list.innerHTML = '<div style="padding:10px 16px;color:#555;font-size:12px">No conversations</div>'; return; }
  ids.forEach(cid => {
    const meta = state.chats[cid], isActive = cid === state.currentChatId;
    const item = document.createElement('div');
    item.className = 'conv-item' + (isActive ? ' active' : '');

    const countBadge = meta.message_count ? `<span class="conv-count">${meta.message_count}</span>` : '';
    const previewText = meta.preview ? `<div class="conv-preview">${escHtml(meta.preview)}</div>` : '';

    item.innerHTML = `
      <div class="conv-active-dot"></div>
      <div class="conv-main">
        <div class="conv-name-row">
          <span class="conv-name">${escHtml(meta.name)}</span>
          ${countBadge}
        </div>
        ${previewText}
      </div>
      <div class="conv-actions">
        <button class="icon-btn rename-btn" title="Rename">
          <svg viewBox="0 0 14 14" fill="none"><path d="M9.5 2.5l2 2-7 7H2.5v-2l7-7z" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"/></svg>
        </button>
        <button class="icon-btn danger delete-btn" title="Delete">
          <svg viewBox="0 0 14 14" fill="none"><path d="M2 4h10M5 4V2.5h4V4M5.5 6.5v4M8.5 6.5v4M3 4l.8 7.5h6.4L11 4" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"/></svg>
        </button>
      </div>`;
    item.querySelector('.conv-main').addEventListener('click', () => selectChat(cid));
    item.querySelector('.conv-active-dot').addEventListener('click', () => selectChat(cid));
    item.querySelector('.rename-btn').addEventListener('click', e => { e.stopPropagation(); startRename(item, cid, meta.name); });
    item.querySelector('.delete-btn').addEventListener('click', e => { e.stopPropagation(); deleteChat(cid); });
    list.appendChild(item);
  });
}

/* ── Rename ── */
function startRename(item, cid, currentName) {
  const nameDiv = item.querySelector('.conv-name'), actions = item.querySelector('.conv-actions');
  const input = document.createElement('input'); input.className = 'conv-rename-input'; input.value = currentName;
  nameDiv.replaceWith(input); actions.style.display = 'none'; input.focus(); input.select();
  async function commit() {
    const newName = input.value.trim(); actions.style.display = '';
    if (!newName || newName === currentName) { input.replaceWith(nameDiv); return; }
    try {
      await apiFetch('PATCH', `/chats/${cid}`, { name: newName });
      state.chats[cid].name = newName;
      if (cid === state.currentChatId) updateTopbar();
      renderConvList();
    } catch (e) { toast('Rename failed: ' + e.message, 'error'); input.replaceWith(nameDiv); }
  }
  input.addEventListener('keydown', e => { if (e.key === 'Enter') { e.preventDefault(); commit(); } if (e.key === 'Escape') { actions.style.display = ''; input.replaceWith(nameDiv); } });
  input.addEventListener('blur', commit);
}

/* ── Chat CRUD ── */
async function loadChats() {
  try {
    const data = await apiFetch('GET', '/chats');
    state.chats = {};
    if (Array.isArray(data)) data.forEach(c => { state.chats[c.id] = c; });
    renderConvList();
  } catch (e) { toast('Failed to load chats: ' + e.message, 'error'); }
}
async function selectChat(cid) {
  if (cid === state.currentChatId) return;
  try {
    const data = await apiFetch('GET', `/chats/${cid}`);
    state.currentChatId = cid; state.messages = data.messages || [];
    renderConvList(); updateTopbar();
    if (state.messages.length === 0) setView('empty');
    else { setView('chat'); renderMessages(); }
    document.getElementById('msg-input').focus();
  } catch (e) { toast('Failed to load chat: ' + e.message, 'error'); }
}
async function createChat() {
  try {
    const res = await apiFetch('POST', '/chats', { name: 'New Conversation' });
    if (!res.id) return;
    state.chats[res.id] = res; state.currentChatId = res.id; state.messages = [];
    renderConvList(); updateTopbar(); setView('empty');
    document.getElementById('msg-input').focus();
  } catch (e) { toast('Failed to create chat: ' + e.message, 'error'); }
}
async function deleteChat(cid) {
  try {
    await apiFetch('DELETE', `/chats/${cid}`);
    delete state.chats[cid];
    if (state.currentChatId === cid) { state.currentChatId = null; state.messages = []; updateTopbar(); setView('empty'); }
    renderConvList(); toast('Conversation deleted');
  } catch (e) { toast('Delete failed: ' + e.message, 'error'); }
}

/* ── Send message ── */
async function sendMessage(text) {
  if (!text || !state.currentChatId || state.isThinking) return;
  state.isThinking = true; updateSendButton();
  if (state.messages.length === 0) setView('chat');
  appendMessage('user', text); state.messages.push({ role: 'user', content: text }); showThinking();
  const input = document.getElementById('msg-input'); input.disabled = true;
  try {
    const res = await apiFetch('POST', `/chats/${state.currentChatId}/messages`, { message: text });
    hideThinking();
    const reply = res.reply || 'No response';
    appendMessage('assistant', reply);
    state.messages = res.messages || [...state.messages, { role: 'assistant', content: reply }];
    await loadChats();
  } catch (e) {
    hideThinking(); appendMessage('assistant', 'An error occurred. Please try again.');
    toast('Message failed: ' + e.message, 'error');
  } finally {
    state.isThinking = false; input.disabled = false; input.focus(); updateSendButton();
  }
}

/* ── Documents ── */
async function loadDocs() {
  try {
    const data = await apiFetch('GET', '/documents');
    state.docs = Array.isArray(data) ? data : [];
    renderDocList();
    if (state.currentChatId && state.messages.length > 0) {
      document.getElementById('table-area').style.display = state.docs.length ? 'block' : 'none';
    }
    const needsPoll = state.docs.some(d => d.status === 'pending' || d.status === 'processing');
    if (needsPoll) {
      if (window._docPollTimeout) clearTimeout(window._docPollTimeout);
      window._docPollTimeout = setTimeout(loadDocs, 3000);
    }
  } catch (e) { console.error('Failed to load documents:', e.message); }
}
function renderDocList() {
  const listTop = document.getElementById('doc-list'), tableBody = document.getElementById('doc-table-body');
  listTop.innerHTML = ''; tableBody.innerHTML = '';

  if (!state.docs.length) return;

  state.docs.forEach(doc => {
    const ext = doc.filename.split('.').pop().toLowerCase();
    const typeLabel = ext === 'pdf' ? 'PDF' : ext === 'md' ? 'MD' : 'TXT';
    const dateStr = new Date().toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });

    // Sidebar list item (Existing)
    const item = document.createElement('div'); item.className = 'doc-item';
    item.innerHTML = `
      <span class="doc-icon">${typeLabel}</span>
      <span class="doc-name" title="${escHtml(doc.filename)}">${escHtml(doc.filename)}</span>
      <span class="doc-status ${doc.status}">${doc.status}</span>
      <button class="icon-btn danger doc-del" title="Delete">
        <svg viewBox="0 0 14 14" fill="none"><path d="M2 4h10M5 4V2.5h4V4M5.5 6.5v4M8.5 6.5v4M3 4l.8 7.5h6.4L11 4" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"/></svg>
      </button>`;
    item.querySelector('.doc-del').addEventListener('click', e => { e.stopPropagation(); deleteDocument(doc.id, doc.filename); });
    listTop.appendChild(item);

    // Table Row (New)
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>
        <div class="td-doc">
          <div class="td-icon">${typeLabel}</div>
          <div class="td-name">${escHtml(doc.filename)}</div>
        </div>
      </td>
      <td>${dateStr}</td>
      <td><span class="badge ${ext}">${typeLabel}</span></td>
      <td><span class="badge ${doc.status}">${doc.status}</span></td>
      <td>
        <button class="icon-btn danger table-doc-del" title="Delete">
          <svg viewBox="0 0 14 14" fill="none"><path d="M2 4h10M5 4V2.5h4V4M5.5 6.5v4M8.5 6.5v4M3 4l.8 7.5h6.4L11 4" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"/></svg>
        </button>
      </td>`;
    tr.querySelector('.table-doc-del').addEventListener('click', () => deleteDocument(doc.id, doc.filename));
    tableBody.appendChild(tr);
  });
}
async function uploadDocument(file) {
  if (state.docs.some(d => d.filename === file.name)) {
    toast(`"${file.name}" is already uploaded. Delete it first.`, 'error');
    document.getElementById('file-input').value = '';
    return;
  }
  const label = document.getElementById('upload-label'), labelText = document.getElementById('upload-label-text');
  label.classList.add('uploading'); labelText.textContent = 'Uploading…';
  const fd = new FormData(); 
  fd.append('file', file);
  if (state.currentChatId) fd.append('chat_id', state.currentChatId);
  try {
    const res = await apiFetch('POST', '/documents', fd, true);
    await loadDocs(); 
    toast(`"${res.filename}" uploaded`, 'success');
  } catch (e) { toast('Upload failed: ' + e.message, 'error'); }
  finally { label.classList.remove('uploading'); labelText.textContent = 'Browse files'; document.getElementById('file-input').value = ''; }
}
async function deleteDocument(docId, filename) {
  try {
    await apiFetch('DELETE', `/documents/${docId}`);
    state.docs = state.docs.filter(d => d.id !== docId); renderDocList(); toast(`"${filename}" deleted`);
  } catch (e) { toast('Delete failed: ' + e.message, 'error'); }
}

/* ── Events ── */
document.getElementById('file-input').addEventListener('change', e => { const f = e.target.files[0]; if (f) uploadDocument(f); });
const msgInput = document.getElementById('msg-input');
msgInput.addEventListener('keydown', e => {
  if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); const t = msgInput.value.trim(); if (!t) return; msgInput.value = ''; autoResize(msgInput); sendMessage(t); }
});
msgInput.addEventListener('input', () => { autoResize(msgInput); updateSendButton(); });
document.getElementById('send-btn').addEventListener('click', () => { const t = msgInput.value.trim(); if (!t) return; msgInput.value = ''; autoResize(msgInput); sendMessage(t); });
document.getElementById('new-chat-btn').addEventListener('click', createChat);

/* ── Init ── */
async function init() { await Promise.all([loadChats(), loadDocs()]); setView('empty'); updateSendButton(); }
init();
