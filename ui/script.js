const API = '/api';
const MAX_IMAGE_BYTES = 5 * 1024 * 1024;
let state = { chats: {}, currentChatId: null, messages: [], docs: [], isThinking: false, pendingImage: null };

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

function sanitizeUrl(url) {
  const value = String(url || '').trim();
  if (/^(https?:\/\/|mailto:)/i.test(value)) return value;
  return '#';
}

function renderInlineMarkdown(text) {
  const tokens = [];
  const withCodePlaceholders = escHtml(text).replace(/`([^`]+)`/g, (_, code) => {
    const token = `@@TOKEN${tokens.length}@@`;
    tokens.push(`<code>${code}</code>`);
    return token;
  });

  const withLinkPlaceholders = withCodePlaceholders.replace(
    /\[([^\]]+)\]\((https?:\/\/[^\s)]+|mailto:[^\s)]+)\)/g,
    (_, label, url) => {
      const token = `@@TOKEN${tokens.length}@@`;
      tokens.push(
        `<a href="${escHtml(sanitizeUrl(url))}" target="_blank" rel="noopener noreferrer">${label}</a>`,
      );
      return token;
    },
  );

  return withLinkPlaceholders
    .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
    .replace(/(^|[\s(])\*([^*\n][^*\n]*?)\*(?=[\s).,!?:;]|$)/g, '$1<em>$2</em>')
    .replace(/@@TOKEN(\d+)@@/g, (_, index) => tokens[Number(index)] || '');
}

function splitTableRow(line) {
  return line
    .trim()
    .replace(/^\|/, '')
    .replace(/\|$/, '')
    .split('|')
    .map(cell => cell.trim());
}

function isTableDividerLine(line) {
  const trimmed = line.trim();
  if (!trimmed || !trimmed.includes('|')) return false;
  return splitTableRow(trimmed).every(cell => /^:?-{3,}:?$/.test(cell));
}

function isTableCandidate(line) {
  const trimmed = line.trim();
  if (!trimmed || !trimmed.includes('|')) return false;
  return splitTableRow(trimmed).length >= 2;
}

function tableAlignments(separatorLine) {
  return splitTableRow(separatorLine).map(cell => {
    if (cell.startsWith(':') && cell.endsWith(':')) return 'center';
    if (cell.endsWith(':')) return 'right';
    return 'left';
  });
}

function renderMarkdownTable(lines, startIndex) {
  const headers = splitTableRow(lines[startIndex]);
  const aligns = tableAlignments(lines[startIndex + 1]);
  const rows = [];
  let index = startIndex + 2;

  while (index < lines.length) {
    const line = lines[index];
    if (!isTableCandidate(line) || !line.trim()) break;
    rows.push(splitTableRow(line));
    index += 1;
  }

  const thead = `<thead><tr>${headers.map((cell, cellIndex) => (
    `<th style="text-align:${aligns[cellIndex] || 'left'}">${renderInlineMarkdown(cell)}</th>`
  )).join('')}</tr></thead>`;

  const tbody = rows.length
    ? `<tbody>${rows.map(row => `<tr>${row.map((cell, cellIndex) => (
      `<td data-label="${escHtml(headers[cellIndex] || '')}" style="text-align:${aligns[cellIndex] || 'left'}">${renderInlineMarkdown(cell)}</td>`
    )).join('')}</tr>`).join('')}</tbody>`
    : '';

  return {
    html: `<div class="md-table-wrap"><table class="md-table">${thead}${tbody}</table></div>`,
    nextIndex: index,
  };
}

function isListItem(line, ordered = false) {
  return ordered
    ? /^\s*\d+\.\s+/.test(line)
    : /^\s*[-*]\s+/.test(line);
}

function stripListMarker(line, ordered = false) {
  return ordered
    ? line.replace(/^\s*\d+\.\s+/, '')
    : line.replace(/^\s*[-*]\s+/, '');
}

function renderMarkdown(text) {
  if (!text) return '';
  const lines = String(text).replace(/\r\n/g, '\n').split('\n');
  const blocks = [];
  let index = 0;

  while (index < lines.length) {
    const line = lines[index];
    const trimmed = line.trim();

    if (!trimmed) {
      index += 1;
      continue;
    }

    if (trimmed.startsWith('```')) {
      const lang = trimmed.slice(3).trim();
      const codeLines = [];
      index += 1;
      while (index < lines.length && !lines[index].trim().startsWith('```')) {
        codeLines.push(lines[index]);
        index += 1;
      }
      if (index < lines.length) index += 1;
      blocks.push(`<pre><code class="lang-${escHtml(lang)}">${escHtml(codeLines.join('\n').trim())}</code></pre>`);
      continue;
    }

    const headingMatch = line.match(/^(#{1,6})\s+(.*)$/);
    if (headingMatch) {
      const level = headingMatch[1].length;
      blocks.push(`<h${level}>${renderInlineMarkdown(headingMatch[2].trim())}</h${level}>`);
      index += 1;
      continue;
    }

    if (/^(-{3,}|\*{3,}|_{3,})$/.test(trimmed)) {
      blocks.push('<hr>');
      index += 1;
      continue;
    }

    if (
      index + 1 < lines.length &&
      isTableCandidate(line) &&
      isTableDividerLine(lines[index + 1])
    ) {
      const table = renderMarkdownTable(lines, index);
      blocks.push(table.html);
      index = table.nextIndex;
      continue;
    }

    if (isListItem(line, false) || isListItem(line, true)) {
      const ordered = isListItem(line, true);
      const tag = ordered ? 'ol' : 'ul';
      const items = [];
      while (index < lines.length && isListItem(lines[index], ordered)) {
        items.push(`<li>${renderInlineMarkdown(stripListMarker(lines[index], ordered))}</li>`);
        index += 1;
      }
      blocks.push(`<${tag}>${items.join('')}</${tag}>`);
      continue;
    }

    if (/^>\s?/.test(trimmed)) {
      const quoteLines = [];
      while (index < lines.length && /^>\s?/.test(lines[index].trim())) {
        quoteLines.push(lines[index].trim().replace(/^>\s?/, ''));
        index += 1;
      }
      blocks.push(`<blockquote><p>${quoteLines.map(renderInlineMarkdown).join('<br>')}</p></blockquote>`);
      continue;
    }

    const paragraphLines = [];
    while (index < lines.length) {
      const current = lines[index];
      const currentTrimmed = current.trim();
      const nextLine = lines[index + 1];

      if (!currentTrimmed) break;
      if (currentTrimmed.startsWith('```')) break;
      if (/^(#{1,6})\s+/.test(currentTrimmed)) break;
      if (isListItem(current, false) || isListItem(current, true)) break;
      if (
        nextLine &&
        isTableCandidate(current) &&
        isTableDividerLine(nextLine)
      ) {
        break;
      }

      paragraphLines.push(renderInlineMarkdown(currentTrimmed));
      index += 1;
    }

    if (paragraphLines.length) {
      blocks.push(`<p>${paragraphLines.join('<br>')}</p>`);
      continue;
    }

    index += 1;
  }

  return blocks.join('');
}
function autoResize(ta) { ta.style.height = 'auto'; ta.style.height = Math.min(ta.scrollHeight, 180) + 'px'; }
function scrollToBottom() { const a = document.getElementById('chat-area'); a.scrollTop = a.scrollHeight; }
function updateSendButton() {
  const i = document.getElementById('msg-input'), s = document.getElementById('send-btn');
  s.disabled = !(i.value.trim() && state.currentChatId && !state.isThinking);
}

function renderUserMessageContent(content, media = null) {
  const parts = [];
  if (media?.name) {
    parts.push(`
      <div class="user-attachment">
        <svg width="12" height="12" viewBox="0 0 16 16" fill="none" aria-hidden="true">
          <path d="M5.5 8.5l4.6-4.6a2.75 2.75 0 113.9 3.9l-6.18 6.18a4 4 0 11-5.66-5.66l5.66-5.66"
            stroke="currentColor" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round"/>
        </svg>
        <span class="user-attachment-name">${escHtml(media.name)}</span>
      </div>
    `);
  }
  if (content) {
    parts.push(`<div class="user-message-text">${escHtml(content)}</div>`);
  }
  return parts.join('');
}

function renderPendingImage() {
  const chip = document.getElementById('image-attachment-chip');
  const preview = chip.querySelector('.attachment-chip-preview');
  const attachBtn = document.getElementById('attach-image-btn');

  if (!state.pendingImage) {
    chip.hidden = true;
    chip.removeAttribute('title');
    preview.style.backgroundImage = '';
    attachBtn.hidden = false;
    attachBtn.classList.remove('has-attachment');
    return;
  }

  chip.hidden = false;
  chip.title = state.pendingImage.name || 'Attached image';
  preview.style.backgroundImage = `url("${state.pendingImage.data_url}")`;
  attachBtn.hidden = true;
  attachBtn.classList.add('has-attachment');
}

function clearPendingImage() {
  state.pendingImage = null;
  document.getElementById('image-input').value = '';
  renderPendingImage();
}

function readFileAsDataUrl(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ''));
    reader.onerror = () => reject(reader.error || new Error('Failed to read image.'));
    reader.readAsDataURL(file);
  });
}

async function setPendingImage(file) {
  if (!file) return;
  if (!file.type.startsWith('image/')) {
    clearPendingImage();
    toast('Please choose a valid image file.', 'error');
    return;
  }
  if (file.size > MAX_IMAGE_BYTES) {
    clearPendingImage();
    toast('Image is too large. Please keep it under 5 MB.', 'error');
    return;
  }

  try {
    const dataUrl = await readFileAsDataUrl(file);
    state.pendingImage = {
      type: 'image',
      name: file.name,
      mime_type: file.type || 'image/png',
      data_url: dataUrl,
    };
    renderPendingImage();
  } catch (e) {
    clearPendingImage();
    toast('Image attachment failed: ' + e.message, 'error');
  }
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
function appendMessage(role, content, media = null) {
  const c = document.getElementById('messages'), row = document.createElement('div');
  row.className = `msg-row ${role}`;
  if (role === 'assistant') {
    row.innerHTML = `<img src="/assets/icon.png" class="msg-avatar" alt="S"><div class="msg-content">${renderMarkdown(content)}</div>`;
  } else {
    row.innerHTML = `<div class="msg-content">${renderUserMessageContent(content, media)}</div>`;
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
  state.messages.forEach(m => appendMessage(m.role, m.content, m.media)); scrollToBottom();
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
    clearPendingImage();
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
    clearPendingImage();
    state.chats[res.id] = res; state.currentChatId = res.id; state.messages = [];
    renderMessages(); renderConvList(); updateTopbar(); setView('empty');
    document.getElementById('msg-input').focus();
  } catch (e) { toast('Failed to create chat: ' + e.message, 'error'); }
}
async function deleteChat(cid) {
  try {
    await apiFetch('DELETE', `/chats/${cid}`);
    delete state.chats[cid];
    if (state.currentChatId === cid) { clearPendingImage(); state.currentChatId = null; state.messages = []; renderMessages(); updateTopbar(); setView('empty'); }
    renderConvList(); toast('Conversation deleted');
  } catch (e) { toast('Delete failed: ' + e.message, 'error'); }
}

/* ── Send message ── */
async function sendMessage(text, media = null) {
  if (!text || !state.currentChatId || state.isThinking) return;
  state.isThinking = true; updateSendButton();
  if (state.messages.length === 0) setView('chat');
  const optimisticMedia = media ? { name: media.name } : null;
  appendMessage('user', text, optimisticMedia);
  state.messages.push({ role: 'user', content: text, ...(optimisticMedia ? { media: optimisticMedia } : {}) });
  showThinking();
  const input = document.getElementById('msg-input'); input.disabled = true;
  try {
    const payload = { message: text };
    if (media) payload.media = media;
    const res = await apiFetch('POST', `/chats/${state.currentChatId}/messages`, payload);
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

function submitCurrentMessage() {
  const text = msgInput.value.trim();
  if (!text) return;
  const media = state.pendingImage ? { ...state.pendingImage } : null;
  msgInput.value = '';
  autoResize(msgInput);
  clearPendingImage();
  sendMessage(text, media);
}

/* ── Events ── */
document.getElementById('file-input').addEventListener('change', e => { const f = e.target.files[0]; if (f) uploadDocument(f); });
document.getElementById('image-input').addEventListener('change', async e => {
  const file = e.target.files[0];
  if (file) await setPendingImage(file);
  e.target.value = '';
});
document.getElementById('attach-image-btn').addEventListener('click', () => {
  if (!state.isThinking) document.getElementById('image-input').click();
});
document.getElementById('clear-image-btn').addEventListener('click', clearPendingImage);
const msgInput = document.getElementById('msg-input');
msgInput.addEventListener('keydown', e => {
  if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); submitCurrentMessage(); }
});
msgInput.addEventListener('input', () => { autoResize(msgInput); updateSendButton(); });
document.getElementById('send-btn').addEventListener('click', submitCurrentMessage);
document.getElementById('new-chat-btn').addEventListener('click', createChat);

/* ── Init ── */
async function init() { await Promise.all([loadChats(), loadDocs()]); clearPendingImage(); setView('empty'); updateSendButton(); }
init();
