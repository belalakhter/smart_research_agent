import streamlit as st
import requests
from typing import Optional

BACKEND_URL = "http://localhost:3000/api"

st.set_page_config(
    page_title="Smat Agent",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;500;600;700;800&family=DM+Mono:ital,wght@0,300;0,400;0,500;1,300&display=swap');

:root {
    --bg: #08080e;
    --surface: #0e0e18;
    --surface-raised: #13131f;
    --surface-hover: #18182a;
    --border: #1a1a2c;
    --border-bright: #252538;
    --accent: #7c6af7;
    --accent-2: #a78bfa;
    --accent-glow: rgba(124, 106, 247, 0.2);
    --accent-subtle: rgba(124, 106, 247, 0.07);
    --text-primary: #eaeaf5;
    --text-secondary: #6e6e96;
    --text-muted: #3a3a55;
    --user-bubble: #14142a;
    --success: #34d399;
    --error: #f87171;
}

*, *::before, *::after { box-sizing: border-box; }

html, body, [data-testid="stAppViewContainer"], [data-testid="stMain"] {
    background-color: var(--bg) !important;
    font-family: 'DM Mono', monospace !important;
    color: var(--text-primary) !important;
}

/* ── SIDEBAR ── */
[data-testid="stSidebar"] {
    background: var(--surface) !important;
    border-right: 1px solid var(--border) !important;
    min-width: 300px !important;
    max-width: 300px !important;
}

[data-testid="stSidebarCollapsedControl"],
[data-testid="stSidebarCollapseButton"],
button[aria-label="Close sidebar"],
button[title="Close sidebar"] {
    display: none !important;
}

/* brand title — NOT italic */
.sb-brand-name {
    font-family: 'Syne', sans-serif !important;
    font-style: normal !important;
    font-size: 1.15rem;
    font-weight: 800;
    color: var(--text-primary);
    letter-spacing: -0.03em;
    padding: 1.2rem 1rem 0.9rem;
    border-bottom: 1px solid var(--border);
}

.sb-label {
    font-size: 0.58rem;
    font-weight: 600;
    letter-spacing: 0.22em;
    text-transform: uppercase;
    color: var(--text-muted);
    padding: 0.9rem 1rem 0.4rem;
}

/* New conversation button */
[data-testid="stSidebar"] button[kind="primary"] {
    background: var(--surface-raised) !important;
    border: 1px solid var(--border-bright) !important;
    color: var(--text-primary) !important;
    font-family: 'Syne', sans-serif !important;
    font-size: 0.82rem !important;
    font-weight: 600 !important;
    border-radius: 8px !important;
    width: 100% !important;
    transition: all 0.15s ease !important;
    box-shadow: none !important;
    margin: 0.75rem 0 0.4rem !important;
}

[data-testid="stSidebar"] button[kind="primary"]:hover {
    background: var(--surface-hover) !important;
}

/* ── CONVERSATION ROW ── */
.th-row {
    display: flex;
    align-items: center;
    gap: 0;
    padding: 0 0.6rem;
    border-radius: 10px;
    border: 1px solid transparent;
    transition: background 0.15s, border-color 0.15s;
    position: relative;
    margin: 1px 0.5rem;
}

.th-row:hover { background: var(--surface-hover); border-color: var(--border); }
.th-row.active { background: rgba(124,106,247,0.09); border-color: rgba(124,106,247,0.22); }

/* Streamlit button resets inside sidebar */
[data-testid="stSidebar"] .stButton > button {
    all: unset !important;
    cursor: pointer !important;
    display: block !important;
    width: 100% !important;
    text-align: left !important;
    font-family: 'DM Mono', monospace !important;
    font-size: 0.8rem !important;
    color: var(--text-primary) !important;
    padding: 0.6rem 0.5rem !important;
    border-radius: 8px !important;
    transition: color 0.15s !important;
    white-space: nowrap !important;
    overflow: hidden !important;
    text-overflow: ellipsis !important;
}

[data-testid="stSidebar"] .stButton > button:hover {
    color: var(--accent-2) !important;
}

/* small icon buttons (delete / rename) */
.del-btn > button {
    all: unset !important;
    cursor: pointer !important;
    display: inline-flex !important;
    align-items: center !important;
    justify-content: center !important;
    font-size: 0.75rem !important;
    color: var(--text-muted) !important;
    padding: 3px 7px !important;
    border-radius: 6px !important;
    border: 1px solid transparent !important;
    transition: all 0.15s !important;
    white-space: nowrap !important;
}

.del-btn > button:hover {
    color: var(--error) !important;
    border-color: rgba(248,113,113,0.35) !important;
    background: rgba(248,113,113,0.07) !important;
}

/* ── MAIN ── */
.main .block-container {
    max-width: 820px !important;
    padding: 0 2rem 2rem !important;
    margin: 0 auto !important;
}

.topbar {
    padding: 1.15rem 0 0.75rem;
    border-bottom: 1px solid var(--border);
    margin-bottom: 1.5rem;
    display: flex;
    align-items: center;
    justify-content: space-between;
}

.topbar-title {
    font-family: 'Syne', sans-serif !important;
    font-style: normal !important;
    font-size: 1rem;
    font-weight: 700;
    color: var(--text-primary);
    letter-spacing: -0.02em;
}

.topbar-sub {
    font-size: 0.65rem;
    color: var(--text-muted);
}

.status-pill {
    display: flex;
    align-items: center;
    gap: 6px;
    padding: 4px 11px;
    background: rgba(52,211,153,0.07);
    border: 1px solid rgba(52,211,153,0.18);
    border-radius: 20px;
    font-size: 0.6rem;
    color: var(--success);
    letter-spacing: 0.1em;
    text-transform: uppercase;
}

.status-dot {
    width: 5px; height: 5px;
    background: var(--success);
    border-radius: 50%;
    animation: pulse 2.5s infinite;
}

@keyframes pulse {
    0%,100%{opacity:1}50%{opacity:0.4}
}

/* ── CHAT MESSAGES ── */
[data-testid="stChatMessage"] {
    background: transparent !important;
    border: none !important;
    padding: 0.45rem 0 !important;
}

[data-testid="stChatMessageContent"] {
    background: var(--surface-raised) !important;
    border: 1px solid var(--border) !important;
    border-radius: 14px !important;
    padding: 0.9rem 1.15rem !important;
    font-family: 'DM Mono', monospace !important;
    font-size: 0.875rem !important;
    line-height: 1.8 !important;
    color: var(--text-primary) !important;
}

[data-testid="stChatMessage"]:has([data-testid="stChatMessageAvatarUser"]) [data-testid="stChatMessageContent"] {
    background: var(--user-bubble) !important;
    border-color: rgba(124,106,247,0.14) !important;
}

/* ── CHAT INPUT ── */
[data-testid="stChatInput"] {
    border-top: 1px solid var(--border) !important;
    background: var(--surface) !important;
    padding: 1rem !important;
}

[data-testid="stChatInputContainer"] {
    background: var(--surface-raised) !important;
    border: 1px solid var(--border-bright) !important;
    border-radius: 14px !important;
    transition: border-color 0.2s !important;
}

[data-testid="stChatInputContainer"]:focus-within {
    border-color: rgba(124,106,247,0.35) !important;
}

[data-testid="stChatInputTextArea"] {
    font-family: 'DM Mono', monospace !important;
    font-size: 0.875rem !important;
    color: var(--text-primary) !important;
    background: transparent !important;
}

/* ── FILE UPLOADER ── */
[data-testid="stSidebar"] [data-testid="stFileUploader"] {
    background: transparent !important;
    border: none !important;
    padding: 0 0.5rem !important;
    margin: 0 !important;
}

[data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"] {
    background: var(--surface-raised) !important;
    border: 1px dashed var(--border-bright) !important;
    border-radius: 10px !important;
    padding: 0.85rem 1rem !important;
    transition: all 0.2s !important;
}

[data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"]:hover {
    border-color: var(--accent) !important;
    background: var(--surface-hover) !important;
}

/* hide default uploader label text; keep Browse files button */
[data-testid="stSidebar"] [data-testid="stFileUploaderDropzoneInstructions"] > div > small {
    display: none !important;
}

[data-testid="stSidebar"] [data-testid="stFileUploaderDropzoneInstructions"] > div > span {
    display: none !important;
}

/* "Browse files" button inside uploader */
[data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"] button {
    all: unset !important;
    cursor: pointer !important;
    display: inline-flex !important;
    align-items: center !important;
    justify-content: center !important;
    gap: 6px !important;
    font-family: 'Syne', sans-serif !important;
    font-size: 0.78rem !important;
    font-weight: 600 !important;
    color: var(--accent-2) !important;
    background: rgba(124,106,247,0.1) !important;
    border: 1px solid rgba(124,106,247,0.25) !important;
    border-radius: 7px !important;
    padding: 0.5rem 1rem !important;
    transition: all 0.15s !important;
    width: 100% !important;
    text-align: center !important;
}

[data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"] button:hover {
    background: rgba(124,106,247,0.18) !important;
    border-color: rgba(124,106,247,0.45) !important;
    color: white !important;
}

/* divider */
.sb-divider {
    height: 1px;
    background: var(--border);
    margin: 0.6rem 0.5rem;
}

/* doc list items */
.doc-item {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0.45rem 0.85rem;
    font-size: 0.72rem;
    color: var(--text-secondary);
    border-radius: 8px;
    margin: 1px 0.5rem;
    border: 1px solid transparent;
    transition: all 0.15s;
}

.doc-item:hover {
    background: var(--surface-hover);
    border-color: var(--border);
    color: var(--text-primary);
}

.doc-name { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; flex: 1; }

.doc-status-pending  { color: #fbbf24; font-size: 0.6rem; letter-spacing: 0.08em; }
.doc-status-completed{ color: var(--success); font-size: 0.6rem; letter-spacing: 0.08em; }

/* empty state */
.empty-state {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    min-height: 55vh;
    gap: 14px;
    text-align: center;
}

.empty-title {
    font-family: 'Syne', sans-serif;
    font-style: normal;
    font-size: 1.15rem;
    font-weight: 700;
    color: var(--text-secondary);
}

.empty-sub {
    font-size: 0.76rem;
    color: var(--text-muted);
    line-height: 1.8;
}

/* text input (for rename) */
[data-testid="stSidebar"] input[type="text"] {
    background: var(--surface-raised) !important;
    border: 1px solid var(--border-bright) !important;
    border-radius: 7px !important;
    color: var(--text-primary) !important;
    font-family: 'DM Mono', monospace !important;
    font-size: 0.8rem !important;
    padding: 0.45rem 0.7rem !important;
}
</style>
""", unsafe_allow_html=True)


def api(method: str, path: str, **kwargs):
    try:
        r = getattr(requests, method)(f"{BACKEND_URL}{path}", timeout=15, **kwargs)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"error": str(e)}


def load_chats():
    data = api("get", "/chats")
    if isinstance(data, list):
        st.session_state.chats_meta = {c["id"]: c for c in data}
    else:
        st.session_state.chats_meta = {}


def load_documents():
    data = api("get", "/documents")
    st.session_state.documents = data if isinstance(data, list) else []


if "chats_meta" not in st.session_state:
    load_chats()
if "documents" not in st.session_state:
    load_documents()
if "current_chat_id" not in st.session_state:
    st.session_state.current_chat_id = None
if "messages" not in st.session_state:
    st.session_state.messages = []
if "new_chat_name" not in st.session_state:
    st.session_state.new_chat_name = ""
if "doc_upload_key" not in st.session_state:
    st.session_state.doc_upload_key = 0
if "editing_chat_id" not in st.session_state:
    st.session_state.editing_chat_id = None

with st.sidebar:
    st.markdown('<div class="sb-brand-name">Smat Agent</div>', unsafe_allow_html=True)

    name_input = st.text_input(
        "Conversation name",
        placeholder="e.g. Research session",
        label_visibility="collapsed",
        key="conv_name_input",
    )

    if st.button("＋  New Conversation", type="primary", use_container_width=True):
        chat_name = name_input.strip() or "New Conversation"
        result = api("post", "/chats", json={"name": chat_name})
        if "id" in result:
            load_chats()
            st.session_state.current_chat_id = result["id"]
            st.session_state.messages = []
            st.rerun()

    if st.session_state.chats_meta:
        st.markdown('<div class="sb-label">Conversations</div>', unsafe_allow_html=True)

        for cid, meta in list(st.session_state.chats_meta.items()):
            is_active = st.session_state.current_chat_id == cid
            label = meta.get("name", f"#{cid[:6]}")
            preview = meta.get("preview", "")

            is_editing = st.session_state.editing_chat_id == cid

            col_main, col_rename, col_del = st.columns([7, 2, 2])

            with col_main:
                if is_editing:
                    st.text_input(
                        "Rename conversation",
                        value=label,
                        key=f"edit_name_{cid}",
                        label_visibility="collapsed",
                    )
                else:
                    display = f"{'▶ ' if is_active else ''}{label}"
                    if st.button(display, key=f"sel_{cid}", use_container_width=True, help=preview):
                        if st.session_state.current_chat_id != cid:
                            st.session_state.current_chat_id = cid
                            chat_data = api("get", f"/chats/{cid}")
                            st.session_state.messages = chat_data.get("messages", []) if "messages" in chat_data else []
                            st.rerun()

            with col_rename:
                st.markdown('<div class="del-btn">', unsafe_allow_html=True)
                if is_editing:
                    if st.button("Save", key=f"save_{cid}", help="Save conversation name"):
                        new_name = st.session_state.get(f"edit_name_{cid}", "").strip()
                        if new_name:
                            result = api("patch", f"/chats/{cid}", json={"name": new_name})
                            if "error" not in result:
                                load_chats()
                                st.session_state.editing_chat_id = None
                                st.session_state.current_chat_id = cid
                                st.rerun()
                        else:
                            st.toast("Name cannot be empty", icon="⚠️")
                else:
                    if st.button("✎", key=f"rename_{cid}", help="Rename conversation"):
                        st.session_state.editing_chat_id = cid
                        st.rerun()
                st.markdown('</div>', unsafe_allow_html=True)

            with col_del:
                st.markdown('<div class="del-btn">', unsafe_allow_html=True)
                if st.button("🗑", key=f"del_{cid}", help="Delete conversation"):
                    api("delete", f"/chats/{cid}")
                    if st.session_state.current_chat_id == cid:
                        st.session_state.current_chat_id = None
                        st.session_state.messages = []
                    if st.session_state.editing_chat_id == cid:
                        st.session_state.editing_chat_id = None
                    load_chats()
                    st.rerun()
                st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.markdown("""
        <div style="padding:1rem 0.5rem;color:var(--text-muted);font-size:0.78rem;text-align:center;">
            No conversations yet
        </div>
        """, unsafe_allow_html=True)

    st.markdown('<div class="sb-divider"></div>', unsafe_allow_html=True)
    st.markdown('<div class="sb-label" style="padding-left:0.5rem;">Documents</div>', unsafe_allow_html=True)

    uploaded = st.file_uploader(
        "Upload document",
        type=["pdf", "txt", "md"],
        key=f"doc_upload_{st.session_state.doc_upload_key}",
        label_visibility="collapsed",
    )

    if uploaded is not None:
        # Upload to backend
        files = {"file": (uploaded.name, uploaded.getvalue(), uploaded.type)}
        result = api("post", "/documents", files=files)
        if "id" in result:
            st.toast(f"✓ {uploaded.name} uploaded", icon="📄")
            load_documents()
            st.session_state.doc_upload_key += 1
            st.rerun()
        elif "error" in result:
            st.toast(f"Upload failed: {result['error']}", icon="⚠️")

    # Uploaded docs list
    if st.session_state.documents:
        for doc in st.session_state.documents:
            status_cls = "doc-status-completed" if doc["status"] == "completed" else "doc-status-pending"
            status_label = "✓" if doc["status"] == "completed" else "…"
            col_name, col_del = st.columns([8, 2])
            with col_name:
                st.markdown(
                    f'<div class="doc-item"><span class="doc-name">{doc["filename"]}</span>'
                    f'<span class="{status_cls}">{status_label}</span></div>',
                    unsafe_allow_html=True,
                )
            with col_del:
                st.markdown('<div class="del-btn">', unsafe_allow_html=True)
                if st.button("🗑", key=f"ddel_{doc['id']}", help="Delete document"):
                    api("delete", f"/documents/{doc['id']}")
                    load_documents()
                    st.rerun()
                st.markdown('</div>', unsafe_allow_html=True)



current = st.session_state.current_chat_id
chat_meta = st.session_state.chats_meta.get(current, {}) if current else {}
chat_title = chat_meta.get("name", "Smat Agent") if current else "Smat Agent"

st.markdown(f"""
<div class="topbar">
  <div>
    <div class="topbar-title">{chat_title}</div>
    <div class="topbar-sub">{'#' + current[:8] if current else 'No conversation selected'}</div>
  </div>
  <div class="status-pill"><div class="status-dot"></div>online</div>
</div>
""", unsafe_allow_html=True)

if current is None:
    st.markdown("""
    <div class="empty-state">
        <div class="empty-title">Start a new conversation</div>
        <div class="empty-sub">Give it a name and press<br>＋ New Conversation in the sidebar.</div>
    </div>
    """, unsafe_allow_html=True)
else:
    msgs = st.session_state.messages

    if not msgs:
        st.markdown("""
        <div class="empty-state">
            <div class="empty-title">New conversation</div>
            <div class="empty-sub">Type a message below to get started.</div>
        </div>
        """, unsafe_allow_html=True)
    else:
        for m in msgs:
            role = m.get("role", "user")
            st.chat_message(role).markdown(m.get("content", ""))

    user_input = st.chat_input("Type your message…")

    if user_input:
        st.chat_message("user").markdown(user_input)
        with st.spinner("Thinking…"):
            result = api("post", f"/chats/{current}/messages", json={"message": user_input})
        reply = result.get("reply", result.get("error", "No response"))
        st.chat_message("assistant").markdown(reply)
        st.session_state.messages = result.get("messages", st.session_state.messages + [
            {"role": "user", "content": user_input},
            {"role": "assistant", "content": reply},
        ])
        load_chats()