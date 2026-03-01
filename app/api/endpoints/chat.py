from flask import Blueprint, request, jsonify
from sqlalchemy.exc import SQLAlchemyError
from database.connection import get_session
from database.models import Chat
from sqlalchemy.orm import Session
import uuid

chat_bp = Blueprint("chat", __name__)

def _session():
    return get_session()

@chat_bp.route("/chats", methods=["POST"])
def create_chat():
    """
    Body (optional): { "name": "My conversation" }
    Returns: { "id": "<uuid>", "name": "...", "messages": [] }
    """
    data = request.get_json(silent=True) or {}
    name = data.get("name", "New Conversation")

    chat = Chat(id=uuid.uuid4(), name=name, messages=[])

    with _session() as session:
        try:
            session.add(chat)
            session.commit()
            session.refresh(chat)
            return jsonify({"id": str(chat.id), "name": chat.name, "messages": chat.messages}), 201
        except SQLAlchemyError as e:
            session.rollback()
            return jsonify({"error": str(e)}), 500



@chat_bp.route("/chats", methods=["GET"])
def list_chats():
    """Returns all chats (id, name, message count)."""
    with _session() as session:
        chats = session.query(Chat).all()
        return jsonify([
            {
                "id": str(c.id),
                "name": c.name,
                "message_count": len(c.messages) if c.messages else 0,
                "preview": _preview(c.messages),
            }
            for c in chats
        ]), 200


@chat_bp.route("/chats/<chat_id>", methods=["GET"])
def get_chat(chat_id):
    with _session() as session:
        chat = session.query(Chat).filter_by(id=chat_id).first()
        if not chat:
            return jsonify({"error": "Chat not found"}), 404
        return jsonify({"id": str(chat.id), "name": chat.name, "messages": chat.messages}), 200


# ── SEND MESSAGE (append + get agent reply) ───────────────────────────────────

@chat_bp.route("/chats/<chat_id>/messages", methods=["POST"])
def send_message(chat_id):
    """
    Body: { "message": "Hello" }
    Appends user message, calls agent, appends reply, returns reply.
    """
    data = request.get_json(silent=True) or {}
    message = data.get("message", "").strip()
    if not message:
        return jsonify({"error": "message is required"}), 400

    with _session() as session:
        chat = session.query(Chat).filter_by(id=chat_id).first()
        if not chat:
            return jsonify({"error": "Chat not found"}), 404

        messages = list(chat.messages or [])
        messages.append({"role": "user", "content": message})

        try:
            from app.agent.graph import run_agent  
            reply = run_agent(chat_id=str(chat.id), messages=messages)
        except Exception as e:
            reply = f"[Agent error] {e}"

        messages.append({"role": "assistant", "content": reply})
        chat.messages = messages

        try:
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            return jsonify({"error": str(e)}), 500

        return jsonify({"reply": reply, "messages": messages}), 200



@chat_bp.route("/chats/<chat_id>", methods=["PATCH"])
def rename_chat(chat_id):
    """Body: { "name": "New name" }"""
    data = request.get_json(silent=True) or {}
    name = data.get("name", "").strip()
    if not name:
        return jsonify({"error": "name is required"}), 400

    with _session() as session:
        chat = session.query(Chat).filter_by(id=chat_id).first()
        if not chat:
            return jsonify({"error": "Chat not found"}), 404
        chat.name = name
        try:
            session.commit()
            return jsonify({"id": str(chat.id), "name": chat.name}), 200
        except SQLAlchemyError as e:
            session.rollback()
            return jsonify({"error": str(e)}), 500


@chat_bp.route("/chats/<chat_id>", methods=["DELETE"])
def delete_chat(chat_id):
    with _session() as session:
        chat = session.query(Chat).filter_by(id=chat_id).first()
        if not chat:
            return jsonify({"error": "Chat not found"}), 404
        try:
            session.delete(chat)
            session.commit()
            return jsonify({"deleted": chat_id}), 200
        except SQLAlchemyError as e:
            session.rollback()
            return jsonify({"error": str(e)}), 500


def _preview(messages):
    if not messages:
        return "No messages yet"
    user_msgs = [m for m in messages if m.get("role") == "user"]
    if not user_msgs:
        return "No messages yet"
    raw = user_msgs[-1].get("content", "")
    return raw[:40] + ("…" if len(raw) > 40 else "")