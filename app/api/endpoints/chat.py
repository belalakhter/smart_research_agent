from flask import Blueprint, request, jsonify
import base64
import binascii
import uuid
import threading

from app.services.map_store import chat_store

chat_bp = Blueprint("chat", __name__)

import json
from app.database.connection import get_redis

chat_bp = Blueprint("chat", __name__)

METADATA_KEY = "smart_agent:chat_metadata"
MAX_INLINE_IMAGE_BYTES = 5 * 1024 * 1024


def _normalize_media_payload(raw_media):
    if not isinstance(raw_media, dict):
        return None

    media_type = str(raw_media.get("type", "image")).strip().lower()
    name = str(raw_media.get("name", "")).strip() or "image"
    mime_type = str(raw_media.get("mime_type", "")).strip().lower()
    data_url = str(raw_media.get("data_url", "")).strip()

    if media_type != "image" or not data_url.startswith("data:image/"):
        return None

    header, _, encoded = data_url.partition(",")
    if not header or not encoded or ";base64" not in header:
        return None

    header_mime = header[5:].split(";", 1)[0].strip().lower()
    mime_type = mime_type or header_mime
    if not mime_type.startswith("image/") or header_mime != mime_type:
        return None

    try:
        binary = base64.b64decode(encoded, validate=True)
    except (ValueError, binascii.Error):
        return None

    if len(binary) > MAX_INLINE_IMAGE_BYTES:
        return None

    return {
        "type": "image",
        "name": name,
        "mime_type": mime_type,
        "data_url": data_url,
    }

@chat_bp.route("/chats", methods=["POST"])
def create_chat():
    """
    Create a new chat
    ---
    tags:
      - Chats
    parameters:
      - in: body
        name: body
        required: false
        schema:
          type: object
          properties:
            name:
              type: string
              example: My conversation
    responses:
      201:
        description: Chat created successfully
        schema:
          type: object
          properties:
            id:
              type: string
              example: "550e8400-e29b-41d4-a716-446655440000"
            name:
              type: string
              example: My conversation
            messages:
              type: array
              items: {}
      500:
        description: Database error
    """
    data = request.get_json(silent=True) or {}
    name = data.get("name", "New Conversation")

    chat_id = uuid.uuid4().hex
    chat_data = {"id": chat_id, "name": name}
    
    try:
        r = get_redis()
        r.hset(METADATA_KEY, chat_id, json.dumps(chat_data))
    except Exception as e:
        return jsonify({"error": f"Failed to save chat: {e}"}), 500

    return jsonify({"id": chat_id, "name": name, "messages": []}), 201


@chat_bp.route("/chats", methods=["GET"])
def list_chats():
    """
    List all chats
    ---
    tags:
      - Chats
    responses:
      200:
        description: List of all chats
        schema:
          type: array
          items:
            type: object
            properties:
              id:
                type: string
                example: "550e8400-e29b-41d4-a716-446655440000"
              name:
                type: string
                example: My conversation
              message_count:
                type: integer
                example: 5
              preview:
                type: string
                example: "Hello, how can I help you…"
    """
    try:
        r = get_redis()
        raw_meta = r.hgetall(METADATA_KEY)
        chats = [json.loads(v) for v in raw_meta.values()]
    except Exception as e:
        return jsonify({"error": f"Failed to list chats: {e}"}), 500

    return jsonify([
        {
            "id": c["id"],
            "name": c["name"],
            "message_count": chat_store.size(c["id"]),
            "preview": _preview(chat_store.get(c["id"])),
        }
        for c in chats
    ]), 200


@chat_bp.route("/chats/<chat_id>", methods=["GET"])
def get_chat(chat_id):
    """
    Get a chat by ID
    ---
    tags:
      - Chats
    parameters:
      - in: path
        name: chat_id
        required: true
        type: string
        description: UUID of the chat
    responses:
      200:
        description: Chat object with full message history
        schema:
          type: object
          properties:
            id:
              type: string
            name:
              type: string
            messages:
              type: array
              items:
                type: object
                properties:
                  role:
                    type: string
                    example: user
                  content:
                    type: string
                    example: Hello!
      404:
        description: Chat not found
    """
    try:
        r = get_redis()
        raw = r.hget(METADATA_KEY, chat_id)
        if not raw:
            return jsonify({"error": "Chat not found"}), 404
        chat = json.loads(raw)
    except Exception as e:
        return jsonify({"error": f"Failed to get chat: {e}"}), 500

    return jsonify({
        "id": chat["id"],
        "name": chat["name"],
        "messages": chat_store.get(chat_id),
    }), 200


@chat_bp.route("/chats/<chat_id>/messages", methods=["POST"])
def send_message(chat_id):
    """
    Send a message to a chat
    ---
    tags:
      - Chats
    parameters:
      - in: path
        name: chat_id
        required: true
        type: string
        description: UUID of the chat
      - in: body
        name: body
        required: true
        schema:
          type: object
          required:
            - message
          properties:
            message:
              type: string
              example: "What is the weather today?"
    responses:
      200:
        description: Agent reply and updated message history
        schema:
          type: object
          properties:
            reply:
              type: string
              example: "I don't have access to real-time weather data."
            messages:
              type: array
              items:
                type: object
                properties:
                  role:
                    type: string
                    example: assistant
                  content:
                    type: string
      400:
        description: Missing message field
      404:
        description: Chat not found
      500:
        description: Database error
    """
    data = request.get_json(silent=True) or {}
    message = data.get("message", "").strip()
    media = None
    if not message:
        return jsonify({"error": "message is required"}), 400
    if data.get("media") is not None:
        media = _normalize_media_payload(data.get("media"))
        if media is None:
            return jsonify({"error": "invalid image attachment"}), 400

    try:
        r = get_redis()
        if not r.hexists(METADATA_KEY, chat_id):
            return jsonify({"error": "Chat not found"}), 404
    except Exception as e:
        return jsonify({"error": f"Database error: {e}"}), 500

    chat_store.push(chat_id, {"role": "user", "content": message})
    messages = chat_store.get(chat_id)
    messages_for_agent = messages
    if media and messages:
        messages_for_agent = [
            *messages[:-1],
            {**messages[-1], "media": [media]},
        ]

    try:
        from app.agent.graph import run_agent
        reply = run_agent(chat_id=chat_id, messages=messages_for_agent)
    except Exception as e:
        reply = f"[Agent error] {e}"

    chat_store.push(chat_id, {"role": "assistant", "content": reply})
    messages = chat_store.get(chat_id)
    return jsonify({"reply": reply, "messages": messages}), 200


@chat_bp.route("/chats/<chat_id>", methods=["PATCH"])
def rename_chat(chat_id):
    """
    Rename a chat
    ---
    tags:
      - Chats
    parameters:
      - in: path
        name: chat_id
        required: true
        type: string
        description: UUID of the chat
      - in: body
        name: body
        required: true
        schema:
          type: object
          required:
            - name
          properties:
            name:
              type: string
              example: "Renamed conversation"
    responses:
      200:
        description: Chat renamed successfully
        schema:
          type: object
          properties:
            id:
              type: string
            name:
              type: string
      400:
        description: Missing name field
      404:
        description: Chat not found
      500:
        description: Database error
    """
    data = request.get_json(silent=True) or {}
    name = data.get("name", "").strip()
    if not name:
        return jsonify({"error": "name is required"}), 400

    try:
        r = get_redis()
        raw = r.hget(METADATA_KEY, chat_id)
        if not raw:
            return jsonify({"error": "Chat not found"}), 404
        
        chat = json.loads(raw)
        chat["name"] = name
        r.hset(METADATA_KEY, chat_id, json.dumps(chat))
    except Exception as e:
        return jsonify({"error": f"Failed to rename chat: {e}"}), 500

    return jsonify({"id": chat_id, "name": name}), 200


@chat_bp.route("/chats/<chat_id>", methods=["DELETE"])
def delete_chat(chat_id):
    """
    Delete a chat
    ---
    tags:
      - Chats
    parameters:
      - in: path
        name: chat_id
        required: true
        type: string
        description: UUID of the chat to delete
    responses:
      200:
        description: Chat deleted successfully
        schema:
          type: object
          properties:
            deleted:
              type: string
              example: "550e8400-e29b-41d4-a716-446655440000"
      404:
        description: Chat not found
      500:
        description: Database error
    """
    try:
        r = get_redis()
        if not r.hdel(METADATA_KEY, chat_id):
            return jsonify({"error": "Chat not found"}), 404
        chat_store.delete(chat_id)
    except Exception as e:
        return jsonify({"error": f"Failed to delete chat: {e}"}), 500

    return jsonify({"deleted": chat_id}), 200


def _preview(messages):
    if not messages:
        return "No messages yet"
    user_msgs = [m for m in messages if m.get("role") == "user"]
    if not user_msgs:
        return "No messages yet"
    raw = user_msgs[-1].get("content", "")
    return raw[:40] + ("…" if len(raw) > 40 else "")
