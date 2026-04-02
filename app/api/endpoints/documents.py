from flask import Blueprint, request, jsonify, send_file
import uuid
import io
import logging

from app.database.document_store import (
    create_document,
    delete_document,
    get_document,
    list_documents,
    set_document_status,
)

from app.services.map_store import chat_store
from app.services.logger import get_logger

logger = get_logger(__name__)

documents_bp = Blueprint("documents", __name__)


@documents_bp.route("/documents", methods=["POST"])
def upload_document():
    """
    Upload a document for RAG ingestion
    ---
    tags:
      - Documents
    consumes:
      - multipart/form-data
    parameters:
      - in: formData
        name: file
        type: file
        required: true
        description: PDF, TXT, or MD file to upload
    responses:
      201:
        description: Document uploaded and queued for ingestion
        schema:
          type: object
          properties:
            id:
              type: string
              example: "550e8400-e29b-41d4-a716-446655440000"
            filename:
              type: string
              example: "report.pdf"
            status:
              type: string
              example: "pending"
      400:
        description: Missing file or unsupported file type
      500:
        description: Database error
    """
    if "file" not in request.files:
        return jsonify({"error": "No file part in request"}), 400

    f = request.files["file"]
    if f.filename == "":
        return jsonify({"error": "No file selected"}), 400

    allowed = {"pdf", "txt", "md"}
    ext = f.filename.rsplit(".", 1)[-1].lower()
    if ext not in allowed:
        return jsonify({"error": f"File type '.{ext}' not allowed. Use: {allowed}"}), 400

    raw = f.read()
    filename = f.filename
    chat_id = request.form.get("chat_id")

    doc_id = uuid.uuid4().hex

    try:
        create_document(doc_id, filename, raw, status="pending")
        if chat_id:
            from app.services.map_store import doc_map
            doc_map.link(chat_id, doc_id)
            logger.info(f"[upload] Linked doc {doc_id} to chat {chat_id}")
    except Exception as e:
        logger.error(f"[upload] FalkorDB or Redis error saving document: {e}")
        return jsonify({"error": str(e)}), 500

    try:
        from app.services.worker_threads import submit_async
        from app.rag.rag_processing import ingest_document
        
        ingest_document(doc_id, raw, filename)
        logger.info(f"[upload] Ingestion queued for doc {doc_id} ({filename})")
    except Exception as e:
        logger.error(f"[upload] Failed to queue ingestion for {doc_id}: {e}")
        _mark_failed(doc_id)

    return jsonify({"id": doc_id, "filename": filename, "status": "pending"}), 201


def _mark_failed(doc_id: str):
    try:
        set_document_status(doc_id, "failed")
    except Exception as e:
        logger.error(f"[upload] Could not mark {doc_id} as failed: {e}")


@documents_bp.route("/documents", methods=["GET"])
def list_documents_route():
    """
    List all uploaded documents
    ---
    tags:
      - Documents
    responses:
      200:
        description: List of all documents
        schema:
          type: array
          items:
            type: object
            properties:
              id:
                type: string
                example: "550e8400-e29b-41d4-a716-446655440000"
              filename:
                type: string
                example: "report.pdf"
              status:
                type: string
                enum: [pending, processing, done, failed]
                example: "done"
    """
    try:
        docs = list_documents()
        return jsonify(docs), 200
    except Exception as e:
        logger.error(f"[documents] list failed: {e}")
        return jsonify({"error": str(e)}), 500


@documents_bp.route("/documents/<doc_id>", methods=["GET"])
def get_document_route(doc_id):
    """
    Download a document by ID
    ---
    tags:
      - Documents
    parameters:
      - in: path
        name: doc_id
        required: true
        type: string
        description: UUID of the document
    produces:
      - application/octet-stream
    responses:
      200:
        description: File download
      404:
        description: Document not found
    """
    try:
        doc = get_document(doc_id)
        if not doc:
            return jsonify({"error": "Document not found"}), 404
        return send_file(
            io.BytesIO(doc["content"]),
            download_name=doc["filename"],
            as_attachment=True,
        )
    except Exception as e:
        logger.error(f"[documents] get failed: {e}")
        return jsonify({"error": str(e)}), 500


@documents_bp.route("/documents/<doc_id>", methods=["DELETE"])
def delete_document_route(doc_id):
    """
    Delete a document by ID
    ---
    tags:
      - Documents
    parameters:
      - in: path
        name: doc_id
        required: true
        type: string
        description: UUID of the document to delete
    responses:
      200:
        description: Document deleted successfully
        schema:
          type: object
          properties:
            deleted:
              type: string
              example: "550e8400-e29b-41d4-a716-446655440000"
      404:
        description: Document not found
      500:
        description: Database error
    """
    try:
        if not delete_document(doc_id):
            return jsonify({"error": "Document not found"}), 404
        
        try:
            from app.rag.rag_processing import delete_document_data
            delete_document_data(doc_id)
        except Exception as e:
            logger.warning(f"[documents] Failed to queue RAG cleanup for {doc_id}: {e}")

        return jsonify({"deleted": doc_id}), 200
    except Exception as e:
        logger.error(f"[documents] delete failed: {e}")
        return jsonify({"error": str(e)}), 500