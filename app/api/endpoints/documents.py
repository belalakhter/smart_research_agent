from flask import Blueprint, request, jsonify, send_file
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from database.connection import get_session
from database.models import Document, StatusEnum
import uuid
import io

documents_bp = Blueprint("documents", __name__)

def _session():
    return get_session()

@documents_bp.route("/documents", methods=["POST"])
def upload_document():
    """
    Multipart form: file field named 'file'.
    Returns: { "id": "<uuid>", "filename": "...", "status": "pending" }
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
    doc = Document(
        id=uuid.uuid4(),
        filename=f.filename,
        content=raw,
        status=StatusEnum.pending,
    )

    with _session() as session:
        try:
            session.add(doc)
            session.commit()
            session.refresh(doc)
        except SQLAlchemyError as e:
            session.rollback()
            return jsonify({"error": str(e)}), 500
   
    try:
        from app.services.worker_threads import submit_task  
        from app.rag.lite_rag import ingest_document          
        submit_task(ingest_document, str(doc.id), raw, f.filename)
    except Exception:
        pass  

    return jsonify({
        "id": str(doc.id),
        "filename": doc.filename,
        "status": doc.status.value,
    }), 201


@documents_bp.route("/documents", methods=["GET"])
def list_documents():
    with _session() as session:
        docs = session.query(Document).all()
        return jsonify([
            {
                "id": str(d.id),
                "filename": d.filename,
                "status": d.status.value,
            }
            for d in docs
        ]), 200


@documents_bp.route("/documents/<doc_id>", methods=["GET"])
def get_document(doc_id):
    with _session() as session:
        doc = session.query(Document).filter_by(id=doc_id).first()
        if not doc:
            return jsonify({"error": "Document not found"}), 404
        return send_file(
            io.BytesIO(doc.content),
            download_name=doc.filename,
            as_attachment=True,
        )


@documents_bp.route("/documents/<doc_id>", methods=["DELETE"])
def delete_document(doc_id):
    with _session() as session:
        doc = session.query(Document).filter_by(id=doc_id).first()
        if not doc:
            return jsonify({"error": "Document not found"}), 404
        try:
            session.delete(doc)
            session.commit()
            return jsonify({"deleted": doc_id}), 200
        except SQLAlchemyError as e:
            session.rollback()
            return jsonify({"error": str(e)}), 500