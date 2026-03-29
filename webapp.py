from __future__ import annotations

import os
from pathlib import Path

from flask import Flask, render_template, request

from lib.manual_uploads import infer_format, parse_uploaded_logs, summarize_matches
from lib.storage import (
    create_manual_upload,
    create_manual_upload_with_unparsed_file,
    insert_manual_upload_matches,
    list_recent_capture_sessions,
    list_recent_manual_uploads,
)


BASE_DIR = Path(__file__).resolve().parent
app = Flask(
    __name__,
    template_folder=str(BASE_DIR / "templates"),
    static_folder=str(BASE_DIR / "static"),
)
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024


@app.get("/")
def index():
    return render_template(
        "upload.html",
        recent_uploads=list_recent_manual_uploads(),
        recent_sessions=list_recent_capture_sessions(),
        error=None,
        success=None,
    )


@app.post("/upload")
def upload():
    upload = request.files.get("log_file")
    session_name = (request.form.get("session_name") or "").strip()
    server_name = (request.form.get("server_name") or "").strip() or None
    uploader_name = (request.form.get("uploader_name") or "").strip() or None
    notes = (request.form.get("notes") or "").strip() or None

    if upload is None or not upload.filename:
        return _render_with_message(error="Choose an HLU export file to upload.")
    if not session_name:
        return _render_with_message(error="Enter a session name so the upload can be identified later.")

    raw_text = upload.stream.read().decode("utf-8", errors="replace")
    file_format = infer_format(upload.filename, upload.content_type)

    metadata = {
        "content_length": len(raw_text.encode("utf-8")),
        "parser": file_format,
    }

    try:
        logs = parse_uploaded_logs(file_format, raw_text)
    except Exception as exc:
        upload_id = create_manual_upload_with_unparsed_file(
            session_name=session_name,
            server_name=server_name,
            source_filename=upload.filename,
            content_type=upload.content_type,
            file_format=file_format,
            uploader_name=uploader_name,
            notes=notes,
            raw_text=raw_text,
            metadata={**metadata, "parse_error": str(exc)},
        )
        return _render_with_message(
            success=f"Uploaded file #{upload_id}. The raw file was saved, but structured parsing failed: {exc}",
        )

    upload_id = create_manual_upload(
        session_name=session_name,
        server_name=server_name,
        source_filename=upload.filename,
        content_type=upload.content_type,
        file_format=file_format,
        uploader_name=uploader_name,
        notes=notes,
        raw_text=raw_text,
        logs=logs,
        metadata=metadata,
    )
    insert_manual_upload_matches(upload_id, summarize_matches(logs))
    return _render_with_message(
        success=f"Uploaded file #{upload_id} with {len(logs)} parsed log rows into the shared archive.",
    )


@app.get("/health")
def health():
    return {"status": "ok"}


def _render_with_message(*, error: str | None = None, success: str | None = None):
    return render_template(
        "upload.html",
        recent_uploads=list_recent_manual_uploads(),
        recent_sessions=list_recent_capture_sessions(),
        error=error,
        success=success,
    )


if __name__ == "__main__":
    app.run(
        host=os.getenv("HLU_WEB_HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", os.getenv("HLU_WEB_PORT", "8080"))),
        debug=False,
    )
