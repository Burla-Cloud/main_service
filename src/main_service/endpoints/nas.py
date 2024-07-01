from fastapi import APIRouter, HTTPException, Depends, Request
from google.cloud import storage

from main_service import NAS_BUCKET, get_user_doc, get_logger, get_request_json
from main_service.helpers import create_signed_gcs_urls, Logger


router = APIRouter()


@router.post("/v1/bcs/upload_urls")
def create_upload_urls(
    request_json: dict = Depends(get_request_json),
    user_doc: dict = Depends(get_user_doc),
    logger: Logger = Depends(get_logger),
):
    filepaths = request_json["remote_filepaths"]
    filepaths_are_absolute = all([fp.startswith("/") for fp in filepaths])
    if not filepaths_are_absolute:
        logger.log(f"Tell this person paths must be absolute?")
        HTTPException(400, detail="paths must be absolute")

    email = user_doc.get("email").replace("@", "_at_")
    uris = [f"gs://{NAS_BUCKET}/{email}{path}" for path in filepaths]
    uris_to_urls = create_signed_gcs_urls(uris)
    uri_to_path = lambda uri: "/" + "/".join(uri.split("/")[4:])
    paths_to_urls = [(uri_to_path(x["uri"]), x["url"]) for x in uris_to_urls]
    return {"paths_to_urls": paths_to_urls}


@router.post("/v1/bcs/download_urls")
def create_download_urls(
    request_json: dict = Depends(get_request_json),
    user_doc: dict = Depends(get_user_doc),
    logger: Logger = Depends(get_logger),
):
    remote_paths = request_json["remote_paths"]
    filepaths_are_absolute = all([fp.startswith("/") for fp in remote_paths])
    if not filepaths_are_absolute:
        logger.log(f"Tell this person paths must be absolute?", tell_slack=True)
        raise HTTPException(400, detail="paths must be absolute")

    email = user_doc.get("email").replace("@", "_at_")
    prefixs = [f"{email}{path}" for path in remote_paths]

    uris = []
    for prefix in prefixs:
        blobs = list(storage.Client().bucket(NAS_BUCKET).list_blobs(prefix=prefix))
        uris.extend([f"gs://{NAS_BUCKET}/{blob.name}" for blob in blobs])

    uris_to_urls = create_signed_gcs_urls(uris, method="GET")
    uri_to_path = lambda uri: "/" + "/".join(uri.split("/")[4:])
    paths_to_urls = [(uri_to_path(x["uri"]), x["url"]) for x in uris_to_urls]
    return {"paths_to_urls": paths_to_urls}


@router.post("/v1/bcs/object_info")
def get_object_info(
    request_json: dict = Depends(get_request_json),
    user_doc: dict = Depends(get_user_doc),
):
    remote_path = request_json["remote_path"]
    client = storage.Client()

    prefix = f"{user_doc.get('email').replace('@', '_at_')}{remote_path}"
    prefix = prefix[:-1] if prefix.endswith("/") else prefix

    sub_blobs = list(client.bucket(NAS_BUCKET).list_blobs(prefix=f"{prefix}/"))
    is_file = storage.Blob.from_string(uri=f"gs://{NAS_BUCKET}/{prefix}", client=client).exists()
    is_folder = (len(sub_blobs) > 0) or (remote_path == "/")
    exists = is_file or is_folder

    if not exists:
        return {"exists": False}
    elif is_file:
        return {"type": "file", "exists": True}
    elif is_folder:
        folder_contents = []
        for sub_blob in sub_blobs:
            file_or_folder = sub_blob.name[len(prefix) :].split("/")[1]
            if (not file_or_folder in folder_contents) and (file_or_folder != ""):
                folder_contents.append(file_or_folder)

        return {"type": "folder", "exists": True, "contents_relative_paths": folder_contents}
