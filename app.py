import os
from flask import Flask, request, jsonify, send_file, send_from_directory
import boto3
from botocore.exceptions import ClientError
import io

app = Flask(__name__, static_folder="static")

BUCKET_NAME = os.environ.get("S3_SERVICE_K8S_BUCKET_NAME", "")
BUCKET_REGION = os.environ.get("S3_SERVICE_K8S_BUCKET_REGION", "us-east-1")
AWS_ACCESS_KEY_ID = os.environ.get("S3_SERVICE_K8S_AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.environ.get("S3_SERVICE_K8S_AWS_SECRET_ACCESS_KEY", "")


def get_s3_client():
    kwargs = {"region_name": BUCKET_REGION}
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        kwargs["aws_access_key_id"] = AWS_ACCESS_KEY_ID
        kwargs["aws_secret_access_key"] = AWS_SECRET_ACCESS_KEY
    return boto3.client("s3", **kwargs)


@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "bucket_name": BUCKET_NAME,
        "bucket_region": BUCKET_REGION,
        "bucket_arn": os.environ.get("S3_SERVICE_K8S_BUCKET_ARN", ""),
        "connected": BUCKET_NAME != "",
    })


@app.route("/objects", methods=["GET"])
def list_objects():
    if not BUCKET_NAME:
        return jsonify({"error": "BUCKET_NAME not configured"}), 500

    s3 = get_s3_client()
    prefix = request.args.get("prefix", "")

    try:
        params = {"Bucket": BUCKET_NAME}
        if prefix:
            params["Prefix"] = prefix

        response = s3.list_objects_v2(**params)
        objects = []
        for obj in response.get("Contents", []):
            objects.append({
                "key": obj["Key"],
                "size": obj["Size"],
                "last_modified": obj["LastModified"].isoformat(),
            })
        return jsonify({"objects": objects, "count": len(objects)})
    except ClientError as e:
        return jsonify({"error": str(e)}), 500


@app.route("/objects/<path:key>", methods=["PUT"])
def upload_object(key):
    if not BUCKET_NAME:
        return jsonify({"error": "BUCKET_NAME not configured"}), 500

    s3 = get_s3_client()

    try:
        content_type = request.content_type or "application/octet-stream"
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=request.get_data(),
            ContentType=content_type,
        )
        return jsonify({"message": f"Uploaded {key}", "key": key})
    except ClientError as e:
        return jsonify({"error": str(e)}), 500


@app.route("/upload", methods=["POST"])
def upload_file():
    if not BUCKET_NAME:
        return jsonify({"error": "BUCKET_NAME not configured"}), 500

    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    s3 = get_s3_client()
    f = request.files["file"]
    key = request.form.get("key", f.filename)

    try:
        s3.upload_fileobj(
            f,
            BUCKET_NAME,
            key,
            ExtraArgs={"ContentType": f.content_type or "application/octet-stream"},
        )
        return jsonify({"message": f"Uploaded {key}", "key": key})
    except ClientError as e:
        return jsonify({"error": str(e)}), 500


@app.route("/objects/<path:key>", methods=["GET"])
def download_object(key):
    if not BUCKET_NAME:
        return jsonify({"error": "BUCKET_NAME not configured"}), 500

    s3 = get_s3_client()

    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        content = response["Body"].read()
        content_type = response.get("ContentType", "application/octet-stream")

        return send_file(
            io.BytesIO(content),
            mimetype=content_type,
            as_attachment=True,
            download_name=key.split("/")[-1],
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return jsonify({"error": f"Object {key} not found"}), 404
        return jsonify({"error": str(e)}), 500


@app.route("/objects/<path:key>", methods=["DELETE"])
def delete_object(key):
    if not BUCKET_NAME:
        return jsonify({"error": "BUCKET_NAME not configured"}), 500

    s3 = get_s3_client()

    try:
        s3.delete_object(Bucket=BUCKET_NAME, Key=key)
        return jsonify({"message": f"Deleted {key}", "key": key})
    except ClientError as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
