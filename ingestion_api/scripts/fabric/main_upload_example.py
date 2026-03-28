import os
import uuid
from datetime import datetime, timezone

from ingestion_api.scripts.fabric.auth import get_fabric_token
from ingestion_api.scripts.fabric.fabric_items import get_or_create_lakehouse
from ingestion_api.scripts.fabric.fabric_lakehouse import get_lakehouse
from ingestion_api.scripts.fabric.onelake_dfs_upload import upload_bytes_to_onelake_dfs


def main() -> None:
    tenant_id = os.environ["TENANT_ID"]
    client_id = os.environ["CLIENT_ID"]
    client_secret = os.environ["CLIENT_SECRET"]

    workspace_id = os.environ.get("FABRIC_WORKSPACE_ID", "cf28125b-1c17-4afa-8510-a290ceaab5f4")
    lakehouse_name = os.environ.get("FABRIC_LAKEHOUSE_NAME", "lh_retail_sales")

    local_file = os.environ.get("LOCAL_CSV_PATH", "data/online_retail.csv")
    original_filename = os.path.basename(local_file)

    token = get_fabric_token(tenant_id, client_id, client_secret)

    lakehouse_item = get_or_create_lakehouse(token, workspace_id, lakehouse_name)
    lakehouse_id = lakehouse_item["id"]

    lh = get_lakehouse(token, workspace_id, lakehouse_id)
    one_lake_files_path = lh.get("properties", {}).get("oneLakeFilesPath") or lh.get("oneLakeFilesPath")
    if not one_lake_files_path:
        raise RuntimeError("Could not find oneLakeFilesPath in lakehouse response")

    with open(local_file, "rb") as f:
        content = f.read()

    ingestion_id = str(uuid.uuid4())
    ingestion_date = datetime.now(timezone.utc).date().isoformat()

    dest_path = (
        f"{one_lake_files_path}/Files/bronze/online_retail/landing/"
        f"ingestion_date={ingestion_date}/{ingestion_id}__{original_filename}"
    )

    upload_bytes_to_onelake_dfs(token, dest_path, content)
    print(f"Uploaded to: {dest_path}")


if __name__ == "__main__":
    main()