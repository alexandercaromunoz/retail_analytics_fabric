import requests

FABRIC_API = "https://api.fabric.microsoft.com/v1"


def list_items(token: str, workspace_id: str) -> list[dict]:
    r = requests.get(
        f"{FABRIC_API}/workspaces/{workspace_id}/items",
        headers={"Authorization": f"Bearer {token}"},
        timeout=60,
    )
    r.raise_for_status()
    return r.json().get("value", [])


def create_lakehouse(token: str, workspace_id: str, display_name: str) -> dict:
    payload = {"displayName": display_name, "type": "Lakehouse"}
    r = requests.post(
        f"{FABRIC_API}/workspaces/{workspace_id}/items",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=payload,
        timeout=60,
    )
    r.raise_for_status()
    return r.json()


def get_or_create_lakehouse(token: str, workspace_id: str, display_name: str) -> dict:
    for it in list_items(token, workspace_id):
        if it.get("type") == "Lakehouse" and it.get("displayName") == display_name:
            return it
    return create_lakehouse(token, workspace_id, display_name)