import requests

FABRIC_API = "https://api.fabric.microsoft.com/v1"


def get_lakehouse(token: str, workspace_id: str, lakehouse_id: str) -> dict:
    """
    Returns lakehouse details; usually includes oneLakeFilesPath which is the DFS base URL for Files.
    """
    r = requests.get(
        f"{FABRIC_API}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=60,
    )
    r.raise_for_status()
    return r.json()