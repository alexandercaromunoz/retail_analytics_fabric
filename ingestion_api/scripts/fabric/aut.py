import requests


def get_fabric_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    """
    Client credentials flow (Entra ID) for Microsoft Fabric REST.
    """
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://api.fabric.microsoft.com/.default",
    }
    r = requests.post(url, data=data, timeout=60)
    r.raise_for_status()
    return r.json()["access_token"]