
import base64
import os
import sys
from pathlib import Path

import requests

# ---------------- CONFIG ----------------
GITLAB_URL = os.environ.get("GITLAB_URL", "https://gitlab.com")
PROJECT_ID = os.environ.get("GITLAB_PROJECT_ID", "")          # e.g. "12345678"
PRIVATE_TOKEN = os.environ.get("GITLAB_TOKEN", "")
BRANCH = os.environ.get("GITLAB_BRANCH", "main")
LOCAL_DIR = os.environ.get("LOCAL_DIR", ".")
COMMIT_MESSAGE = os.environ.get("COMMIT_MESSAGE", "Upload local code via API")

# Folders/files to skip when walking LOCAL_DIR
IGNORE_NAMES = {".git", "__pycache__", ".venv", "node_modules", ".DS_Store"}
# -----------------------------------------


def api_url(path: str) -> str:
    return f"{GITLAB_URL}/api/v4/{path}"


def headers() -> dict:
    return {"PRIVATE-TOKEN": PRIVATE_TOKEN}


def file_exists_in_repo(rel_path: str) -> bool:
    url = api_url(f"projects/{PROJECT_ID}/repository/files/{requests.utils.quote(rel_path, safe='')}")
    resp = requests.get(url, headers=headers(), params={"ref": BRANCH})
    return resp.status_code == 200


def collect_files(local_dir: str):
    base = Path(local_dir).resolve()
    for root, dirs, files in os.walk(base):
        dirs[:] = [d for d in dirs if d not in IGNORE_NAMES]
        for name in files:
            if name in IGNORE_NAMES:
                continue
            full_path = Path(root) / name
            rel_path = full_path.relative_to(base).as_posix()
            yield full_path, rel_path


def build_actions():
    actions = []
    for full_path, rel_path in collect_files(LOCAL_DIR):
        content = base64.b64encode(full_path.read_bytes()).decode("utf-8")
        action = "update" if file_exists_in_repo(rel_path) else "create"
        actions.append(
            {
                "action": action,
                "file_path": rel_path,
                "content": content,
                "encoding": "base64",
            }
        )
        print(f"  {action}: {rel_path}")
    return actions


def commit_actions(actions, chunk_size: int = 90):
    """GitLab limits payload size; commit in chunks if there are many files."""
    url = api_url(f"projects/{PROJECT_ID}/repository/commits")
    for i in range(0, len(actions), chunk_size):
        chunk = actions[i : i + chunk_size]
        payload = {
            "branch": BRANCH,
            "commit_message": f"{COMMIT_MESSAGE} (part {i // chunk_size + 1})"
            if len(actions) > chunk_size
            else COMMIT_MESSAGE,
            "actions": chunk,
        }
        resp = requests.post(url, headers=headers(), json=payload)
        if resp.status_code != 201:
            print(f"Failed to commit chunk starting at index {i}: {resp.status_code} {resp.text}")
            sys.exit(1)
        print(f"Committed {len(chunk)} files -> {resp.json().get('id')}")


def main():
    missing = [n for n, v in [("PROJECT_ID", PROJECT_ID), ("PRIVATE_TOKEN", PRIVATE_TOKEN)] if not v]
    if missing:
        print(f"Missing required config: {', '.join(missing)}")
        print("Set them as environment variables (GITLAB_PROJECT_ID, GITLAB_TOKEN) or edit the script.")
        sys.exit(1)

    print(f"Scanning {LOCAL_DIR} ...")
    actions = build_actions()
    if not actions:
        print("No files found to upload.")
        return

    print(f"Uploading {len(actions)} file(s) to project {PROJECT_ID}, branch '{BRANCH}' ...")
    commit_actions(actions)
    print("Done.")


if __name__ == "__main__":
    main()
