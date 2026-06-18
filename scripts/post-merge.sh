#!/bin/bash
set -e

npm install --legacy-peer-deps
cp server.js live-tv/server.js

echo "==> Pushing to GitHub..."

python3 - <<'EOF'
import json, base64, urllib.request, os, sys

token = os.environ.get('GITHUB_TOKEN', '')
if not token:
    print('GITHUB_TOKEN not set, skipping push.')
    sys.exit(0)

FILES = ['server.js', 'live-tv/server.js', 'Dockerfile', 'scripts/post-merge.sh']

for filepath in FILES:
    if not os.path.exists(filepath):
        continue
    try:
        req = urllib.request.Request(
            f'https://api.github.com/repos/miz-app-builder/Live-TV/contents/{filepath}',
            headers={
                'Authorization': f'Bearer {token}',
                'Accept': 'application/vnd.github.v3+json',
            }
        )
        sha = None
        try:
            with urllib.request.urlopen(req) as resp:
                sha = json.loads(resp.read()).get('sha')
        except:
            pass

        with open(filepath, 'rb') as f:
            content = base64.b64encode(f.read()).decode('utf-8')

        payload = {
            'message': f'chore: auto-update {filepath}',
            'content': content,
            'branch': 'main'
        }
        if sha:
            payload['sha'] = sha

        req2 = urllib.request.Request(
            f'https://api.github.com/repos/miz-app-builder/Live-TV/contents/{filepath}',
            data=json.dumps(payload).encode('utf-8'),
            method='PUT',
            headers={
                'Authorization': f'Bearer {token}',
                'Accept': 'application/vnd.github.v3+json',
                'Content-Type': 'application/json'
            }
        )
        with urllib.request.urlopen(req2) as resp:
            result = json.loads(resp.read())
            commit_sha = result.get('commit', {}).get('sha', '')[:7]
            print(f'Pushed {filepath} → {commit_sha}')
    except Exception as e:
        print(f'Warning: could not push {filepath}: {e}')

print('GitHub push complete.')
EOF

echo "==> Done."
