import requests

# --- CONFIGURATION ---
# 1. PASTE YOUR KEYS DIRECTLY INSIDE THE QUOTES BELOW
CLIENT_ID = "sdzd5FAgNCpmYbdN4KSHB2ewVOQPS61c52lHejY7AQg" 
CLIENT_SECRET = "Jp0H9gbKB9Yls3cJ69TMCiaJGA1wN1kVGgqdTxNo5dY"

# 2. ARE WE IN SANDBOX OR PROD?
# Try leaving this as True first. If it fails, change it to False.
IS_SANDBOX = False 

# ---------------------

if IS_SANDBOX:
    BASE_URL = "https://login-sandbox.procore.com"
else:
    BASE_URL = "https://login.procore.com"

REDIRECT_URI = "urn:ietf:wg:oauth:2.0:oob"

print(f"--- DEBUG MODE: Target is {'SANDBOX' if IS_SANDBOX else 'PRODUCTION'} ---")

# A. GENERATE LINK
auth_url = f"{BASE_URL}/oauth/authorize?response_type=code&client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}"
print("\n1. Click/Copy this link to login:")
print(f"\n{auth_url}\n")

# B. INPUT CODE
auth_code = input("2. Paste the code from the browser here: ").strip()

# C. EXCHANGE
token_url = f"{BASE_URL}/oauth/token"
payload = {
    "grant_type": "authorization_code",
    "code": auth_code,
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "redirect_uri": REDIRECT_URI
}

print(f"\n3. Contacting {token_url}...")
response = requests.post(token_url, data=payload)

if response.status_code == 200:
    print("\nSUCCESS! It worked.")
    print(f"Access Token: {response.json()['access_token'][:10]}...")
    # Save it
    with open("tokens.json", "w") as f:
        f.write(response.text)
else:
    print("\nFAILED AGAIN.")
    print(f"Status: {response.status_code}")
    print(f"Response: {response.text}")