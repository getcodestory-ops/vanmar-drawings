import os
import requests
from dotenv import load_dotenv

# 1. Load credentials
load_dotenv()
# .strip() removes any accidental spaces at the start or end
CLIENT_ID = os.getenv('PROCORE_CLIENT_ID').strip()
CLIENT_SECRET = os.getenv('PROCORE_CLIENT_SECRET').strip()
REDIRECT_URI = "urn:ietf:wg:oauth:2.0:oob"

# 2. Generate the Login URL
base_auth_url = "https://login-sandbox.procore.com/oauth/authorize"
login_url = f"{base_auth_url}?response_type=code&client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}"

print("\n--- STEP 1: LOG IN ---")
print("Click this link (or copy-paste it into your browser):")
print(f"\n{login_url}\n")

# 3. User inputs the code
auth_code = input("STEP 2: After logging in, copy the code from the screen and paste it here: ").strip()

# 4. Exchange code for Access Token
print("\n--- STEP 3: EXCHANGING CODE FOR TOKEN ---")
token_url = "https://login-sandbox.procore.com/oauth/token"

payload = {
    "grant_type": "authorization_code",
    "code": auth_code,
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "redirect_uri": REDIRECT_URI
}

# CHANGE IS HERE: We use 'data=' instead of 'json=' to send form-encoded data
response = requests.post(token_url, data=payload)

if response.status_code == 200:
    # Success!
    with open("tokens.json", "w") as f:
        f.write(response.text)
        
    data = response.json()
    print("\nSUCCESS! WE GOT THE TOKENS!")
    print("-" * 30)
    print(f"Access Token: {data['access_token'][:10]}...") 
    print("-" * 30)
    print("Tokens saved to 'tokens.json'.")
else:
    print("\nFAILED to get token.")
    print(f"Status Code: {response.status_code}")
    print("Error Details:", response.text)