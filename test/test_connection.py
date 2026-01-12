import os
from dotenv import load_dotenv

# 1. Load your passwords from the .env file
load_dotenv()
CLIENT_ID = os.getenv('PROCORE_CLIENT_ID')

# 2. Check if the computer can see them
if CLIENT_ID:
    print(f"Success! I found your Client ID: {CLIENT_ID[:5]}...")
else:
    print("Error: Could not find credentials.")