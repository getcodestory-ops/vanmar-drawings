# OAuth Login Troubleshooting Guide

## 🔍 Quick Diagnosis Steps

### Step 1: Check Debug Endpoint
Visit: `http://localhost:8000/debug/config`

This will show you:
- ✅ If all required configuration is set
- ✅ Current redirect URI
- ✅ Database connection status
- ✅ Specific error messages

### Step 2: Check Server Logs
When you try to login, check your terminal/console where uvicorn is running. Look for error messages.

---

## 🐛 Common Issues & Solutions

### Issue 1: "Login Failed" - Generic Error

**Symptoms:**
- Clicking login redirects back with "Login Failed" message
- No specific error shown

**Diagnosis:**
1. Visit `/debug/config` to see configuration status
2. Check server logs for detailed errors
3. Look at the browser's Network tab (F12) to see the actual error

**Solutions:**

#### A. Redirect URI Mismatch (Most Common)
**Problem:** The redirect URI in your Procore app doesn't match what the app is using.

**Fix:**
1. Check what redirect URI your app is using:
   - **Production (Render):** `https://YOUR-APP-NAME.onrender.com/debug/redirect-uri`
   - **Local Development:** `http://localhost:8000/debug/config`
   
   Copy the **exact** `redirect_uri` value shown.

2. Go to Procore Developer Portal:
   - https://developers.procore.com/
   - Select your app
   - Go to "Redirect URIs"
   - **Exactly match** the redirect URI shown in debug config
   - Common mistakes:
     - Missing `http://` or `https://`
     - Wrong port number
     - Trailing slash: `/callback/` vs `/callback`
     - `localhost` vs `127.0.0.1` (use `localhost`)

3. Save in Procore
4. Restart your app
5. Try login again

#### B. Invalid Client ID or Secret
**Problem:** Credentials don't match what's in Procore.

**Fix:**
1. Check your `.env` file:
   ```bash
   cd production
   cat .env
   ```

2. Verify:
   - `PROCORE_CLIENT_ID` matches exactly (no spaces)
   - `PROCORE_CLIENT_SECRET` matches exactly
   - No quotes around values (unless part of the value itself)

3. Restart the server after changing `.env`

#### C. Missing SECRET_KEY
**Problem:** SECRET_KEY not set, can't encrypt tokens.

**Fix:**
1. Generate a key:
   ```bash
   python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   ```

2. Add to `.env`:
   ```ini
   SECRET_KEY=your_generated_key_here
   ```

3. Restart server

---

### Issue 2: "Invalid redirect_uri" Error

**Error Message:**
```
error: invalid_request
error_description: The redirect_uri does not match the registered redirect_uri
```

**Solution:**
This is a redirect URI mismatch. Follow **Solution A** above.

**Common Mistakes:**
- ✅ Correct: `http://localhost:8000/callback`
- ❌ Wrong: `http://127.0.0.1:8000/callback`
- ❌ Wrong: `http://localhost:8000/callback/`
- ❌ Wrong: `localhost:8000/callback`
- ❌ Wrong: `https://localhost:8000/callback` (unless using HTTPS)

---

### Issue 3: "Invalid client" Error

**Error Message:**
```
error: invalid_client
error_description: Client authentication failed
```

**Solution:**
1. Verify `PROCORE_CLIENT_ID` in `.env` matches Procore exactly
2. Verify `PROCORE_CLIENT_SECRET` in `.env` matches Procore exactly
3. Make sure there are no extra spaces or characters
4. Regenerate client secret in Procore if needed
5. Restart server

---

### Issue 4: "Code expired" or Missing Code

**Error Message:**
- "No authorization code received"
- Code expired errors

**Solution:**
1. Authorization codes expire in ~60 seconds
2. Complete the OAuth flow quickly
3. Don't refresh the callback page
4. Try the login flow again from the beginning

---

### Issue 5: Environment Variables Not Loading

**Symptoms:**
- `/debug/config` shows values as not set
- Even though `.env` file exists

**Solution:**

1. **Check .env file location:**
   ```bash
   cd production
   ls -la .env
   ```
   Must be in the `production/` directory

2. **Check .env format:**
   ```ini
   PROCORE_CLIENT_ID=your_value_here
   PROCORE_CLIENT_SECRET=your_value_here
   SECRET_KEY=your_key_here
   ```
   - No spaces around `=`
   - No quotes unless part of value
   - One variable per line

3. **Restart server** after changing `.env`

4. **Verify loading:**
   ```bash
   python3 -c "from dotenv import load_dotenv; import os; load_dotenv(); print(os.getenv('PROCORE_CLIENT_ID'))"
   ```

---

### Issue 6: "Database Error" During Token Storage

**Error Message:**
- `no such column: token_store.refresh_token_encrypted`
- `no such column: token_store.refresh_token`
- Token storage failed
- Database connection errors

**Cause:** Database schema is outdated (from v1.0, needs v2.0 schema)

**Quick Fix (Development - Deletes Old Data):**
```bash
cd production
# Backup if you want to keep old data
cp jobs.db jobs.db.backup
# Delete old database
rm jobs.db
# Restart server - it will create new schema automatically
```

**Proper Fix (Preserves Data):**
```bash
cd production
# Run migration script
python3 migrate_database.py --db jobs.db --backup

# If you get "SECRET_KEY not found" error:
# 1. Generate a key:
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# 2. Add to .env file:
# SECRET_KEY=your_generated_key_here

# 3. Run migration again:
python3 migrate_database.py --db jobs.db --secret-key $(grep SECRET_KEY .env | cut -d= -f2)
```

**Check database file:**
```bash
cd production
ls -la jobs.db
```

**Check permissions:**
```bash
chmod 644 jobs.db
```

---

## 🔧 Step-by-Step Verification Checklist

### Before First Login:

- [ ] `.env` file exists in `production/` directory
- [ ] `PROCORE_CLIENT_ID` is set in `.env`
- [ ] `PROCORE_CLIENT_SECRET` is set in `.env`
- [ ] `SECRET_KEY` is set in `.env` (generate if needed)
- [ ] All dependencies installed: `pip3 install -r requirements.txt`
- [ ] Server restarted after changing `.env`
- [ ] Visit `/debug/config` - all checks should pass

### In Procore Developer Portal:

- [ ] Redirect URI added: `http://localhost:8000/callback`
- [ ] Redirect URI matches exactly (no trailing slash, correct port)
- [ ] Client ID matches `PROCORE_CLIENT_ID` in `.env`
- [ ] Client Secret matches `PROCORE_CLIENT_SECRET` in `.env`
- [ ] App has appropriate permissions enabled

### During Login:

- [ ] Click "Login with Procore"
- [ ] Redirected to Procore login page
- [ ] Successfully log in to Procore
- [ ] Authorize the application
- [ ] Redirected back to `http://localhost:8000/callback`
- [ ] Should see dashboard (not error page)

---

## 📋 Detailed Error Messages (New in v2.0)

The improved error handling now shows specific errors:

### Redirect URI Error
```
HTTP 400 Error:
invalid_request: The redirect_uri does not match...
```

**Fix:** Update redirect URI in Procore Developer Portal

### Client Authentication Error
```
HTTP 401 Error:
invalid_client: Client authentication failed
```

**Fix:** Verify client ID and secret match exactly

### Missing Configuration
```
Configuration Error: PROCORE_CLIENT_ID not set
```

**Fix:** Add to `.env` file and restart

### Token Encryption Error
```
SECRET_KEY is not set. This is required for token encryption.
```

**Fix:** Generate and add SECRET_KEY to `.env`

---

## 🧪 Testing OAuth Flow Manually

### Test 1: Check Configuration
```bash
curl http://localhost:8000/debug/config
```

### Test 2: Verify Redirect URI
1. Check what URI your app uses: `/debug/config`
2. Manually construct OAuth URL:
   ```
   https://login.procore.com/oauth/authorize?response_type=code&client_id=YOUR_CLIENT_ID&redirect_uri=http://localhost:8000/callback
   ```
3. Replace `YOUR_CLIENT_ID` with actual value
4. Open in browser - should redirect to Procore login

### Test 3: Check Procore API
```bash
# Test if Procore API is reachable
curl -I https://api.procore.com
```

---

## 🆘 Still Having Issues?

### Collect Debug Information:

1. **Server Logs:**
   - Copy the full error from terminal
   - Look for stack traces

2. **Browser Console:**
   - Open DevTools (F12)
   - Check Console tab for errors
   - Check Network tab for failed requests

3. **Configuration Check:**
   - Visit `/debug/config`
   - Take a screenshot or copy the output
   - **DO NOT** share actual secret values!

4. **Procore Developer Portal:**
   - Verify redirect URI exactly matches
   - Check app status (active/inactive)
   - Verify permissions

### What to Share (Safely):

✅ Safe to share:
- Error messages (without tokens)
- Configuration status (without secrets)
- Redirect URI values
- HTTP status codes

❌ **NEVER** share:
- Client Secret
- SECRET_KEY
- Access tokens
- Refresh tokens
- Any actual credentials

---

## 📚 Additional Resources

- [Procore OAuth Documentation](https://developers.procore.com/guides/oauth)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Application README](production/README.md)
- [Deployment Guide](DEPLOYMENT.md)

---

## ✅ Quick Fix Checklist

Run through this quickly:

```bash
# 1. Check .env exists
cd production && cat .env

# 2. Generate SECRET_KEY if missing
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# 3. Verify redirect URI
curl http://localhost:8000/debug/config | grep redirect_uri

# 4. Check Procore Developer Portal
# - Redirect URI: http://localhost:8000/callback (exactly)
# - Client ID matches .env
# - Client Secret matches .env

# 5. Restart server
# Ctrl+C to stop, then:
uvicorn app:app --reload
```

---

**Most common fix:** Redirect URI mismatch - double-check it matches exactly! 🔍
