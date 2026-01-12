# Quick Start Guide - 15 Minutes to Production

## ⚡ Fast Track to Deployment

This guide gets you from code to production in 15 minutes.

---

## Step 1: Local Testing (5 minutes)

### Install Dependencies First
```bash
cd production
pip3 install -r requirements.txt
```

### Generate Secret Key
```bash
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```
**Save this output!** You'll need it for both local and production.

**Alternative (if pip install fails):**
```bash
pip3 install cryptography
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### Create .env File
```bash
cd production
cat > .env << EOF
PROCORE_CLIENT_ID=your_client_id_here
PROCORE_CLIENT_SECRET=your_client_secret_here
PROCORE_COMPANY_ID=4907
DATABASE_URL=sqlite:///./jobs.db
ENVIRONMENT=development
SECRET_KEY=paste_your_generated_key_here
EOF
```

### Run the Application
```bash
# Make sure you're in the production directory and dependencies are installed
uvicorn app:app --reload
```

### Test It
1. Open http://localhost:8000
2. Login with Procore
3. Run a small test job
4. If it works, proceed to deployment!

---

## Step 2: Deploy to Render (5 minutes)

### Push to Git
```bash
git init  # if not already a repo
git add .
git commit -m "Production ready v2.0"
git push origin main  # or create new repo on GitHub
```

### Deploy on Render
1. Go to https://dashboard.render.com/ (sign up if needed)
2. Click **"New +"** → **"Blueprint"**
3. Connect your Git repository
4. Render detects `render.yaml` automatically
5. Set these environment variables when prompted:
   - `PROCORE_CLIENT_ID`: (from your Procore app)
   - `PROCORE_CLIENT_SECRET`: (from your Procore app)
   - `SECRET_KEY`: (paste the key you generated)
6. Click **"Apply"**
7. Wait 5-10 minutes for deployment

---

## Step 3: Configure Procore (2 minutes)

### Update Redirect URI
1. Your Render URL: `https://YOUR-APP-NAME.onrender.com`
2. Go to Procore Developer Portal
3. Add redirect URI: `https://YOUR-APP-NAME.onrender.com/callback`
4. Save changes

---

## Step 4: Verify Production (2 minutes)

### Health Check
```bash
curl https://YOUR-APP-NAME.onrender.com/health
```

Expected response:
```json
{
  "status": "healthy",
  "database": "healthy",
  "disk_space_mb": 950,
  "environment": "production"
}
```

### Test Login
1. Visit `https://YOUR-APP-NAME.onrender.com`
2. Click "Login with Procore"
3. Authorize app
4. Should redirect to dashboard

### Test Job
1. Select a small project
2. Choose 1 discipline
3. Run job
4. Wait for completion
5. Verify PDF in Procore

---

## Step 5: Keep It Awake (1 minute)

Free tier sleeps after 15 minutes. Prevent this:

1. Sign up at https://uptimerobot.com (free)
2. Add monitor:
   - Type: HTTP(s)
   - URL: `https://YOUR-APP-NAME.onrender.com/health`
   - Interval: 5 minutes
3. Done!

---

## 🎉 You're Live!

Your app is now:
- ✅ Running 24/7
- ✅ Automatically processing jobs
- ✅ Secured with encryption
- ✅ Optimized for performance
- ✅ Ready for production use

---

## Common Issues

### "Login Failed"
- **Fix:** Double-check redirect URI matches exactly
- **Fix:** Verify client ID and secret are correct

### "No Projects Found"
- **Fix:** Go to `/login` to refresh token
- **Fix:** Verify you have Procore access

### App is Slow
- **Cause:** First request after sleep (free tier)
- **Fix:** Wait 30 seconds, then it's fast
- **Fix:** Setup UptimeRobot (Step 5)

### Database Error
- **Fix:** Check Render logs for details
- **Fix:** Verify DATABASE_URL is set correctly

---

## Next Steps

### Set Up Automation
1. Go to dashboard
2. Build queue of projects
3. Click "Schedule Recurring"
4. Set day/time
5. Done! Runs automatically

### Monitor Performance
- Check `/health` daily
- Review Render logs weekly
- Monitor job history

### Upgrade (Optional)
For better performance:
- Render Starter: $7/month (1GB RAM, no sleep)
- PostgreSQL paid: $7/month (backups, better performance)

---

## Need Help?

1. **Health Check:** `https://YOUR-APP-NAME.onrender.com/health`
2. **Logs:** Render dashboard → Your service → Logs tab
3. **Documentation:** See [DEPLOYMENT.md](DEPLOYMENT.md) for details
4. **Testing:** See [TESTING.md](TESTING.md) for test cases

---

## Cheat Sheet

### Essential Commands
```bash
# Local development
cd production && uvicorn app:app --reload

# Check health
curl https://YOUR-APP-NAME.onrender.com/health

# View logs (Render CLI)
render logs

# Generate new secret key (install cryptography first if needed: pip install cryptography)
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Database backup (local)
cp jobs.db jobs_backup_$(date +%Y%m%d).db
```

### Essential URLs
- **Production App:** `https://YOUR-APP-NAME.onrender.com`
- **Health Check:** `https://YOUR-APP-NAME.onrender.com/health`
- **Force Login:** `https://YOUR-APP-NAME.onrender.com/login`
- **Render Dashboard:** https://dashboard.render.com/
- **Procore Dev Portal:** https://developers.procore.com/

---

**Total Time:** ~15 minutes  
**Cost:** $0 (free tier)  
**Status:** Production Ready  

🚀 **Happy Automating!**
