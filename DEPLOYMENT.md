# Deployment Guide - Procore PDF Merger

This guide will walk you through deploying the Procore PDF Merger to Render.com.

## Prerequisites

1. **Procore Developer Account**
   - Create an app at https://developers.procore.com/
   - Note your Client ID and Client Secret

2. **Render Account**
   - Sign up at https://render.com/ (free tier available)

3. **Git Repository**
   - Push your code to GitHub, GitLab, or Bitbucket

---

## Step 1: Prepare Your Code

1. **Update Procore OAuth Redirect URI**
   
   Before deploying, you need to know your Render URL. It will be in the format:
   ```
   https://procore-pdf-merger.onrender.com
   ```
   
   Go to your Procore Developer Portal and add this redirect URI:
   ```
   https://your-app-name.onrender.com/callback
   ```

2. **Generate a Secret Key**
   
   For token encryption, generate a Fernet key:
   ```bash
   python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   ```
   
   Save this key - you'll need it in Step 3.

---

## Step 2: Deploy to Render

### Option A: Using render.yaml (Recommended)

1. **Connect Your Repository**
   - Go to https://dashboard.render.com/
   - Click "New +" → "Blueprint"
   - Connect your Git repository
   - Render will automatically detect `render.yaml`

2. **Configure Environment Variables**
   
   Render will prompt you to set these variables:
   - `PROCORE_CLIENT_ID`: Your Procore app client ID
   - `PROCORE_CLIENT_SECRET`: Your Procore app client secret
   - `SECRET_KEY`: The Fernet key you generated
   - `PROCORE_COMPANY_ID`: Your Procore company ID (default: 4907)

3. **Deploy**
   - Click "Apply"
   - Render will create the web service and PostgreSQL database
   - Wait for deployment to complete (5-10 minutes)

### Option B: Manual Setup

1. **Create PostgreSQL Database**
   - Click "New +" → "PostgreSQL"
   - Name: `procore-db`
   - Plan: Free
   - Create database

2. **Create Web Service**
   - Click "New +" → "Web Service"
   - Connect your repository
   - Settings:
     - Name: `procore-pdf-merger`
     - Environment: Python 3
     - Region: Oregon (or closest to you)
     - Build Command: `pip install -r production/requirements.txt`
     - Start Command: `cd production && uvicorn app:app --host 0.0.0.0 --port $PORT`
     - Plan: Free

3. **Configure Environment Variables**
   
   In the web service settings, add:
   ```
   ENVIRONMENT=production
   DATABASE_URL=[Link to your PostgreSQL database]
   PROCORE_CLIENT_ID=your_client_id
   PROCORE_CLIENT_SECRET=your_client_secret
   SECRET_KEY=your_fernet_key
   PROCORE_COMPANY_ID=4907
   ```

4. **Add Persistent Disk**
   - In web service settings → Disks
   - Name: `output-storage`
   - Mount Path: `/opt/render/project/src/production/output`
   - Size: 1 GB
   - Create

5. **Deploy**
   - Click "Manual Deploy" → "Deploy latest commit"

---

## Step 3: Post-Deployment Configuration

### 1. Verify Deployment

Visit your app URL: `https://your-app-name.onrender.com/health`

You should see:
```json
{
  "status": "healthy",
  "database": "healthy",
  "disk_space_mb": 950,
  "environment": "production",
  "version": "1.0.0"
}
```

### 2. First Login

1. Go to `https://your-app-name.onrender.com`
2. Click "Login with Procore"
3. Authorize the application
4. You should be redirected to the dashboard

### 3. Test a Job

1. Select a project
2. Choose disciplines
3. Click "Add to Queue"
4. Click "Run Now"
5. Monitor progress in the job history table

---

## Step 4: Keep Your App Awake (Free Tier)

Render's free tier sleeps after 15 minutes of inactivity. To prevent this:

1. **Use UptimeRobot (Free)**
   - Sign up at https://uptimerobot.com/
   - Add a new monitor:
     - Type: HTTP(s)
     - URL: `https://your-app-name.onrender.com/health`
     - Interval: 5 minutes
   - This will ping your app every 5 minutes, keeping it awake

2. **Alternative: Cron-job.org**
   - Sign up at https://cron-job.org/
   - Create a job to hit your `/health` endpoint every 5 minutes

---

## Step 5: Configure Scheduled Jobs

Once deployed, you can set up automated runs:

1. Go to your dashboard
2. Build a queue of projects and disciplines
3. Click "Schedule Recurring"
4. Select day and time
5. Click "Save Automation"

The app will automatically run these jobs at the specified time.

---

## Monitoring and Maintenance

### View Logs

In Render dashboard:
- Go to your web service
- Click "Logs" tab
- View real-time logs

### Database Management

To access your database:
1. Get connection string from Render dashboard
2. Use a PostgreSQL client like pgAdmin or DBeaver
3. Connect using the credentials

### Backup Data

Export job history:
```sql
\copy jobs TO 'jobs_backup.csv' CSV HEADER;
\copy scheduled_jobs TO 'schedules_backup.csv' CSV HEADER;
```

---

## Troubleshooting

### Issue: "Login Failed"

**Solution:**
1. Verify `PROCORE_CLIENT_ID` and `PROCORE_CLIENT_SECRET` are correct
2. Check that redirect URI in Procore matches your Render URL exactly
3. Ensure your Procore app has the correct permissions

### Issue: "No Projects Found"

**Solution:**
1. Your access token may have expired
2. Go to `/login` to force a fresh login
3. Verify your Procore account has access to projects

### Issue: "Upload Failed (403 Forbidden)"

**Solution:**
1. Check user permissions in Procore
2. User must have "Standard" or "Admin" on Documents tool
3. Verify company ID is correct

### Issue: App is Slow/Timing Out

**Solution:**
1. Free tier has limited resources (512MB RAM)
2. Process fewer drawings at once
3. Consider upgrading to Starter plan ($7/month) for better performance

### Issue: Database Connection Errors

**Solution:**
1. Verify `DATABASE_URL` is set correctly
2. Check PostgreSQL database is running in Render dashboard
3. Restart the web service

---

## Upgrading from Free Tier

For better performance and reliability:

### Render Starter Plan ($7/month per service)
- 1 GB RAM (2x more)
- No sleep on inactivity
- Better CPU allocation
- Background workers support

### PostgreSQL Paid Plan ($7/month)
- 1 GB storage (same as free)
- Better performance
- Daily backups
- Point-in-time recovery

---

## Security Best Practices

1. **Rotate Secrets Regularly**
   - Change `SECRET_KEY` periodically
   - Update Procore client secret if compromised

2. **Monitor Access**
   - Review logs regularly
   - Set up alerts for failed logins

3. **Limit Permissions**
   - Use a dedicated Procore service account
   - Grant minimum required permissions

4. **Enable HTTPS Only**
   - Already enabled by default on Render
   - Never use HTTP in production

---

## Cost Estimate

### Free Tier (Sufficient for most use cases)
- Web service: Free (750 hours/month)
- PostgreSQL: Free (1GB storage)
- Disk storage: Free (1GB)
- **Total: $0/month**

### Paid Tier (Better performance)
- Web service Starter: $7/month
- PostgreSQL: $7/month
- **Total: $14/month**

---

## Support

If you encounter issues:

1. Check the logs in Render dashboard
2. Review the `/health` endpoint for system status
3. Refer to Procore API documentation: https://developers.procore.com/
4. Render documentation: https://render.com/docs

---

## Next Steps

After successful deployment:

1. ✅ Test with a small project first
2. ✅ Set up automated schedules
3. ✅ Configure UptimeRobot to keep app awake
4. ✅ Train users on the system
5. ✅ Monitor performance and adjust as needed

Congratulations! Your Procore PDF Merger is now production-ready! 🎉
