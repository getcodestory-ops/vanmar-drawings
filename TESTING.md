# Testing Guide - Procore PDF Merger

This guide covers testing the application both locally and in production.

## Local Testing

### 1. Setup Test Environment

```bash
cd production
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure Test Environment

Create `.env` file:
```ini
PROCORE_CLIENT_ID=your_test_client_id
PROCORE_CLIENT_SECRET=your_test_client_secret
PROCORE_COMPANY_ID=4907
DATABASE_URL=sqlite:///./test_jobs.db
ENVIRONMENT=development
DEBUG=true
SECRET_KEY=test_key_for_development_only
```

### 3. Start Application

```bash
uvicorn app:app --reload --host 0.0.0.0 --port 8000
```

### 4. Run Test Checklist

#### ✅ Authentication Tests

**Test 1: OAuth Flow**
1. Visit `http://localhost:8000`
2. Click "Login with Procore"
3. Authorize application
4. Verify redirect to dashboard
5. Check database has encrypted refresh token:
   ```bash
   sqlite3 test_jobs.db "SELECT * FROM token_store;"
   ```

**Test 2: Token Refresh**
1. Wait 1 hour (or manually expire cookie)
2. Click on any API endpoint
3. Should auto-refresh token
4. Check logs for "Successfully refreshed access token"

**Test 3: Health Check (No Auth Required)**
```bash
curl http://localhost:8000/health
```
Expected: `{"status":"healthy","database":"healthy",...}`

#### ✅ API Tests

**Test 4: Get Projects**
```bash
# With valid cookie from browser
curl http://localhost:8000/api/projects \
  -H "Cookie: access_token=YOUR_TOKEN"
```
Expected: JSON array of projects

**Test 5: Get Disciplines**
```bash
curl "http://localhost:8000/api/disciplines?project_id=12345" \
  -H "Cookie: access_token=YOUR_TOKEN"
```
Expected: `{"area_id": 123, "disciplines": ["Architectural", ...]}`

**Test 6: Rate Limiting**
```bash
# Run this 40 times rapidly
for i in {1..40}; do
  curl http://localhost:8000/api/projects \
    -H "Cookie: access_token=YOUR_TOKEN"
done
```
Expected: After 30 requests, should get `429 Too Many Requests`

#### ✅ Job Execution Tests

**Test 7: Single Project Job**
1. Select a small project (< 10 drawings)
2. Choose 1 discipline
3. Add to queue
4. Click "Run Now"
5. Monitor progress bar (should update every few seconds)
6. Verify completion
7. Download PDF and verify:
   - TOC is present
   - All drawings included
   - Markups are visible (if any exist)

**Test 8: Multi-Project Batch**
1. Queue 3 different projects
2. Click "Run Now"
3. Verify sequential processing
4. Check all 3 PDFs are generated
5. Verify 5-second delay between jobs (check logs)

**Test 9: Large Project (Performance)**
1. Select a project with 50+ drawings
2. Choose discipline with most drawings
3. Run job
4. Measure time (should be < 2 minutes)
5. Check memory usage doesn't exceed 500MB
6. Verify all drawings are included

**Test 10: Error Handling**
1. Create invalid job (wrong project ID):
   ```bash
   curl -X POST http://localhost:8000/api/start_job \
     -H "Content-Type: application/json" \
     -H "Cookie: access_token=YOUR_TOKEN" \
     -d '{"queue":[{"project_id":99999,"project_name":"Test","disciplines":["Test"]}]}'
   ```
2. Job should fail gracefully
3. Check error details in job history
4. Verify app is still responsive

#### ✅ Scheduling Tests

**Test 11: Create Schedule**
1. Build a queue
2. Click "Schedule Recurring"
3. Set time to 2 minutes from now
4. Set day to current day
5. Save
6. Wait 2 minutes
7. Verify job runs automatically
8. Check job history shows "[AUTO]" prefix

**Test 12: Timezone Handling**
1. Change browser timezone (use browser dev tools)
2. Create schedule
3. Verify timezone is correctly saved in database:
   ```bash
   sqlite3 test_jobs.db "SELECT timezone, hour, minute FROM scheduled_jobs;"
   ```

**Test 13: Delete Schedule**
1. Create a schedule
2. Click "DELETE" in Active Automations
3. Verify it's removed from list
4. Verify it doesn't run at scheduled time

#### ✅ Procore Integration Tests

**Test 14: Upload to Procore**
1. Run a job for a project you have access to
2. Wait for completion
3. Log in to Procore
4. Navigate to: Project → Documents → Drawing & Specs → Combined IFC
5. Verify new PDF is uploaded
6. Check filename format: `ProjectName_Merged_DD_MM_YY.pdf`

**Test 15: Archiving**
1. Run same job again for same project
2. Check Combined IFC folder in Procore
3. Verify old PDF was moved to Archive subfolder
4. Verify old PDF has `_archived_TIMESTAMP` suffix
5. Verify new PDF is in main Combined IFC folder

**Test 16: Archive Folder Creation**
1. Delete Archive folder in Procore (if exists)
2. Run a job for that project
3. Verify Archive folder is auto-created
4. Verify archiving works

#### ✅ Dashboard/UI Tests

**Test 17: Progress Tracking**
1. Start a large job
2. Watch progress bar
3. Verify it updates smoothly (0% → 90% → 95% → 100%)
4. Verify status changes: QUEUED → GENERATING → UPLOADING → SUCCESS

**Test 18: Multiple Jobs Display**
1. Queue 3 jobs
2. Run them
3. Verify all 3 show in job history
4. Verify sorting (most recent first)
5. Verify old jobs remain visible

**Test 19: Download Link**
1. Complete a job
2. Click "Download PDF" button in job history
3. Verify PDF downloads
4. Verify it opens correctly

---

## Production Testing

### Pre-Deployment Checklist

- [ ] All local tests pass
- [ ] `.env` file NOT committed to git
- [ ] `render.yaml` configured correctly
- [ ] Procore redirect URI updated for production URL
- [ ] Secret key generated and set
- [ ] Database URL configured

### Post-Deployment Validation

**Test 20: Production Health Check**
```bash
curl https://your-app.onrender.com/health
```
Expected: `{"status":"healthy",...}`

**Test 21: Production OAuth**
1. Visit production URL
2. Complete OAuth flow
3. Verify redirect works
4. Check Render logs for any errors

**Test 22: Production Job Execution**
1. Run a small test job
2. Monitor Render logs
3. Verify completion
4. Check Procore for uploaded file

**Test 23: Production Scheduling**
1. Create a test schedule for 5 minutes from now
2. Wait for execution
3. Check Render logs at scheduled time
4. Verify job ran automatically

**Test 24: Error Monitoring**
1. Intentionally cause an error (invalid project ID)
2. Check Render logs
3. Verify structured JSON logging
4. Verify error is captured properly

**Test 25: Performance Under Load**
1. Queue 5 projects
2. Run them
3. Monitor Render metrics (RAM, CPU)
4. Verify free tier resources are sufficient

**Test 26: Wake from Sleep (Free Tier)**
1. Wait 20 minutes (app should sleep)
2. Visit the URL
3. First request should be slow (~30s)
4. Subsequent requests should be fast
5. Setup UptimeRobot to prevent sleep

---

## Automated Testing (Future)

### Unit Tests (Example)

Create `production/tests/test_core.py`:

```python
import pytest
from core_engine import find_folder_by_name_pattern

def test_find_folder():
    folders = [
        {"name": "Drawing & Specs", "id": 1},
        {"name": "Photos", "id": 2}
    ]
    
    result = find_folder_by_name_pattern(folders, ["drawing", "specs"])
    assert result["id"] == 1
    
    result = find_folder_by_name_pattern(folders, ["combined"])
    assert result is None
```

Run tests:
```bash
pytest production/tests/
```

### Integration Tests (Example)

```python
from fastapi.testclient import TestClient
from app import app

client = TestClient(app)

def test_health_endpoint():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"

def test_auth_required():
    response = client.get("/api/projects")
    assert response.status_code == 401
```

---

## Load Testing

### Using Apache Bench

```bash
# Test health endpoint (100 requests, 10 concurrent)
ab -n 100 -c 10 https://your-app.onrender.com/health

# Test with authentication
ab -n 50 -c 5 -C "access_token=YOUR_TOKEN" \
  https://your-app.onrender.com/api/projects
```

### Expected Results (Free Tier)
- Health endpoint: ~50ms per request
- API endpoints: ~200-500ms per request
- Job execution: ~45-60s for 50 drawings

---

## Troubleshooting Tests

### Test Fails: "No refresh token found"
**Solution:**
- Complete OAuth flow first
- Check token_store table has entry
- Verify encryption/decryption works

### Test Fails: "Rate limit exceeded"
**Solution:**
- This is expected behavior
- Wait 1 minute and try again
- Or restart application

### Test Fails: "Upload failed"
**Solution:**
- Verify Procore permissions
- Check folder structure exists
- Verify company ID is correct
- Check Procore API status

### Test Fails: Job timeout
**Solution:**
- Project might be too large
- Increase `JOB_TIMEOUT` in config
- Check network connectivity
- Verify Procore API isn't down

---

## Continuous Monitoring

### Daily Checks
- [ ] Check health endpoint
- [ ] Verify scheduled jobs ran
- [ ] Review error logs
- [ ] Check disk space usage

### Weekly Checks
- [ ] Review all job history for patterns
- [ ] Check database size
- [ ] Verify backup systems
- [ ] Test OAuth refresh

### Monthly Checks
- [ ] Full regression testing
- [ ] Performance benchmarking
- [ ] Security audit
- [ ] Dependency updates

---

## Test Data Cleanup

After testing:

```bash
# Clear test database
rm test_jobs.db

# Clear output files
rm -rf output/*.pdf
rm -rf temp_job_*

# Clear logs (if file-based)
rm -f *.log
```

---

## Success Criteria

All tests should pass with:
- ✅ No authentication errors
- ✅ All jobs complete successfully
- ✅ Progress tracking works smoothly
- ✅ Uploads to Procore succeed
- ✅ Archiving works correctly
- ✅ Schedules execute on time
- ✅ No memory leaks (< 512MB RAM)
- ✅ Fast response times (< 500ms for API)
- ✅ Proper error handling and logging

---

## Reporting Issues

When reporting bugs, include:
1. Steps to reproduce
2. Expected vs actual behavior
3. Screenshots/logs
4. Environment (local/production)
5. Browser/OS information
6. Timestamp of occurrence

---

**Happy Testing! 🧪**
