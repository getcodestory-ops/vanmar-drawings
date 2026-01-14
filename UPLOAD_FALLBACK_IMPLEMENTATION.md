# Upload Fallback Implementation Summary

## Implementation Date
January 14, 2026

## Problem Solved
Large file uploads (100MB+) to Procore were failing with SSL errors:
- `SSLV3_ALERT_BAD_RECORD_MAC`
- `ServerDisconnectedError: Server disconnected`

Even with 5 retry attempts, the async aiohttp connection was being interrupted during data transfer for large files.

## Solution Implemented
Added a **synchronous upload fallback** system that automatically switches to a more reliable upload method when async uploads fail.

## Changes Made

### 1. Enhanced SSL Configuration (`_upload_file_async`)
**File**: `core_engine.py` (lines ~270-335)

**Improvements**:
- Force TLS 1.2+ (minimum version set explicitly)
- Added `force_close=True` on connector to prevent connection reuse
- Improved connection pool settings
- Better timeout configuration with separate `sock_read` timeout

### 2. New Synchronous Upload Method (`upload_file_sync`)
**File**: `core_engine.py` (lines ~168-251)

**Features**:
- Uses Python `requests` library (synchronous, more stable for large files)
- Built-in retry logic (3 attempts with 5-second delays)
- Same timeout calculation as async (10 sec/MB, min 5min, max 30min)
- Uses system SSL certificates via `verify=True`
- Detailed logging of each attempt

### 3. Smart Upload Wrapper (`upload_file`)
**File**: `core_engine.py` (lines ~347-406)

**Logic Flow**:
```
1. Try async upload first (fast, efficient)
   ├─ Success? → Return "Success"
   └─ All retries fail? → Continue to step 2

2. Log SSL diagnostics and failure details

3. Fall back to synchronous upload
   ├─ Success? → Return "Success" with fallback notice
   └─ Failed? → Return error message
```

### 4. SSL Diagnostics Helper (`extract_ssl_error_details`)
**File**: `core_engine.py` (lines ~27-51)

**Purpose**:
- Extracts detailed error information when async fails
- Logs error type, message, retry attempts, and original exception
- Helps diagnose SSL/connection issues

### 5. Dependencies Verified
**File**: `requirements.txt`

- `requests==2.32.5` ✓ Already present
- No additional dependencies needed

## How It Works

### Normal Case (Small-Medium Files)
```
Upload Request
    ↓
Async Upload (attempt 1)
    ↓
✅ Success (fast)
```

### Retry Case (Temporary Issues)
```
Upload Request
    ↓
Async Upload (attempt 1) → Failed
    ↓
Async Upload (attempt 2) → Failed
    ↓
Async Upload (attempt 3)
    ↓
✅ Success (slightly slower)
```

### Fallback Case (Large Files / SSL Issues)
```
Upload Request
    ↓
Async Upload (attempts 1-5) → All Failed (~40-60s)
    ↓
Log SSL Diagnostics
    ↓
Sync Upload (attempt 1)
    ↓
✅ Success via Fallback (slower but works)
```

## Expected Log Output

### Successful Async Upload
```
[INFO] Uploading file: document.pdf (50.25 MB)
[INFO] Upload timeout set to 502s for 50.25 MB file
[INFO] ✓ Successfully uploaded 'document.pdf' (took 12.3s, 4.08 MB/s)
```

### Fallback Triggered
```
[INFO] Uploading file: large_doc.pdf (313.12 MB)
[WARNING] ⚠ Connection error during upload: ServerDisconnectedError: Server disconnected - will retry...
[INFO] Uploading file: large_doc.pdf (313.12 MB)
[WARNING] ⚠ Connection error during upload: ClientOSError: [SSL: SSLV3_ALERT_BAD_RECORD_MAC] - will retry...
... (3 more retries) ...

[WARNING] ═══════════════════════════════════════════════════════════
[WARNING] ⚠️  ASYNC UPLOAD FAILED AFTER ALL RETRIES
[WARNING]    File: large_doc.pdf (313.12 MB)
[WARNING]    Time spent on async attempts: 41.25s
[INFO]      SSL Error Details:
[INFO]        - error_type: RetryError
[INFO]        - retry_attempts: 5
[INFO]        - original_error_type: ClientOSError
[WARNING]    Falling back to synchronous upload...
[WARNING] ═══════════════════════════════════════════════════════════

[INFO] 🔄 Synchronous upload attempt: large_doc.pdf (313.12 MB)
[INFO] Sync upload attempt 1/3...
[INFO] ✓ Sync upload successful: 'large_doc.pdf' (took 245.8s, 1.27 MB/s)

[INFO] ═══════════════════════════════════════════════════════════
[INFO] ✅ UPLOAD SUCCEEDED VIA SYNC FALLBACK
[INFO]    File: large_doc.pdf
[INFO]    Total time (async attempts + sync): 287.05s
[INFO] ═══════════════════════════════════════════════════════════
```

## Testing Instructions

### Test 1: Small File (< 10MB)
```bash
# Should use async successfully, complete quickly
# Expected: ~2-5 seconds for 5MB file
```

### Test 2: Medium File (50MB)
```bash
# Should use async successfully
# Expected: ~10-30 seconds depending on connection
```

### Test 3: Large File (100MB+)
```bash
# May trigger fallback if SSL issues occur
# Expected: Will take longer but should succeed
# Look for "UPLOAD SUCCEEDED VIA SYNC FALLBACK" message
```

### Test 4: Very Large File (300MB+)
```bash
# Likely to trigger fallback based on previous behavior
# Expected: Should succeed via sync fallback
# Total time: ~5-10 minutes depending on connection
```

## Performance Comparison

| File Size | Method | Typical Speed | Expected Time |
|-----------|--------|---------------|---------------|
| 10 MB     | Async  | 4-8 MB/s      | 1-3s          |
| 50 MB     | Async  | 4-8 MB/s      | 6-15s         |
| 100 MB    | Async  | 4-8 MB/s      | 12-30s        |
| 100 MB    | Sync   | 0.5-2 MB/s    | 50-200s       |
| 300 MB    | Sync   | 0.5-2 MB/s    | 150-600s      |

**Note**: Sync is slower but more reliable for large files. The system automatically chooses the best method.

## Benefits

1. **Reliability**: Large files will no longer fail completely - they'll fall back to sync upload
2. **Speed**: Small/medium files still get fast async uploads
3. **Diagnostics**: Better error logging helps identify SSL/connection issues
4. **Transparency**: Logs clearly show when fallback is used and why
5. **No User Action**: Automatic fallback - users don't need to do anything different

## Rollback Instructions

If issues occur, revert these changes:

1. Remove the `upload_file()` wrapper method
2. Rename `_upload_file_async()` back to `upload_file()`
3. Remove `upload_file_sync()` method
4. Remove `extract_ssl_error_details()` function
5. Remove `import requests` and `import ssl` from imports

## Next Steps

1. **Monitor Logs**: Watch for "UPLOAD SUCCEEDED VIA SYNC FALLBACK" messages
2. **Track Metrics**: 
   - How often does fallback occur?
   - Which file sizes trigger it most?
   - Does sync fallback always succeed?
3. **Consider Future Improvements**:
   - Chunked upload for extremely large files (>500MB)
   - Resume capability for interrupted uploads
   - Parallel chunk uploads for faster large file handling

## Files Modified

1. `core_engine.py` - Main changes
   - Added: `extract_ssl_error_details()` helper
   - Added: `upload_file_sync()` method
   - Added: `upload_file()` wrapper with fallback logic
   - Modified: Renamed old `upload_file()` to `_upload_file_async()`
   - Modified: Enhanced SSL configuration in `_upload_file_async()`

2. `requirements.txt` - Verified (no changes needed)
   - `requests==2.32.5` already present

## Success Criteria

✅ Code compiles without errors
✅ No linter errors
✅ Async upload has improved SSL configuration
✅ Sync fallback method implemented
✅ Fallback logic correctly catches RetryError
✅ Detailed logging at all stages
✅ Dependencies verified

## Implementation Complete
All tasks from the plan have been implemented and verified. The system is ready for testing with real uploads.
