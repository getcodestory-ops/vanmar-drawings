import os
import re
import gc
import time
import json
import shutil
import asyncio
import aiohttp
import pymupdf
import requests
import ssl
import threading
from datetime import datetime
from typing import List, Dict, Any, Optional, Callable
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, RetryError
import logging

from config import settings

logger = logging.getLogger(__name__)

# Optional import for memory tracking
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    logger.warning("psutil not available - memory tracking will be disabled. Install with: pip install psutil")

# Commented out imports - only needed for legacy markup overlay functions
# import math
# from collections import defaultdict
# import statistics
# import itertools


# ---------------------------------------------------------
# MEMORY TRACKING
# ---------------------------------------------------------

def log_memory(label: str = ""):
    """Log current memory usage."""
    if not PSUTIL_AVAILABLE:
        return 0.0
    try:
        process = psutil.Process(os.getpid())
        mem_info = process.memory_info()
        rss_mb = mem_info.rss / (1024 * 1024)  # Resident Set Size in MB
        logger.info(f"          📊 RAM: {rss_mb:.1f} MB {label}")
        return rss_mb
    except Exception as e:
        logger.debug(f"          Could not log memory: {e}")
        return 0.0


class MemoryTracker:
    """Track peak memory usage during a job."""
    
    def __init__(self):
        self.peak_mb = 0
        self.running = False
        self._thread = None
    
    def start(self):
        """Start monitoring memory usage."""
        if not PSUTIL_AVAILABLE:
            return
        self.peak_mb = 0
        self.running = True
        self._thread = threading.Thread(target=self._monitor, daemon=True)
        self._thread.start()
    
    def _monitor(self):
        """Background thread to continuously monitor memory."""
        if not PSUTIL_AVAILABLE:
            return
        process = psutil.Process(os.getpid())
        while self.running:
            try:
                current_mb = process.memory_info().rss / (1024 * 1024)
                if current_mb > self.peak_mb:
                    self.peak_mb = current_mb
            except:
                pass
            time.sleep(0.5)  # Check every 500ms
    
    def stop(self) -> float:
        """Stop monitoring and return peak memory."""
        if not PSUTIL_AVAILABLE:
            return 0.0
        self.running = False
        if self._thread:
            self._thread.join(timeout=1)
        return self.peak_mb
    
    def get_current(self) -> float:
        """Get current memory usage."""
        if not PSUTIL_AVAILABLE:
            return 0.0
        try:
            process = psutil.Process(os.getpid())
            return process.memory_info().rss / (1024 * 1024)
        except:
            return 0.0


# Global tracker instance
memory_tracker = MemoryTracker()


# ---------------------------------------------------------
# RATE LIMIT TRACKER
# ---------------------------------------------------------

class RateLimitTracker:
    """Tracks Procore API rate limits across all requests."""
    
    def __init__(self):
        self.limit: Optional[int] = None  # X-Rate-Limit-Limit
        self.remaining: Optional[int] = None  # X-Rate-Limit-Remaining
        self.reset_time: Optional[int] = None  # X-Rate-Limit-Reset (Unix timestamp)
        self.last_updated: Optional[float] = None  # When we last updated these values
        self._lock = asyncio.Lock()  # Thread-safe updates
    
    def update_from_headers(self, headers: Dict[str, str]):
        """Update rate limit info from response headers."""
        try:
            limit_str = headers.get('X-Rate-Limit-Limit')
            remaining_str = headers.get('X-Rate-Limit-Remaining')
            reset_str = headers.get('X-Rate-Limit-Reset')
            
            if limit_str:
                self.limit = int(limit_str)
            if remaining_str:
                self.remaining = int(remaining_str)
            if reset_str:
                self.reset_time = int(reset_str)
            
            self.last_updated = time.time()
            
            # Log if we're getting low on requests
            if self.remaining is not None and self.limit is not None:
                percentage = (self.remaining / self.limit) * 100
                if percentage < 10:
                    logger.warning(f"⚠️  Rate limit low: {self.remaining}/{self.limit} requests remaining ({percentage:.1f}%)")
                elif percentage < 25:
                    logger.info(f"ℹ️  Rate limit: {self.remaining}/{self.limit} requests remaining ({percentage:.1f}%)")
        except (ValueError, TypeError) as e:
            logger.debug(f"Could not parse rate limit headers: {e}")
    
    async def check_and_wait_if_needed(self):
        """
        Check if we need to wait before making a request.
        Returns True if we waited, False otherwise.
        """
        async with self._lock:
            current_time = time.time()
            
            # If we don't have rate limit info, proceed
            if self.remaining is None or self.reset_time is None:
                return False
            
            # If we have remaining requests, proceed
            if self.remaining > 0:
                return False
            
            # We're out of requests - check if reset time has passed
            if current_time >= self.reset_time:
                # Reset time has passed, but we haven't gotten new headers yet
                # Proceed anyway - the next request will update our info
                logger.info(f"Rate limit reset time passed ({self.reset_time}), proceeding with request")
                return False
            
            # We need to wait until reset time
            wait_seconds = self.reset_time - current_time
            if wait_seconds > 0:
                wait_minutes = wait_seconds / 60
                logger.warning(f"⏸️  Rate limit exhausted ({self.remaining}/{self.limit}). Waiting {wait_seconds:.0f}s ({wait_minutes:.1f} min) until reset at {datetime.fromtimestamp(self.reset_time).strftime('%H:%M:%S')}")
                await asyncio.sleep(wait_seconds)
                return True
            
            return False
    
    def get_status(self) -> Dict[str, Any]:
        """Get current rate limit status for API/UI."""
        current_time = time.time()
        
        status = {
            "limit": self.limit,
            "remaining": self.remaining,
            "reset_time": self.reset_time,
            "last_updated": self.last_updated,
            "is_available": True,
            "wait_seconds": 0,
            "percentage_remaining": None
        }
        
        if self.remaining is not None and self.limit is not None:
            status["percentage_remaining"] = (self.remaining / self.limit) * 100
        
        if self.remaining is not None and self.remaining <= 0 and self.reset_time is not None:
            if current_time < self.reset_time:
                status["is_available"] = False
                status["wait_seconds"] = int(self.reset_time - current_time)
        
        return status


# Global rate limit tracker instance
_rate_limit_tracker = RateLimitTracker()


# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def extract_ssl_error_details(error: Exception) -> dict:
    """Extract detailed SSL error information for diagnostics."""
    details = {
        "error_type": type(error).__name__,
        "error_message": str(error)
    }
    
    # Try to extract retry attempt information
    if hasattr(error, 'last_attempt'):
        try:
            details["retry_attempts"] = error.last_attempt.attempt_number
        except:
            details["retry_attempts"] = "unknown"
    
    # Try to extract the original exception
    if isinstance(error, RetryError):
        try:
            original_error = error.last_attempt.exception()
            details["original_error_type"] = type(original_error).__name__
            details["original_error_message"] = str(original_error)
        except:
            pass
    
    return details


# ---------------------------------------------------------
# SECTION 1: THE PROCORE MANAGER
# ---------------------------------------------------------

class ProcoreDocManager:
    """Manages Procore document operations (upload, move, archive)."""
    
    def __init__(self, company_id: int, project_id: int, access_token: str):
        self.company_id = company_id
        self.project_id = project_id
        self.base_url = settings.PROCORE_BASE_URL
        self.access_token = access_token
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Procore-Company-Id": str(self.company_id),
            "Accept": "application/json"
        }
        self.session = None
        self.ssl_context = self._create_ssl_context()
    
    def _create_ssl_context(self):
        """Create SSL context with proper certificate handling."""
        import ssl
        
        ssl_context = ssl.create_default_context()
        
        # Try to use certifi certificates
        try:
            import certifi
            ssl_context = ssl.create_default_context(cafile=certifi.where())
            logger.info("ProcoreDocManager: Using certifi SSL certificates")
        except ImportError:
            logger.warning("ProcoreDocManager: certifi not available, using system certificates")
        except Exception as ssl_error:
            logger.error(f"ProcoreDocManager: SSL configuration error: {ssl_error}")
            
            # Development fallback: disable SSL verification
            if settings.ENVIRONMENT == "development":
                logger.warning("⚠️  ProcoreDocManager: SSL VERIFICATION DISABLED (DEVELOPMENT ONLY)")
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
            else:
                raise Exception("SSL certificate verification failed. Run: pip install --upgrade certifi")
        
        return ssl_context

    # @retry(
    #     stop=stop_after_attempt(3),
    #     wait=wait_exponential(multiplier=1, min=2, max=10),
    #     retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    # )
    # async def _make_request(self, method: str, url: str, **kwargs):
    #     """Make HTTP request with retry logic."""
    #     if not self.session:
    #         connector = aiohttp.TCPConnector(ssl=self.ssl_context)
    #         self.session = aiohttp.ClientSession(connector=connector)
        
    #     try:
    #         async with self.session.request(method, url, **kwargs) as response:
    #             if response.status == 429:  # Rate limit
    #                 retry_after = int(response.headers.get('Retry-After', 60))
    #                 logger.warning(f"Rate limited. Waiting {retry_after}s")
    #                 await asyncio.sleep(retry_after)
    #                 raise aiohttp.ClientError("Rate limited, retrying")
                
    #             response.raise_for_status()
    #             return await response.json() if response.content_type == 'application/json' else await response.text()
    #     except Exception as e:
    #         logger.error(f"Request failed: {method} {url} - {e}")
    #         raise


    async def _ensure_session(self):
        """
        Creates a persistent aiohttp session if one doesn't exist.
        CRITICAL FIX: Enables Keep-Alive to prevent connection churn/timeouts.
        """
        if not self.session or self.session.closed:
            # Configure TCP Connector for Persistent Connections
            connector = aiohttp.TCPConnector(
                ssl=self.ssl_context,     # Use the SSL context from __init__
                limit=10,                 # Max concurrent connections
                limit_per_host=10,        # Max connections to Procore
                enable_cleanup_closed=True,
                force_close=False,        # <--- FALSE enables Keep-Alive (Fixes timeouts)
                keepalive_timeout=60,     # Keep connection open for 60s
                ttl_dns_cache=300         # Cache DNS to save lookups
            )
            
            # Standard timeout for general API calls
            # (We override this for uploads specifically in the upload method)
            timeout = aiohttp.ClientTimeout(total=settings.DOWNLOAD_TIMEOUT, connect=60)
            
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector
            )
            logger.info("ProcoreDocManager: Created new persistent ClientSession")


    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def _make_request(self, method: str, url: str, **kwargs):
        """
        Make HTTP request reusing the persistent session to prevent connection churn.
        """
        # 1. Ensure we use the shared persistent session
        await self._ensure_session()
        
        try:
            # 2. Use self.session directly
            async with self.session.request(method, url, **kwargs) as response:
                
                # Handle Rate Limiting (429)
                if response.status == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limited. Waiting {retry_after}s")
                    await asyncio.sleep(retry_after)
                    # Raise exception to trigger the @retry decorator
                    raise aiohttp.ClientError("Rate limited, retrying")
                
                # Check for standard HTTP errors
                response.raise_for_status()
                
                # Return JSON or Text based on content type
                if 'application/json' in response.headers.get('Content-Type', ''):
                    return await response.json()
                return await response.text()
                
        except Exception as e:
            logger.error(f"Request failed: {method} {url} - {type(e).__name__}: {e}")
            raise

    async def get_all_files_in_folder(self, folder_id: int) -> List[Dict]:
        """Returns a list of ALL files in a folder."""
        endpoint = f"{self.base_url}/rest/v1.0/folders/{folder_id}"
        params = {"project_id": self.project_id}
        
        try:
            data = await self._make_request('GET', endpoint, headers=self.headers, params=params)
            return data.get('files', []) if isinstance(data, dict) else []
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                logger.warning(f"Folder {folder_id} not found (404)")
                return []
            logger.error(f"Could not list files (Status {e.status})")
            return []
        except Exception as e:
            logger.error(f"Error listing files: {e}")
            return []

    async def move_and_rename_file(self, file_id: int, current_name: str, destination_folder_id: int) -> bool:
        """Renames a file (adds creation date) and moves it to Archive."""
        endpoint = f"{self.base_url}/rest/v1.0/files/{file_id}"
        params = {"project_id": self.project_id}
        
        # Get file metadata to retrieve creation date
        file_created_date = None
        try:
            file_data = await self._make_request('GET', endpoint, headers=self.headers, params=params)
            # Try different possible date fields from Procore API
            if isinstance(file_data, dict):
                created_at = file_data.get('created_at') or file_data.get('created_at_date') or file_data.get('uploaded_at')
                if created_at:
                    # Parse the date string from Procore (format may vary)
                    try:
                        # Handle ISO format with timezone (e.g., "2026-01-15T10:30:00Z" or "2026-01-15T10:30:00+00:00")
                        if isinstance(created_at, str) and 'T' in created_at:
                            # Remove timezone info and parse
                            date_str = created_at.split('T')[0] if 'T' in created_at else created_at
                            dt = datetime.strptime(date_str, '%Y-%m-%d')
                        elif isinstance(created_at, str):
                            # Try direct date format
                            dt = datetime.strptime(created_at.split()[0], '%Y-%m-%d')
                        else:
                            # If it's already a datetime object or timestamp
                            dt = created_at if isinstance(created_at, datetime) else datetime.fromtimestamp(created_at)
                        file_created_date = dt.strftime("%d_%m_%y")
                        logger.debug(f"Extracted creation date: {file_created_date} from {created_at}")
                    except (ValueError, AttributeError, TypeError) as parse_error:
                        logger.debug(f"Could not parse creation date '{created_at}': {parse_error}")
        except Exception as e:
            logger.debug(f"Could not fetch file metadata for creation date: {e}")
        
        # If we couldn't get creation date, use current date as fallback
        # (This should rarely happen, but ensures the function still works)
        if not file_created_date:
            file_created_date = datetime.now().strftime("%d_%m_%y")
            logger.debug(f"Using current date as fallback for file creation date")
        
        # Generate timestamp for archive time to prevent duplicates
        # Format: HHMMSS (24-hour format, no colons)
        archive_timestamp = datetime.now().strftime("%H%M%S")
        
        name_part, ext_part = os.path.splitext(current_name)
        clean_name = name_part.replace(" old", "").replace("_Merged", "")  # Remove _Merged if present
        new_name = f"{clean_name}_Merged_{file_created_date}_archived_{archive_timestamp}{ext_part}"

        payload = {"file": {"name": new_name, "parent_id": destination_folder_id}}
        headers = self.headers.copy()
        headers["Content-Type"] = "application/json"

        try:
            await self._make_request('PATCH', endpoint, headers=headers, params=params, json=payload)
            logger.info(f"Archived: {current_name} -> {new_name}")
            return True
        except Exception as e:
            logger.error(f"Error archiving file {current_name}: {e}")
            return False

    def upload_file_sync(self, local_file_path: str, target_folder_id: int) -> str:
        """
        Synchronous streaming upload using requests library.
        Uses a file generator to avoid loading entire file into memory.
        Critical for low-RAM environments like Render.com.
        """
        from requests_toolbelt import MultipartEncoder, MultipartEncoderMonitor
        
        endpoint = f"{self.base_url}/rest/v1.0/files"
        params = {"project_id": self.project_id}
        file_name = os.path.basename(local_file_path)

        # Get file size for logging
        file_size_mb = os.path.getsize(local_file_path) / (1024 * 1024)
        logger.info(f"         🔄 Streaming upload: {file_name} ({file_size_mb:.2f} MB)")
        
        # Calculate timeout (same as async: 10 sec/MB, min 5min, max 30min)
        upload_timeout = min(max(300, int(file_size_mb * 10)), 1800)
        
        # Retry logic for sync upload (3 attempts with 5s delay)
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"         Streaming upload attempt {attempt}/{max_retries}...")
                upload_start = time.time()
                
                # Use streaming multipart encoder to avoid loading file into memory
                # The file is read in chunks as it's being uploaded
                with open(local_file_path, 'rb') as f:
                    encoder = MultipartEncoder(
                        fields={
                            'file[parent_id]': str(target_folder_id),
                            'file[name]': file_name,
                        'file[data]': (file_name, f, 'application/pdf')
                    }
                    )
                    
                    # Create headers with content type from encoder
                    upload_headers = self.headers.copy()
                    upload_headers['Content-Type'] = encoder.content_type
                    
                    # Make streaming POST request
                    response = requests.post(
                        endpoint,
                        headers=upload_headers,
                        params=params,
                        data=encoder,  # Streams the file in chunks
                        timeout=upload_timeout,
                        verify=True
                    )
                
                if response.status_code in [200, 201]:
                    upload_time = time.time() - upload_start
                    speed = file_size_mb / upload_time if upload_time > 0 else 0
                    logger.info(f"         ✓ Streaming upload successful: '{file_name}' (took {upload_time:.2f}s, {speed:.2f} MB/s)")
                    return "Success"
                else:
                    error_msg = f"Error {response.status_code}: {response.text[:200]}"
                    logger.warning(f"         ⚠ Streaming upload attempt {attempt} failed: {error_msg}")
                    
                    if attempt < max_retries:
                        time.sleep(5)
                        continue
                    else:
                        logger.error(f"         ✗ Streaming upload failed after {max_retries} attempts: {error_msg}")
                        return error_msg
                        
            except requests.exceptions.Timeout as e:
                error_msg = f"Streaming upload timeout after {upload_timeout}s"
                logger.warning(f"         ⚠ {error_msg} (attempt {attempt}/{max_retries})")
                if attempt < max_retries:
                    time.sleep(5)
                    continue
                else:
                    logger.error(f"         ✗ {error_msg}")
                    return error_msg
                    
            except ImportError:
                # Fallback if requests_toolbelt is not installed
                logger.warning(f"         ⚠ requests_toolbelt not available, using standard upload")
                return self._upload_file_sync_fallback(local_file_path, target_folder_id)
                    
            except Exception as e:
                error_msg = f"Streaming upload error: {type(e).__name__}: {str(e)}"
                logger.warning(f"         ⚠ {error_msg} (attempt {attempt}/{max_retries})")
                if attempt < max_retries:
                    time.sleep(5)
                    continue
                else:
                    logger.error(f"         ✗ {error_msg}")
                    return error_msg
        
        return "Streaming upload failed after all retries"
    
    def _upload_file_sync_fallback(self, local_file_path: str, target_folder_id: int) -> str:
        """
        Fallback upload if requests_toolbelt is not available.
        Less memory-efficient but still functional.
        """
        endpoint = f"{self.base_url}/rest/v1.0/files"
        params = {"project_id": self.project_id}
        file_name = os.path.basename(local_file_path)
        file_size_mb = os.path.getsize(local_file_path) / (1024 * 1024)
        upload_timeout = min(max(300, int(file_size_mb * 10)), 1800)
        
        try:
            with open(local_file_path, 'rb') as f:
                files = {'file[data]': (file_name, f, 'application/pdf')}
                data = {'file[parent_id]': str(target_folder_id), 'file[name]': file_name}
                
                response = requests.post(
                    endpoint,
                    headers=self.headers,
                    params=params,
                    files=files,
                    data=data,
                    timeout=upload_timeout,
                    verify=True
                )
            
            if response.status_code in [200, 201]:
                return "Success"
            return f"Error {response.status_code}: {response.text[:200]}"
        except Exception as e:
            return f"Fallback upload error: {type(e).__name__}: {str(e)}"



    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=5, max=60),
        retry=retry_if_exception_type((aiohttp.ServerDisconnectedError, aiohttp.ClientError, asyncio.TimeoutError, BrokenPipeError, OSError))
    )
    async def _upload_file_async(self, local_file_path: str, target_folder_id: int) -> str:
        """
        Async upload reusing the persistent session to avoid Handshake Storms.
        """
        # 1. Ensure we have a valid persistent session
        await self._ensure_session()
        
        endpoint = f"{self.base_url}/rest/v1.0/files"
        params = {"project_id": self.project_id}
        file_name = os.path.basename(local_file_path)
        file_size_mb = os.path.getsize(local_file_path) / (1024 * 1024)
        
        # 2. Calculate dynamic timeout based on file size (10s per MB, min 60s)
        # This prevents "ReadTimeout" on slow networks while keeping it tight for small files
        custom_timeout_val = max(60, int(file_size_mb * 10))
        
        # sock_read is the crucial timeout here - it waits for data chunks
        req_timeout = aiohttp.ClientTimeout(total=custom_timeout_val * 2, sock_read=custom_timeout_val, connect=30)

        logger.info(f"          Uploading: {file_name} ({file_size_mb:.2f} MB)")
        upload_start = time.time()

        try:
            with open(local_file_path, 'rb') as f:
                data = aiohttp.FormData()
                data.add_field('file[data]', f, filename=file_name, content_type='application/pdf')
                data.add_field('file[parent_id]', str(target_folder_id))
                data.add_field('file[name]', file_name)
                
                # 3. Perform Upload using the existing persistent session
                async with self.session.post(
                    endpoint, 
                    headers=self.headers, 
                    params=params, 
                    data=data,
                    timeout=req_timeout # Apply the specific timeout here
                ) as response:
                    
                    if response.status not in [200, 201]:
                        text = await response.text()
                        # If it's a temporary server error (Gateway/RateLimit), raise to trigger retry
                        if response.status in [429, 502, 503, 504]:
                            response.raise_for_status()
                        
                        error_msg = f"Error {response.status}: {text[:200]}"
                        logger.error(f"          ✗ Upload failed: {error_msg}")
                        return error_msg
                    
                    upload_time = time.time() - upload_start
                    logger.info(f"          ✓ Uploaded '{file_name}' in {upload_time:.2f}s")
                    return "Success"

        except asyncio.TimeoutError:
            logger.warning(f"          ⚠ Timeout uploading {file_name} - Retrying...")
            raise # Let tenacity retry
        except Exception as e:
            # Let tenacity handle connection errors, but log others
            if isinstance(e, (aiohttp.ClientError, OSError)):
                raise
            return f"Upload Error: {str(e)}"



    async def upload_file(self, local_file_path: str, target_folder_id: int) -> str:
        """
        Smart Upload: 
        - Uses Standard Upload for files < 30MB (reduced for low-memory environments)
        - Uses Segmented (S3) Upload for files >= 30MB (Prevents timeouts and memory spikes)
        """
        file_name = os.path.basename(local_file_path)
        file_size_mb = os.path.getsize(local_file_path) / (1024 * 1024)
        
        # 30MB threshold (reduced from 60MB) - segmented upload is more memory-efficient
        # This helps prevent OOM on low-RAM environments like Render.com
        if file_size_mb >= 30:
            logger.info(f"          📦 Large file ({file_size_mb:.2f} MB). Switching to Segmented S3 Upload.")
            return await self.upload_large_file_segmented(local_file_path, target_folder_id)
        
        # Standard flow for small files
        try:
            return await self._upload_file_async(local_file_path, target_folder_id)
        except RetryError as e:
            # Fallback to sync if async fails (existing logic)
            # ... (keep your existing error handling here) ...
            return self.upload_file_sync(local_file_path, target_folder_id)
        except Exception as e:
            return f"Error: {str(e)}"

    async def process_file_update(self, file_path: str, target_folder_id: int, archive_folder_id: Optional[int]) -> str:
        """Clean up existing files and upload new one."""
        upload_start = time.time()
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        logger.info(f"═══════════════════════════════════════════════════════════")
        logger.info(f"📤 STARTING PROCORE UPLOAD PROCESS")
        logger.info(f"   Target folder ID: {target_folder_id}")
        logger.info(f"   Archive folder ID: {archive_folder_id}")
        logger.info(f"   File: {os.path.basename(file_path)}")
        logger.info(f"   File size: {file_size_mb:.2f} MB")
        logger.info(f"═══════════════════════════════════════════════════════════")

        # 1. Clean Sweep: Move ALL existing files in target -> Archive
        logger.info(f"[UPLOAD STEP 1/2] Checking for existing files to archive...")
        check_start = time.time()
        existing_files = await self.get_all_files_in_folder(target_folder_id)
        check_time = time.time() - check_start
        
        if existing_files:
            logger.info(f"         Found {len(existing_files)} file(s) to archive (took {check_time:.2f}s)")
            
            # FAILSAFE: If Archive ID is missing, try to CREATE it
            if not archive_folder_id:
                logger.warning("         Archive folder not found. Creating 'Archive' folder...")
                archive_folder_id = await self._create_archive_folder(target_folder_id)
                if archive_folder_id:
                    logger.info(f"         ✓ Created Archive folder (ID: {archive_folder_id})")

            if archive_folder_id:
                logger.info(f"         Archiving {len(existing_files)} file(s)...")
                archive_start = time.time()
                # Archive files concurrently
                archive_tasks = [
                    self.move_and_rename_file(f['id'], f['name'], archive_folder_id)
                    for f in existing_files
                ]
                results = await asyncio.gather(*archive_tasks, return_exceptions=True)
                archive_time = time.time() - archive_start
                
                successful = sum(1 for r in results if r is True)
                failed = len(results) - successful
                logger.info(f"         ✓ Archived {successful}/{len(existing_files)} files (took {archive_time:.2f}s)")
                if failed > 0:
                    logger.warning(f"         ⚠ {failed} file(s) failed to archive")
            else:
                logger.error("         ✗ Critical: Could not find or create Archive folder. Old files will remain.")
        else:
            logger.info(f"         ✓ Target folder is empty. No archiving needed (took {check_time:.2f}s)")

        # 2. Upload New
        logger.info(f"[UPLOAD STEP 2/2] Uploading new file to Procore...")
        upload_file_start = time.time()
        result = await self.upload_file(file_path, target_folder_id)
        upload_file_time = time.time() - upload_file_start
        
        total_upload_time = time.time() - upload_start
        if result == "Success":
            logger.info(f"         ✓ Upload successful (took {upload_file_time:.2f}s)")
            logger.info(f"═══════════════════════════════════════════════════════════")
            logger.info(f"✅ UPLOAD COMPLETED SUCCESSFULLY")
            logger.info(f"   Total time: {total_upload_time:.2f}s")
            logger.info(f"═══════════════════════════════════════════════════════════")
        else:
            logger.error(f"         ✗ Upload failed: {result}")
            logger.error(f"═══════════════════════════════════════════════════════════")
            logger.error(f"❌ UPLOAD FAILED after {total_upload_time:.2f}s")
            logger.error(f"   Error: {result}")
            logger.error(f"═══════════════════════════════════════════════════════════")
        
        return result

    async def _create_archive_folder(self, parent_folder_id: int) -> Optional[int]:
        """Create Archive subfolder."""
        try:
            # Correct endpoint: /rest/v1.0/folders with project_id as query param
            create_url = f"{self.base_url}/rest/v1.0/folders"
            create_data = {"folder": {"name": "Archive", "parent_id": parent_folder_id}}
            create_params = {"project_id": self.project_id}
            create_head = self.headers.copy()
            create_head["Content-Type"] = "application/json"
            
            data = await self._make_request('POST', create_url, headers=create_head, params=create_params, json=create_data)
            archive_id = data.get('id')
            logger.info(f"Created Archive Folder: {archive_id}")
            return archive_id
        except Exception as e:
            logger.debug(f"Could not create Archive folder: {e}")
            return None

    async def upload_large_file_segmented(self, local_file_path: str, target_folder_id: int) -> str:
        """
        Handles large file uploads (>100MB) using Procore's Segmented Upload (S3) flow.
        """
        file_name = os.path.basename(local_file_path)
        file_size = os.path.getsize(local_file_path)
        
        logger.info(f"          🔄 Starting Segmented Upload for {file_name} ({file_size / (1024*1024):.2f} MB)...")

        # STEP 1: Initialize Upload (Get UUID and S3 URL)
        await self._ensure_session()
        init_url = f"{self.base_url}/rest/v1.0/companies/{self.company_id}/uploads"
        
        # Add Content-Type header for JSON
        init_headers = self.headers.copy()
        init_headers["Content-Type"] = "application/json"
        
        payload = {
            "upload": {
            "name": file_name,
            "size": file_size,
                "content_type": "application/pdf"
            }
        }
        
        try:
            # Initialize
            async with self.session.post(init_url, headers=init_headers, json=payload) as response:
                if response.status == 400:
                    error_text = await response.text()
                    logger.error(f"          Init error details: {error_text}")
                response.raise_for_status()
                init_data = await response.json()
            
            upload_uuid = init_data.get("uuid")
            s3_url = init_data.get("url")
            s3_fields = init_data.get("fields", {})
            
            if not upload_uuid or not s3_url:
                raise Exception("Failed to get upload UUID or S3 URL from Procore")

            logger.info(f"          ✓ Step 1: Init complete (UUID: {upload_uuid})")

            # STEP 2: Upload to S3
            # S3 requires specific form fields in strict order
            logger.info(f"          ⏳ Step 2: Uploading binary to S3...")
            
            upload_start = time.time()
            form = aiohttp.FormData()
            
            # Add AWS fields first
            for key, value in s3_fields.items():
                form.add_field(key, value)
            
            # Add file last
            with open(local_file_path, 'rb') as f:
                form.add_field('file', f, filename=file_name, content_type='application/pdf')
                
                # Use a specific long timeout for the S3 transfer (1 hour)
                s3_timeout = aiohttp.ClientTimeout(total=3600, connect=60)
                
                # Direct post to S3 (no Procore headers)
                async with aiohttp.ClientSession(timeout=s3_timeout) as s3_session:
                    async with s3_session.post(s3_url, data=form) as s3_resp:
                        if s3_resp.status not in [200, 201, 204]:
                            text = await s3_resp.text()
                            raise Exception(f"S3 Error {s3_resp.status}: {text}")
                            
            logger.info(f"          ✓ Step 2: S3 Upload complete (took {time.time() - upload_start:.2f}s)")

            # STEP 3: Finalize in Procore
            logger.info(f"          ⏳ Step 3: Finalizing...")
            finalize_url = f"{self.base_url}/rest/v1.0/files"
            finalize_params = {"project_id": self.project_id}
            
            finalize_payload = {
                "file": {
                    "upload_uuid": upload_uuid,
                    "parent_id": target_folder_id,
                    "name": file_name
                }
            }
            
            # Add Content-Type header for JSON payload
            finalize_headers = self.headers.copy()
            finalize_headers["Content-Type"] = "application/json"
            
            # Use standard _make_request for finalization with project_id param
            await self._make_request('POST', finalize_url, headers=finalize_headers, params=finalize_params, json=finalize_payload)
            logger.info(f"          ✓ Step 3: File created successfully")
            
            return "Success"

        except Exception as e:
            logger.error(f"          ✗ Segmented upload failed: {str(e)}")
            return f"Segmented Upload Error: {str(e)}"

    async def close(self):
        """Close the session."""
        if self.session:
            await self.session.close()


# ---------------------------------------------------------
# SECTION 2: ASYNC PROCORE API CLIENT
# ---------------------------------------------------------



class AsyncProcoreClient:
    """Async client for Procore API calls with persistent connections."""
    
    def __init__(self, access_token: str, company_id: int):
        self.access_token = access_token
        self.company_id = company_id
        self.base_url = settings.PROCORE_BASE_URL
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Procore-Company-Id": str(company_id),
            "Accept": "application/json"
        }
        self.session: Optional[aiohttp.ClientSession] = None
        # FIX: Initialize the SSL context here so _ensure_session can use it
        self.ssl_context = self._create_ssl_context()
    
    def _create_ssl_context(self):
        """Create SSL context with proper certificate handling."""
        import ssl

        ssl_context = ssl.create_default_context()

        # Try to use certifi certificates
        try:
            import certifi
            ssl_context = ssl.create_default_context(cafile=certifi.where())
            logger.info("ProcoreDocManager: Using certifi SSL certificates")
        except ImportError:
            logger.warning("ProcoreDocManager: certifi not available, using system certificates")
        except Exception as ssl_error:
            logger.error(f"ProcoreDocManager: SSL configuration error: {ssl_error}")

            # Development fallback: disable SSL verification
            if settings.ENVIRONMENT == "development":
                logger.warning("⚠️  ProcoreDocManager: SSL VERIFICATION DISABLED (DEVELOPMENT ONLY)")
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
            else:
                raise Exception("SSL certificate verification failed. Run: pip install --upgrade certifi")

        return ssl_context

    async def _ensure_session(self):
        """Ensure persistent session is created with Keep-Alive."""
        if not self.session or self.session.closed:
            # Configure TCP Connector for Persistent Connections
            connector = aiohttp.TCPConnector(
                ssl=self.ssl_context,     # Now this attribute exists!
                limit=10,
                limit_per_host=10,
                enable_cleanup_closed=True,
                force_close=False,        # Keep-Alive enabled
                keepalive_timeout=60,
                ttl_dns_cache=300
            )
            
            # Standard timeout for API calls
            timeout = aiohttp.ClientTimeout(total=settings.DOWNLOAD_TIMEOUT, connect=60)
            
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector
            )
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def get(self, endpoint: str, params: Optional[Dict] = None) -> Any:
        """Make GET request with retry logic and connection reuse."""
        await self._ensure_session()
        url = f"{self.base_url}{endpoint}"
        
        # Rate limit check
        waited = await _rate_limit_tracker.check_and_wait_if_needed()
        if waited:
            logger.info(f"          Resumed after rate limit wait")
        
        try:
            async with self.session.get(url, headers=self.headers, params=params) as response:
                # Update tracker
                _rate_limit_tracker.update_from_headers(dict(response.headers))
                
                if response.status == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limited. Waiting {retry_after}s")
                    await asyncio.sleep(retry_after)
                    raise aiohttp.ClientError("Rate limited")
                
                if response.status in [401, 403]:
                    text = await response.text()
                    raise aiohttp.ClientResponseError(
                        response.request_info, response.history,
                        status=response.status, message=text
                    )
                
                response.raise_for_status()
                return await response.json()
                
        except Exception as e:
            logger.error(f"API GET failed: {endpoint} - {str(e)}")
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def post(self, endpoint: str, json: Optional[Dict] = None, params: Optional[Dict] = None) -> Any:
        """Make POST request with retry logic and connection reuse."""
        await self._ensure_session()
        url = f"{self.base_url}{endpoint}"
        
        # Rate limit check
        await _rate_limit_tracker.check_and_wait_if_needed()
        
        try:
            headers = self.headers.copy()
            headers["Content-Type"] = "application/json"
            
            async with self.session.post(url, headers=headers, json=json, params=params) as response:
                _rate_limit_tracker.update_from_headers(dict(response.headers))
                
                if response.status == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    await asyncio.sleep(retry_after)
                    raise aiohttp.ClientError("Rate limited")
                
                response.raise_for_status()
                return await response.json()
                    
        except Exception as e:
            logger.error(f"API POST failed: {endpoint} - {str(e)}")
            raise
    
    async def generate_pdf_with_markups(self, project_id: int, revision_id: int, layer_ids: List[int]) -> str:
        """Generate and download a PDF with markups."""
        endpoint = f"/rest/v1.0/projects/{project_id}/drawing_revisions/{revision_id}/pdf_download_pages"
        payload = {
            "pdf_download_page": {
                "markup_layer_ids": layer_ids,
                "filtered_types": [],
                "filtered_items": {"GenericToolItem": {"generic_tool_id": []}}
            }
        }
        
        logger.info(f"          Requesting PDF (rev {revision_id})...")
        response_data = await self.post(endpoint, json=payload)
        return response_data.get('url')
    
    async def download_file(self, url: str, destination: str) -> bool:
        """Download file using the persistent session."""
        await self._ensure_session()
        
        try:
            async with self.session.get(url) as response:
                response.raise_for_status()
                with open(destination, 'wb') as f:
                    while True:
                        chunk = await response.content.read(8192)
                        if not chunk: break
                        f.write(chunk)
                return True
        except Exception as e:
            logger.error(f"          ✗ Download failed: {str(e)}")
            return False
    
    async def close(self):
        if self.session:
            await self.session.close()

# ---------------------------------------------------------
# SECTION 3: FOLDER FINDERS
# ---------------------------------------------------------

def find_folder_by_name_pattern(folders_list: List[Dict], search_terms: List[str]) -> Optional[Dict]:
    """Find folder matching all search terms."""
    search_terms = [term.lower() for term in search_terms]
    for folder in folders_list:
        folder_name = folder.get('name', '').lower()
        if all(term in folder_name for term in search_terms):
            return folder
    return None


async def get_target_combined_folder(project_id: int, access_token: str) -> tuple[Optional[int], Optional[int]]:
    """Find Combined IFC and Archive folder IDs."""
    client = AsyncProcoreClient(access_token, settings.PROCORE_COMPANY_ID)
    
    try:
        # 1. Get Root folders
        # Note: Procore API uses /rest/v1.0/folders with project_id as query param, not /projects/{id}/folders
        data = await client.get("/rest/v1.0/folders", params={"project_id": project_id})
        root_folders = data if isinstance(data, list) else data.get('folders', [])

        # 2. Find Drawing & Specs
        drawing_specs = find_folder_by_name_pattern(root_folders, ["drawing", "specs"])
        if not drawing_specs:
            return None, None

        # 3. Find Combined IFC
        data = await client.get(f"/rest/v1.0/folders/{drawing_specs['id']}", params={"project_id": project_id})
        subfolders = data.get('folders', []) if isinstance(data, dict) else []

        combined_ifc = find_folder_by_name_pattern(subfolders, ["combined", "ifc"])
        if not combined_ifc:
            return None, None
        
        # 4. Find Archive
        data_c = await client.get(f"/rest/v1.0/folders/{combined_ifc['id']}", params={"project_id": project_id})
        comb_subs = data_c.get('folders', []) if isinstance(data_c, dict) else []
        
        archive = find_folder_by_name_pattern(comb_subs, ["archive"])
        archive_id = archive['id'] if archive else None
        
        return combined_ifc['id'], archive_id
    except Exception as e:
        logger.error(f"Error finding folders: {e}")
        return None, None
    finally:
        await client.close()


async def handle_procore_upload(project_id: int, local_filepath: str, access_token: str) -> str:
    """Upload file to Procore with archiving."""
    logger.info("--- STARTING PROCORE UPLOAD SEQUENCE ---")
    target_id, archive_id = await get_target_combined_folder(project_id, access_token)
    
    if not target_id:
        return "Upload Skipped: Could not find 'Combined IFC' folder."
    
    manager = ProcoreDocManager(settings.PROCORE_COMPANY_ID, project_id, access_token)
    try:
        result = await manager.process_file_update(local_filepath, target_id, archive_id)
        return result
    finally:
        await manager.close()


# ---------------------------------------------------------
# SECTION 4: ASYNC DRAWING PROCESSOR
# ---------------------------------------------------------

async def download_and_process_drawing(
    client: AsyncProcoreClient,
    drawing: Dict,
    discipline_name: str,
    temp_dir: str,
    project_id: int
) -> Optional[Dict]:
    """Download a drawing with markups pre-rendered by Procore."""
    d_num = drawing.get('number', 'NoNum')
    start_time = time.time()
    
    try:
        # Log drawing metadata
        logger.info(f"═══════════════════════════════════════════════════════════")
        logger.info(f"📄 PROCESSING DRAWING: {d_num}")
        logger.info(f"═══════════════════════════════════════════════════════════")
        
        current_rev = drawing.get('current_revision', {})
        d_title = drawing.get('title', 'NoTitle')
        d_rev = current_rev.get('revision_number', '0')
        revision_id = current_rev.get('id')
        
        if not revision_id:
            logger.warning(f"         ⚠ No revision ID for drawing {d_num}")
            return None

        logger.info(f"         Drawing: {d_num}")
        logger.info(f"         Title: {d_title}")
        logger.info(f"         Revision: {d_rev}")
        logger.info(f"         Revision ID: {revision_id}")

        # Create clean filename
        clean_num = re.sub(r'[^\w\.-]', '', d_num)
        clean_title = re.sub(r'[^a-zA-Z0-9]', '-', d_title).strip('-')
        clean_rev = re.sub(r'[^\w]', '', str(d_rev))
        final_name = f"{clean_num}-{clean_title}-Rev.{clean_rev}.pdf"
        filename = os.path.join(temp_dir, final_name)

        # Fetch markup layers to get layer IDs
        logger.debug(f"         Fetching markup layers for {d_num}...")
        m_params = {
            "project_id": project_id,
            "holder_id": revision_id,
            "holder_type": "DrawingRevision",
            "page": 1,
            "page_size": 200
        }
        
        try:
            markup_start = time.time()
            markup_data = await client.get("/rest/v1.0/markup_layers", params=m_params)
            markup_time = time.time() - markup_start
            
            # Extract layer IDs
            layer_ids = []
            if markup_data and len(markup_data) > 0:
                for layer in markup_data:
                    if 'id' in layer:
                        layer_ids.append(layer['id'])
                
                logger.info(f"         Found {len(layer_ids)} markup layers (took {markup_time:.2f}s)")
                logger.debug(f"         Layer IDs: {layer_ids}")
            else:
                logger.info(f"         No markup layers found for {d_num}")
            
            # Request pre-rendered PDF from Procore
            pdf_url = await client.generate_pdf_with_markups(project_id, revision_id, layer_ids)
            
            # Download the pre-rendered PDF
            logger.debug(f"         Downloading pre-rendered PDF for {d_num}...")
            download_start = time.time()
            success = await client.download_file(pdf_url, filename)
            download_time = time.time() - download_start
            
            if not success:
                logger.error(f"         ✗ Download failed for {d_num}")
                return None
            
            file_size = os.path.getsize(filename) / 1024  # KB
            logger.info(f"         ✓ Downloaded {d_num} with markups ({file_size:.1f} KB, took {download_time:.2f}s)")
            
        except Exception as e:
            logger.error(f"         ✗ Failed to get pre-rendered PDF for {d_num}: {type(e).__name__}: {str(e)}")
            raise  # Re-raise to fail the job as per requirements
        
        total_time = time.time() - start_time
        logger.info(f"         ✓ Completed {d_num} in {total_time:.2f}s")
        logger.info(f"═══════════════════════════════════════════════════════════")
        
        return {
            "path": filename,
            "title": final_name.replace(".pdf", ""),
            "discipline": discipline_name
        }
    except Exception as e:
        total_time = time.time() - start_time
        logger.error(f"         ✗ Error processing {d_num} after {total_time:.2f}s: {type(e).__name__}: {str(e)}")
        return None





# ---------------------------------------------------------
# SECTION 5: MAIN JOB RUNNER (ASYNC)
# ---------------------------------------------------------

async def run_job_async(
    project_id: int,
    target_disciplines: List[str],
    access_token: str,
    progress_callback: Optional[Callable[[int, int, int], None]] = None,  # (percent, processed, total)
    drawing_ids: Optional[List[int]] = None  # Optional: specific drawing IDs to process
) -> str:
    """Main async job runner with parallel downloads."""
    start_time = time.time()
    
    # Start memory tracking
    memory_tracker.start()
    initial_ram = memory_tracker.get_current()
    
    logger.info(f"═══════════════════════════════════════════════════════════")
    logger.info(f"🚀 STARTING PDF GENERATION JOB")
    logger.info(f"   Project ID: {project_id}")
    logger.info(f"   Disciplines: {', '.join(target_disciplines)}")
    logger.info(f"   Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"   📊 Initial RAM: {initial_ram:.1f} MB")
    logger.info(f"═══════════════════════════════════════════════════════════")
    
    client = AsyncProcoreClient(access_token, settings.PROCORE_COMPANY_ID)
    
    try:
        # STEP 1: Fetch project info
        step_start = time.time()
        logger.info(f"[STEP 1/6] Fetching project information...")
        project_data = await client.get(f"/rest/v1.0/projects/{project_id}", params={"company_id": settings.PROCORE_COMPANY_ID})
        logger.info(f"═══════════════════════════════════════════════════════════")
        logger.info(f"🏗️  PROJECT METADATA")
        logger.info(f"═══════════════════════════════════════════════════════════")
        logger.info(f"Project object keys: {list(project_data.keys())}")
        logger.info(f"Project metadata:")
        for key, value in project_data.items():
            if isinstance(value, dict):
                logger.info(f"  {key}: {value}")
            elif isinstance(value, list):
                logger.info(f"  {key}: [list with {len(value)} items]")
            else:
                logger.info(f"  {key}: {value}")
        logger.info(f"═══════════════════════════════════════════════════════════")
        project_name = project_data.get('name', 'Project')
        logger.info(f"         ✓ Project: {project_name} (took {time.time() - step_start:.2f}s)")

        # STEP 2: Find IFC drawing area
        step_start = time.time()
        logger.info(f"[STEP 2/6] Finding IFC drawing area...")
        areas_data = await client.get(f"/rest/v1.1/projects/{project_id}/drawing_areas")
        logger.info(f"═══════════════════════════════════════════════════════════")
        logger.info(f"📍 DRAWING AREAS METADATA")
        logger.info(f"═══════════════════════════════════════════════════════════")
        logger.info(f"Number of areas: {len(areas_data) if isinstance(areas_data, list) else 'N/A'}")
        if areas_data and len(areas_data) > 0:
            logger.info(f"Sample area object keys: {list(areas_data[0].keys())}")
            for idx, area in enumerate(areas_data):
                logger.info(f"--- Area {idx + 1} ---")
                logger.info(f"Area metadata:")
                for key, value in area.items():
                    if isinstance(value, dict):
                        logger.info(f"  {key}: {value}")
                    elif isinstance(value, list):
                        logger.info(f"  {key}: [list with {len(value)} items]")
                    else:
                        logger.info(f"  {key}: {value}")
        logger.info(f"═══════════════════════════════════════════════════════════")
        area_id = None
        for a in areas_data:
            if a['name'].strip().upper() == "IFC":
                area_id = a['id']
                break
        
        if not area_id:
            logger.error(f"         ✗ No IFC Area found. Available areas: {[a['name'] for a in areas_data]}")
            raise Exception("No IFC Area found")
        logger.info(f"         ✓ Found IFC area (ID: {area_id}) (took {time.time() - step_start:.2f}s)")

        # STEP 3: Fetch all drawings (paginated)
        step_start = time.time()
        logger.info(f"[STEP 3/6] Fetching drawings list (this may take a moment)...")
        endpoint = f"/rest/v1.1/drawing_areas/{area_id}/drawings"
        all_drawings = []
        page = 1
        
        while True:
            logger.debug(f"         Fetching page {page}...")
            page_start = time.time()
            data = await client.get(endpoint, params={"project_id": project_id, "page": page, "per_page": 100})
            if not data:
                logger.debug(f"         Page {page}: No data returned")
                break
            
            # Log sample drawing metadata from first page
            if page == 1 and data and len(data) > 0:
                logger.info(f"═══════════════════════════════════════════════════════════")
                logger.info(f"📊 SAMPLE DRAWING METADATA (from API response)")
                logger.info(f"═══════════════════════════════════════════════════════════")
                sample_drawing = data[0]
                logger.info(f"Sample drawing number: {sample_drawing.get('number', 'Unknown')}")
                logger.info(f"Drawing object keys: {list(sample_drawing.keys())}")
                logger.info(f"Full drawing object:")
                for key, value in sample_drawing.items():
                    if key == 'current_revision':
                        logger.info(f"  {key}: [revision object - see below]")
                    elif isinstance(value, dict):
                        logger.info(f"  {key}: {value}")
                    elif isinstance(value, list):
                        logger.info(f"  {key}: [list with {len(value)} items]")
                    else:
                        logger.info(f"  {key}: {value}")
                
                # Log current_revision details from sample
                if 'current_revision' in sample_drawing:
                    sample_rev = sample_drawing.get('current_revision', {})
                    logger.info(f"Sample current_revision keys: {list(sample_rev.keys())}")
                    logger.info(f"Sample current_revision object:")
                    for key, value in sample_rev.items():
                        if isinstance(value, dict):
                            logger.info(f"    {key}: {value}")
                        elif isinstance(value, list):
                            logger.info(f"    {key}: [list with {len(value)} items]")
                        else:
                            logger.info(f"    {key}: {value}")
                logger.info(f"═══════════════════════════════════════════════════════════")
            
            all_drawings.extend(data)
            logger.debug(f"         Page {page}: Got {len(data)} drawings (took {time.time() - page_start:.2f}s)")
            if len(data) < 100:
                break
            page += 1

        logger.info(f"         ✓ Retrieved {len(all_drawings)} total drawings across {page} page(s) (took {time.time() - step_start:.2f}s)")

        # STEP 4: Filter by discipline and/or drawing IDs
        step_start = time.time()
        if drawing_ids and len(drawing_ids) > 0:
            logger.info(f"[STEP 4/6] Filtering drawings by selected drawing IDs ({len(drawing_ids)} IDs)...")
        else:
            logger.info(f"[STEP 4/6] Filtering drawings by selected disciplines...")
        final_batch = []
        discipline_counts = {}
        drawing_ids_set = set(drawing_ids) if drawing_ids else None
        
        for dwg in all_drawings:
            # If specific drawing IDs are provided, filter by ID first
            if drawing_ids_set:
                if dwg.get("id") not in drawing_ids_set:
                    continue
            
            # Then filter by discipline (if IDs provided, this ensures discipline matches too)
            d_name = "Unknown"
            if dwg.get("drawing_discipline"):
                d_name = dwg["drawing_discipline"].get("name", "Unknown")
            elif dwg.get("discipline"):
                d_name = dwg["discipline"]
            
            if d_name in target_disciplines:
                final_batch.append((dwg, d_name))
                discipline_counts[d_name] = discipline_counts.get(d_name, 0) + 1

        if not final_batch:
            if drawing_ids_set:
                logger.error(f"         ✗ No drawings found matching the selected drawing IDs and disciplines")
            else:
                logger.error(f"         ✗ No drawings found for disciplines: {', '.join(target_disciplines)}")
            raise Exception("No drawings found for selected criteria")

        logger.info(f"         ✓ Filtered to {len(final_batch)} drawings:")
        for disc, count in discipline_counts.items():
            logger.info(f"           - {disc}: {count} drawing(s)")
        logger.info(f"         (took {time.time() - step_start:.2f}s)")

        # STEP 5: Create temp directory
        step_start = time.time()
        logger.info(f"[STEP 5/6] Setting up temporary workspace...")
        job_timestamp = int(time.time())
        temp_dir = f"temp_job_{project_id}_{job_timestamp}"
        if os.path.exists(temp_dir):
            logger.warning(f"         Cleaning up existing temp directory: {temp_dir}")
            shutil.rmtree(temp_dir)
        os.makedirs(temp_dir)
        if not os.path.exists("output"):
            os.makedirs("output")
        logger.info(f"         ✓ Temp directory: {temp_dir} (took {time.time() - step_start:.2f}s)")

        # STEP 6: Download and process drawings in parallel batches
        step_start = time.time()
        logger.info(f"[STEP 6/6] Downloading and processing {len(final_batch)} drawings...")
        files_to_merge = []
        total_files = len(final_batch)
        batch_size = settings.MAX_CONCURRENT_DOWNLOADS
        
        logger.info(f"         Configuration: batch_size={batch_size}, total_drawings={total_files}")
        
        for batch_start in range(0, total_files, batch_size):
            batch_end = min(batch_start + batch_size, total_files)
            batch = final_batch[batch_start:batch_end]
            batch_num = (batch_start // batch_size) + 1
            total_batches = (total_files + batch_size - 1) // batch_size
            
            batch_start_time = time.time()
            logger.info(f"         ┌─ Batch {batch_num}/{total_batches} (drawings {batch_start+1}-{batch_end} of {total_files})")
            
            # Progress update
            if progress_callback:
                percent = int((batch_start / total_files) * 90)
                progress_callback(percent, batch_start, total_files)
                logger.debug(f"         Progress: {percent}% ({batch_start}/{total_files} drawings)")
            
            # Log each drawing in batch
            for idx, (dwg, d_name) in enumerate(batch, start=1):
                dwg_num = dwg.get('number', 'Unknown')
                logger.debug(f"           [{idx}/{len(batch)}] Queuing: {dwg_num} ({d_name})")
            
            # Download batch concurrently
            tasks = [
                download_and_process_drawing(client, dwg, d_name, temp_dir, project_id)
                for dwg, d_name in batch
            ]
            
            logger.info(f"         Downloading {len(tasks)} drawings in parallel...")
            download_start = time.time()
            results = await asyncio.gather(*tasks, return_exceptions=True)
            download_time = time.time() - download_start
            
            # Process results
            successful = 0
            failed = 0
            for idx, result in enumerate(results, start=1):
                if isinstance(result, Exception):
                    failed += 1
                    dwg_num = batch[idx-1][0].get('number', 'Unknown')
                    logger.error(f"           [{idx}/{len(batch)}] ✗ Failed: {dwg_num} - {type(result).__name__}: {str(result)}")
                elif result:
                    successful += 1
                    files_to_merge.append(result)
                    logger.debug(f"           [{idx}/{len(batch)}] ✓ Success: {result.get('title', 'Unknown')}")
            
            # Update progress after batch completes
            processed_count = batch_end
            if progress_callback:
                percent = int((processed_count / total_files) * 90)
                progress_callback(percent, processed_count, total_files)
            
            logger.info(f"         └─ Batch {batch_num} complete: {successful} succeeded, {failed} failed (took {download_time:.2f}s)")
            
            # Force garbage collection to free memory (critical for low-RAM environments)
            if settings.LOW_MEMORY_MODE:
                gc.collect()
            
            # Log memory after each batch
            log_memory(f"[After Download Batch {batch_num}]")
            
            # Small delay between batches
            if batch_end < total_files:
                logger.debug(f"         Waiting 0.5s before next batch...")
                await asyncio.sleep(0.5)

        if not files_to_merge:
            logger.error(f"         ✗ No PDFs generated successfully out of {total_files} attempts")
            raise Exception("No PDFs generated successfully")

        log_memory("[After All Downloads Complete]")
        logger.info(f"         ✓ Successfully processed {len(files_to_merge)}/{total_files} drawings (took {time.time() - step_start:.2f}s)")

        # STEP 7: Merging phase
        step_start = time.time()
        logger.info(f"[MERGE] Merging {len(files_to_merge)} PDFs into final document...")
        if progress_callback:
            progress_callback(92, total_files, total_files)  # All drawings processed

        # Merge PDFs (CPU-intensive, run in thread)
        log_memory("[Before Merge]")
        final_output_path, actual_page_count = await asyncio.to_thread(
            merge_pdfs,
            files_to_merge,
            project_name,
            temp_dir
        )
        log_memory("[After Merge Complete]")
        
        # STEP 8: Validate PDF page count
        expected_pages = len(files_to_merge)
        logger.info(f"[VALIDATION] Verifying PDF integrity...")
        logger.info(f"         Expected pages: {expected_pages} (one per drawing)")
        logger.info(f"         Actual pages in PDF: {actual_page_count}")
        
        if actual_page_count != expected_pages:
            error_msg = f"PDF validation failed: Expected {expected_pages} pages but PDF has {actual_page_count} pages. The merge process may have failed or some drawings were corrupted."
            logger.error(f"         ✗ {error_msg}")
            # Clean up the invalid PDF
            try:
                if os.path.exists(final_output_path):
                    os.remove(final_output_path)
                    logger.info(f"         Removed invalid PDF: {final_output_path}")
            except Exception as e:
                logger.warning(f"         Could not remove invalid PDF: {e}")
            raise Exception(error_msg)
        
        logger.info(f"         ✓ PDF validation passed: {actual_page_count} pages match expected {expected_pages} pages")
        
        merge_time = time.time() - step_start
        total_time = time.time() - start_time
        
        # Stop memory tracking and log peak usage
        peak_ram = memory_tracker.stop()
        current_ram = memory_tracker.get_current()
        
        logger.info(f"         ✓ Merge complete: {os.path.basename(final_output_path)} (took {merge_time:.2f}s)")
        logger.info(f"═══════════════════════════════════════════════════════════")
        logger.info(f"✅ JOB COMPLETED SUCCESSFULLY")
        logger.info(f"   Output: {final_output_path}")
        logger.info(f"   Total time: {total_time:.2f}s ({total_time/60:.1f} minutes)")
        logger.info(f"   Processed: {len(files_to_merge)}/{total_files} drawings")
        logger.info(f"   Pages: {actual_page_count} (verified)")
        logger.info(f"   📊 RAM Usage: Peak {peak_ram:.1f} MB / 2048 MB ({peak_ram/2048*100:.1f}%) | Final {current_ram:.1f} MB")
        logger.info(f"═══════════════════════════════════════════════════════════")

        return final_output_path

    except Exception as e:
        total_time = time.time() - start_time
        
        # Stop memory tracking and log peak usage even on error
        peak_ram = memory_tracker.stop()
        current_ram = memory_tracker.get_current()
        
        logger.error(f"═══════════════════════════════════════════════════════════")
        logger.error(f"❌ JOB FAILED after {total_time:.2f}s")
        logger.error(f"   Error: {type(e).__name__}: {str(e)}")
        logger.error(f"   Project ID: {project_id}")
        logger.error(f"   Disciplines: {', '.join(target_disciplines)}")
        logger.error(f"   📊 RAM at crash: {current_ram:.1f} MB | Peak: {peak_ram:.1f} MB / 2048 MB ({peak_ram/2048*100:.1f}%)")
        logger.error(f"═══════════════════════════════════════════════════════════", exc_info=True)
        raise
    finally:
        await client.close()


def cleanup_old_pdfs_per_project():
    """
    Clean up old PDF files per project, keeping only the latest PDF for each project.
    """
    output_dir = "output"
    if not os.path.exists(output_dir):
        return
    
    try:
        # Group PDF files by project name
        project_pdfs = {}  # {project_name: [(filepath, mtime, filename), ...]}
        
        for filename in os.listdir(output_dir):
            filepath = os.path.join(output_dir, filename)
            if os.path.isfile(filepath) and filename.lower().endswith('.pdf'):
                # Extract project name from filename pattern: {ProjectName}_Merged.pdf
                if '_Merged.pdf' in filename:
                    project_name = filename.replace('_Merged.pdf', '')
                    mtime = os.path.getmtime(filepath)
                    
                    if project_name not in project_pdfs:
                        project_pdfs[project_name] = []
                    project_pdfs[project_name].append((filepath, mtime, filename))
        
        # For each project, keep only the most recent PDF
        total_deleted = 0
        total_freed_mb = 0
        
        for project_name, files in project_pdfs.items():
            if len(files) > 1:
                # Sort by modification time (newest first)
                files.sort(key=lambda x: x[1], reverse=True)
                
                # Delete all but the most recent
                files_to_delete = files[1:]
                
                for filepath, _, filename in files_to_delete:
                    try:
                        file_size = os.path.getsize(filepath) / (1024 * 1024)
                        os.remove(filepath)
                        total_deleted += 1
                        total_freed_mb += file_size
                        logger.debug(f"         Deleted old PDF: {filename} ({file_size:.2f} MB)")
                    except Exception as e:
                        logger.warning(f"         Could not delete {filename}: {e}")
        
        if total_deleted > 0:
            logger.info(f"         Cleaned up {total_deleted} old PDF(s), freed {total_freed_mb:.2f} MB")
    
    except Exception as e:
        logger.warning(f"         Error during PDF cleanup: {e}")




def merge_pdfs(files_to_merge: List[Dict], project_name: str, temp_dir: str) -> tuple[str, int]:
    """
    Standard Tier Merge: Optimized for 2GB RAM.
    - Uses Batching for safety.
    - DISABLES final compression to prevent memory crashes.
    """
    merge_start = time.time()
    log_memory("[MERGE START]")
    logger.info(f"          Starting PDF merge (Low Memory Mode: {settings.LOW_MEMORY_MODE})...")
    logger.info(f"          Files to merge: {len(files_to_merge)}")
    
    # 1. Setup paths (cleanup moved to after successful generation)
    clean_proj = re.sub(r'[^a-zA-Z0-9]', '_', project_name)
    final_filename = f"{clean_proj}_Merged.pdf"
    final_output_path = os.path.join("output", final_filename)
    
    # 2. Setup Batches - use configurable batch size (default 15 for low memory)
    BATCH_SIZE = settings.MERGE_BATCH_SIZE
    logger.info(f"          Merge batch size: {BATCH_SIZE}")
    intermediate_dir = os.path.join(temp_dir, "batches")
    os.makedirs(intermediate_dir, exist_ok=True)
    
    batch_files = []
    total_pages = 0
    
    # GLOBAL TOC - built with absolute page numbers across all batches
    # This is added to the final PDF at the end (insert_pdf does NOT merge TOCs)
    global_toc = []
    current_discipline = None
    
    # --- PHASE A: CREATE BATCHES ---
    num_batches = (len(files_to_merge) + BATCH_SIZE - 1) // BATCH_SIZE
    
    for i in range(0, len(files_to_merge), BATCH_SIZE):
        batch_num = (i // BATCH_SIZE) + 1
        batch_items = files_to_merge[i : i + BATCH_SIZE]
        
        logger.info(f"          Processing Batch {batch_num}/{num_batches}...")
        
        # Track source files to delete after batch is created
        source_files_to_delete = []
        
        with pymupdf.open() as batch_doc:
            for item in batch_items:
                try:
                    with pymupdf.open(item['path']) as src_doc:
                        # Calculate ABSOLUTE page number (across all batches)
                        absolute_page_num = total_pages + len(batch_doc) + 1
                        
                        batch_doc.insert_pdf(src_doc)
                        
                        # Build GLOBAL TOC with absolute page numbers
                        if item['discipline'] != current_discipline:
                            global_toc.append([1, item['discipline'], absolute_page_num])
                            current_discipline = item['discipline']
                        global_toc.append([2, item['title'], absolute_page_num])
                        
                    # Mark source file for deletion (progressive cleanup)
                    if settings.LOW_MEMORY_MODE:
                        source_files_to_delete.append(item['path'])
                        
                except Exception as e:
                    logger.warning(f"          ⚠ Skipping corrupt file {item.get('title')}: {e}")

            # Save batch WITHOUT TOC (we add global TOC at the end)
            batch_path = os.path.join(intermediate_dir, f"batch_{batch_num}.pdf")
            batch_doc.save(batch_path, garbage=0, deflate=False)
            batch_files.append(batch_path)
            total_pages += len(batch_doc)
        
        # Progressive cleanup: delete source PDFs after batch is saved
        # This frees disk space and reduces memory pressure
        if settings.LOW_MEMORY_MODE and source_files_to_delete:
            deleted_count = 0
            for src_path in source_files_to_delete:
                try:
                    if os.path.exists(src_path):
                        os.remove(src_path)
                        deleted_count += 1
                except Exception:
                    pass  # Ignore cleanup errors
            if deleted_count > 0:
                logger.debug(f"          Cleaned up {deleted_count} source PDFs from batch {batch_num}")
            
        gc.collect()

    if not batch_files:
        raise Exception("No valid batches were created.")

    log_memory("[AFTER PHASE A - Batches Created]")

    # --- PHASE B: INCREMENTAL ASSEMBLY ---
    logger.info(f"          Assembling final PDF from {len(batch_files)} batches...")
    
    # Init with first batch
    shutil.copy(batch_files[0], final_output_path)
    
    # Append the rest
    if len(batch_files) > 1:
        for idx, batch_path in enumerate(batch_files[1:], start=2):
            try:
                doc_final = pymupdf.open(final_output_path)
                doc_batch = pymupdf.open(batch_path)
                doc_final.insert_pdf(doc_batch)
                
                # Incremental save is fast and low-memory
                doc_final.save(
                    final_output_path, 
                    incremental=True, 
                    encryption=pymupdf.PDF_ENCRYPT_KEEP
                )
                doc_final.close()
                doc_batch.close()
                
                # Remove batch file immediately to free disk space
                os.remove(batch_path)
                
                # In LOW_MEMORY_MODE, gc after every batch (critical for 200+ drawings)
                # Otherwise, gc every 5 batches
                if settings.LOW_MEMORY_MODE:
                    gc.collect()
                elif idx % 5 == 0:
                    gc.collect()
                
                # Log progress for large merges
                if idx % 10 == 0 or idx == len(batch_files):
                    logger.info(f"          Assembly progress: {idx}/{len(batch_files)} batches")
                
            except Exception as e:
                logger.error(f"          ✗ Error merging batch {batch_path}: {e}")
                raise Exception(f"Merge failed at batch {os.path.basename(batch_path)}")

    if os.path.exists(batch_files[0]):
        os.remove(batch_files[0])

    log_memory("[AFTER PHASE B - Assembly Complete]")

    # --- PHASE C: ADD TOC ---
    # insert_pdf() does NOT merge TOCs, so we add the global TOC at the end
    # This requires opening the file but set_toc() only modifies outline metadata, not page content
    
    actual_page_count = total_pages
    
    if not os.path.exists(final_output_path):
        raise Exception("Final PDF not created")
    
    logger.info(f"          Adding Table of Contents ({len(global_toc)} entries)...")
    
    try:
        # Open the merged PDF to add TOC
        doc_final = pymupdf.open(final_output_path)
        
        # Validate and set TOC
        # Filter out any entries that point to pages beyond the document
        valid_toc = [entry for entry in global_toc if entry[2] <= len(doc_final)]
        
        if valid_toc:
            doc_final.set_toc(valid_toc)
            logger.info(f"          ✓ TOC added: {len(valid_toc)} entries")
        
        # # Save with TOC - use non-incremental save to ensure TOC is written
        # temp_final = final_output_path + ".toc.tmp"
        # doc_final.save(temp_final, garbage=0, deflate=False)
        # doc_final.close()
        
        # # Replace original with TOC version
        # os.replace(temp_final, final_output_path)
        
        # # Force garbage collection after TOC operation
        # gc.collect()

        # MANDATORY FIX: Use incremental save to append TOC metadata
        # RAM Usage: ~1MB | Time: ~0.1s | Prevents full file rewrite
        doc_final.save(
            final_output_path, 
            incremental=True, 
            encryption=pymupdf.PDF_ENCRYPT_KEEP
        )
        
        doc_final.close()
        
        # Force garbage collection after TOC operation
        gc.collect()
        log_memory("[AFTER PHASE C - TOC Added]")
        
    except Exception as toc_error:
        # If TOC fails, the PDF is still valid - just log warning
        logger.warning(f"          ⚠ Could not add TOC (PDF still valid): {toc_error}")
        try:
            if 'doc_final' in locals() and doc_final:
                doc_final.close()
        except:
            pass
    
    # Cleanup temp directory
    try:
        shutil.rmtree(temp_dir)
    except:
        pass
    
    # Force garbage collection before returning
    gc.collect()

    total_time = time.time() - merge_start
    final_size_mb = os.path.getsize(final_output_path) / (1024 * 1024)
    
    # Clean up old PDFs for this project (keep only the latest)
    cleanup_old_pdfs_per_project()
    
    logger.info(f"          ✓ MERGE COMPLETE: {actual_page_count} pages, {final_size_mb:.2f} MB")
    
    return final_output_path, actual_page_count

def _cleanup_after_merge(temp_dir: str, files_merged: int, total_files: int, page_count: int, merge_start: float):
    """Helper function to clean up after merge and log summary."""
    # Cleanup old PDFs after saving (to maintain max 20 files)
    cleanup_old_pdfs(max_files=20)
    
    # Cleanup temp directory
    logger.debug(f"         Cleaning up temporary directory...")
    cleanup_start = time.time()
    try:
        shutil.rmtree(temp_dir)
        cleanup_time = time.time() - cleanup_start
        logger.debug(f"         ✓ Cleaned up {temp_dir} (took {cleanup_time:.2f}s)")
    except Exception as e:
        logger.warning(f"         ⚠ Could not clean up temp directory: {e}")
    
    total_merge_time = time.time() - merge_start
    logger.info(f"         Merge summary: {files_merged}/{total_files} files, {page_count} pages, {total_merge_time:.2f}s total")


# ---------------------------------------------------------
# SYNC WRAPPER (for backward compatibility)
# ---------------------------------------------------------

def run_job_api(
    project_id: int,
    target_disciplines: List[str],
    access_token: str,
    progress_callback: Optional[Callable[[int, int, int], None]] = None,  # (percent, processed, total)
    drawing_ids: Optional[List[int]] = None  # Optional: specific drawing IDs to process
) -> str:
    """Synchronous wrapper for async job runner."""
    return asyncio.run(run_job_async(project_id, target_disciplines, access_token, progress_callback, drawing_ids))


# ---------------------------------------------------------
# LEGACY API FUNCTIONS (Synchronous)
# ---------------------------------------------------------

def api_get_projects(access_token: str):
    """Get list of projects (sync version)."""
    async def _get_projects():
        client = AsyncProcoreClient(access_token, settings.PROCORE_COMPANY_ID)
        try:
            data = await client.get("/rest/v1.0/projects", params={"company_id": settings.PROCORE_COMPANY_ID})
            projects = data if isinstance(data, list) else []
            projects.sort(key=lambda x: x.get('name', ''))
            return projects
        except aiohttp.ClientResponseError as e:
            if e.status == 401:
                return None
            return []
        except:
            return []
        finally:
            await client.close()
    
    return asyncio.run(_get_projects())


def api_get_disciplines(project_id: int, access_token: str):
    """Get list of disciplines for a project (sync version)."""
    async def _get_disciplines():
        client = AsyncProcoreClient(access_token, settings.PROCORE_COMPANY_ID)
        try:
            r_area = await client.get(f"/rest/v1.1/projects/{project_id}/drawing_areas")
            area_id = None
            for a in r_area:
                if a['name'].strip().upper() == "IFC":
                    area_id = a['id']
                    break
            
            if not area_id:
                return {"error": "No 'IFC' Drawing Area found."}

            endpoint = f"/rest/v1.1/drawing_areas/{area_id}/drawings"
            all_drawings = []
            page = 1
            
            while True:
                data = await client.get(endpoint, params={"project_id": project_id, "page": page, "per_page": 100})
                if not data:
                    break
                all_drawings.extend(data)
                if len(data) < 100:
                    break
                page += 1

            disciplines = set()
            for dwg in all_drawings:
                d_name = "Unknown"
                if dwg.get("drawing_discipline"):
                    d_name = dwg["drawing_discipline"].get("name", "Unknown")
                elif dwg.get("discipline"):
                    d_name = dwg["discipline"]
                disciplines.add(d_name)
            
            return {"area_id": area_id, "disciplines": sorted(list(disciplines))}
        except Exception as e:
            return {"error": str(e)}
        finally:
            await client.close()
    
    return asyncio.run(_get_disciplines())
