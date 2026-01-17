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
from datetime import datetime
from typing import List, Dict, Any, Optional, Callable
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, RetryError
import logging

from config import settings

# Commented out imports - only needed for legacy markup overlay functions
# import math
# from collections import defaultdict
# import statistics
# import itertools

logger = logging.getLogger(__name__)


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

    # @retry(
    #     stop=stop_after_attempt(5),  # Try up to 5 times
    #     wait=wait_exponential(multiplier=2, min=5, max=60),  # 5s, 10s, 20s, 40s, 60s
    #     retry=retry_if_exception_type((aiohttp.ServerDisconnectedError, aiohttp.ClientError, asyncio.TimeoutError, BrokenPipeError, OSError))
    # )
    # async def _upload_file_async(self, local_file_path: str, target_folder_id: int) -> str:
    #     """
    #     Async upload with retry logic for connection issues.
    #     Improved SSL configuration for better compatibility.
    #     """
    #     endpoint = f"{self.base_url}/rest/v1.0/files"
    #     params = {"project_id": self.project_id}
    #     file_name = os.path.basename(local_file_path)
        
    #     # Get file size for logging
    #     file_size_mb = os.path.getsize(local_file_path) / (1024 * 1024)
    #     logger.info(f"         Uploading file: {file_name} ({file_size_mb:.2f} MB)")

    #     try:
    #         # Create timeout - use longer timeout for large files
    #         # Calculate timeout based on file size: 10 seconds per MB, minimum 5 minutes, maximum 30 minutes
    #         upload_timeout = min(max(300, int(file_size_mb * 10)), 1800)  # 10 sec/MB, min 5min, max 30min
    #         timeout = aiohttp.ClientTimeout(total=upload_timeout, connect=60, sock_read=upload_timeout)
            
    #         # Close and recreate session if it exists (to ensure fresh connection)
    #         if self.session:
    #             try:
    #                 await self.session.close()
    #             except:
    #                 pass
    #             self.session = None
            
    #         # Enhanced SSL context for better compatibility
    #         ssl_context = ssl.create_default_context()
    #         ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2  # Force TLS 1.2+
    #         ssl_context.check_hostname = True
    #         ssl_context.verify_mode = ssl.CERT_REQUIRED
            
    #         # Create new session with improved settings for large file uploads
    #         # Note: force_close=True prevents connection reuse but is incompatible with keepalive_timeout
    #         connector = aiohttp.TCPConnector(
    #             ssl=ssl_context,
    #             limit=100,
    #             limit_per_host=10,
    #             enable_cleanup_closed=True,
    #             force_close=True  # Don't reuse connections - prevents stale connection issues
    #         )
    #         self.session = aiohttp.ClientSession(
    #             timeout=timeout,
    #             connector=connector
    #         )
            
    #         logger.info(f"         Upload timeout set to {upload_timeout}s for {file_size_mb:.2f} MB file")
    #         upload_start = time.time()
            
    #         with open(local_file_path, 'rb') as f:
    #             data = aiohttp.FormData()
    #             data.add_field('file[data]', f, filename=file_name, content_type='application/pdf')
    #             data.add_field('file[parent_id]', str(target_folder_id))
    #             data.add_field('file[name]', file_name)
                
    #             async with self.session.post(endpoint, headers=self.headers, params=params, data=data) as response:
    #                 if response.status not in [200, 201]:
    #                     text = await response.text()
    #                     error_msg = f"Error {response.status}: {text[:200]}"
    #                     logger.error(f"         ✗ Upload failed: {error_msg}")
    #                     return error_msg
                    
    #                 upload_time = time.time() - upload_start
    #                 logger.info(f"         ✓ Successfully uploaded '{file_name}' (took {upload_time:.2f}s, {file_size_mb/upload_time:.2f} MB/s)")
    #                 return "Success"
    #     except asyncio.TimeoutError as e:
    #         error_msg = f"Upload timeout after {upload_timeout}s (file size: {file_size_mb:.2f} MB)"
    #         logger.error(f"         ✗ {error_msg}")
    #         raise  # Re-raise to trigger retry
    #     except (aiohttp.ServerDisconnectedError, aiohttp.ClientError) as e:
    #         error_msg = f"Connection error during upload: {type(e).__name__}: {str(e)}"
    #         logger.warning(f"         ⚠ {error_msg} - will retry...")
    #         raise  # Re-raise to trigger retry
    #     except BrokenPipeError as e:
    #         error_msg = f"Broken pipe error during upload: {str(e)}"
    #         logger.warning(f"         ⚠ {error_msg} - will retry...")
    #         raise  # Re-raise to trigger retry
    #     except OSError as e:
    #         if e.errno == 32:  # Broken pipe
    #             error_msg = f"Broken pipe error (connection closed): {str(e)}"
    #             logger.warning(f"         ⚠ {error_msg} - will retry...")
    #             raise  # Re-raise to trigger retry
    #         else:
    #             error_msg = f"OS error during upload: {str(e)}"
    #             logger.error(f"         ✗ {error_msg}")
    #             return error_msg
    #     except Exception as e:
    #         # For other exceptions, don't retry
    #         error_msg = f"Error uploading file: {type(e).__name__}: {str(e)}"
    #         logger.error(f"         ✗ {error_msg}")
    #         return error_msg

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

    # async def upload_file(self, local_file_path: str, target_folder_id: int) -> str:
    #     """
    #     Upload a file to Procore.
    #     Tries async upload first, falls back to synchronous upload if async fails.
    #     """
    #     file_name = os.path.basename(local_file_path)
    #     file_size_mb = os.path.getsize(local_file_path) / (1024 * 1024)
    #     async_start = time.time()
        
    #     try:
    #         # Try async upload first (with automatic retries via @retry decorator)
    #         result = await self._upload_file_async(local_file_path, target_folder_id)
    #         return result
            
    #     except RetryError as e:
    #         # All async retries exhausted
    #         async_duration = time.time() - async_start
    #         logger.warning(f"═══════════════════════════════════════════════════════════")
    #         logger.warning(f"⚠️  ASYNC UPLOAD FAILED AFTER ALL RETRIES")
    #         logger.warning(f"   File: {file_name} ({file_size_mb:.2f} MB)")
    #         logger.warning(f"   Time spent on async attempts: {async_duration:.2f}s")
            
    #         # Log SSL diagnostics
    #         ssl_details = extract_ssl_error_details(e)
    #         logger.info(f"   SSL Error Details:")
    #         for key, value in ssl_details.items():
    #             logger.info(f"     - {key}: {value}")
            
    #         logger.warning(f"   Falling back to synchronous upload...")
    #         logger.warning(f"═══════════════════════════════════════════════════════════")
            
    #         # Try synchronous fallback
    #         sync_result = self.upload_file_sync(local_file_path, target_folder_id)
            
    #         if sync_result == "Success":
    #             total_time = time.time() - async_start
    #             logger.info(f"═══════════════════════════════════════════════════════════")
    #             logger.info(f"✅ UPLOAD SUCCEEDED VIA SYNC FALLBACK")
    #             logger.info(f"   File: {file_name}")
    #             logger.info(f"   Total time (async attempts + sync): {total_time:.2f}s")
    #             logger.info(f"═══════════════════════════════════════════════════════════")
            
    #         return sync_result
            
    #     except Exception as e:
    #         # Unexpected error not caught by retry logic
    #         error_msg = f"Unexpected upload error: {type(e).__name__}: {str(e)}"
    #         logger.error(f"         ✗ {error_msg}")
    #         return error_msg

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
        
        payload = {
            "name": file_name,
            "size": file_size,
            "mimetype": "application/pdf"
        }
        
        try:
            # Initialize
            async with self.session.post(init_url, headers=self.headers, json=payload) as response:
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
            
            finalize_payload = {
                "file": {
                    "upload_uuid": upload_uuid,
                    "parent_id": target_folder_id,
                    "name": file_name
                }
            }
            
            # Use standard _make_request for finalization
            await self._make_request('POST', finalize_url, headers=self.headers, json=finalize_payload)
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

# class AsyncProcoreClient:
#     """Async client for Procore API calls."""
    
#     def __init__(self, access_token: str, company_id: int):
#         self.access_token = access_token
#         self.company_id = company_id
#         self.base_url = settings.PROCORE_BASE_URL
#         self.headers = {
#             "Authorization": f"Bearer {access_token}",
#             "Procore-Company-Id": str(company_id),
#             "Accept": "application/json"
#         }
#         self.session: Optional[aiohttp.ClientSession] = None
    
#     # async def _ensure_session(self):
#     #     """Ensure session is created."""
#     #     if not self.session:
#     #         import ssl
#     #         timeout = aiohttp.ClientTimeout(total=settings.DOWNLOAD_TIMEOUT)
            
#     #         # Create SSL context with proper certificate handling
#     #         ssl_context = ssl.create_default_context()
            
#     #         # Try to use certifi certificates if available
#     #         try:
#     #             import certifi
#     #             ssl_context = ssl.create_default_context(cafile=certifi.where())
#     #             logger.info("Using certifi SSL certificates")
#     #         except ImportError:
#     #             logger.warning("certifi not available, using system certificates")
#     #         except Exception as ssl_error:
#     #             logger.error(f"SSL configuration error: {ssl_error}")
                
#     #             # Development fallback: disable SSL verification
#     #             if settings.ENVIRONMENT == "development":
#     #                 logger.warning("⚠️  SSL VERIFICATION DISABLED (DEVELOPMENT ONLY)")
#     #                 logger.warning("⚠️  Fix by running: /Applications/Python\\ 3.14/Install\\ Certificates.command")
#     #                 ssl_context.check_hostname = False
#     #                 ssl_context.verify_mode = ssl.CERT_NONE
#     #             else:
#     #                 # Production: don't disable SSL
#     #                 raise Exception("SSL certificate verification failed. Install certificates: pip install --upgrade certifi")
            
#     #         connector = aiohttp.TCPConnector(ssl=ssl_context)
#     #         self.session = aiohttp.ClientSession(
#     #             timeout=timeout,
#     #             connector=connector
#     #         )

#     async def _ensure_session(self):
#             """
#             Creates a persistent aiohttp session if one doesn't exist.
#             CRITICAL FIX: Enables Keep-Alive to prevent connection churn/timeouts.
#             """
#             if not self.session or self.session.closed:
#                 # Configure TCP Connector for Persistent Connections
#                 connector = aiohttp.TCPConnector(
#                     ssl=self.ssl_context,     # Use the SSL context from __init__
#                     limit=10,                 # Max concurrent connections
#                     limit_per_host=10,        # Max connections to Procore
#                     enable_cleanup_closed=True,
#                     force_close=False,        # <--- FALSE enables Keep-Alive (Fixes timeouts)
#                     keepalive_timeout=60,     # Keep connection open for 60s
#                     ttl_dns_cache=300         # Cache DNS to save lookups
#                 )
                
#                 # Default timeout for general operations
#                 # (We override this for uploads specifically in the upload method)
#                 timeout = aiohttp.ClientTimeout(total=300, connect=60)
                
#                 self.session = aiohttp.ClientSession(
#                     timeout=timeout,
#                     connector=connector
#                 )
#                 logger.info("ProcoreDocManager: Created new persistent ClientSession")
    
#     @retry(
#         stop=stop_after_attempt(3),
#         wait=wait_exponential(multiplier=1, min=2, max=10),
#         retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
#     )
#     async def get(self, endpoint: str, params: Optional[Dict] = None) -> Any:
#         """Make GET request with retry logic and rate limit tracking."""
#         await self._ensure_session()
#         url = f"{self.base_url}{endpoint}"
#         request_start = time.time()
        
#         # Check rate limit before making request
#         waited = await _rate_limit_tracker.check_and_wait_if_needed()
#         if waited:
#             logger.info(f"         Resumed after rate limit wait")
        
#         # Log request details (but not sensitive params)
#         safe_params = {k: v for k, v in (params or {}).items() if k not in ['client_secret', 'access_token']}
#         logger.debug(f"         API GET: {endpoint} {safe_params}")
        
#         try:
#             async with self.session.get(url, headers=self.headers, params=params) as response:
#                 request_time = time.time() - request_start
                
#                 # Update rate limit tracker from response headers
#                 _rate_limit_tracker.update_from_headers(dict(response.headers))
                
#                 if response.status == 429:
#                     # Use X-Rate-Limit-Reset if available, otherwise fall back to Retry-After
#                     reset_timestamp = response.headers.get('X-Rate-Limit-Reset')
#                     if reset_timestamp:
#                         reset_time = int(reset_timestamp)
#                         current_time = time.time()
#                         wait_seconds = max(0, reset_time - current_time)
#                         logger.warning(f"         ⚠ Rate limited. Waiting {wait_seconds:.0f}s until reset at {datetime.fromtimestamp(reset_time).strftime('%H:%M:%S')} (request took {request_time:.2f}s)")
#                         if wait_seconds > 0:
#                             await asyncio.sleep(wait_seconds)
#                     else:
#                         retry_after = int(response.headers.get('Retry-After', 60))
#                         logger.warning(f"         ⚠ Rate limited. Waiting {retry_after}s (request took {request_time:.2f}s)")
#                         await asyncio.sleep(retry_after)
#                     raise aiohttp.ClientError("Rate limited")
                
#                 # Don't raise for 401/403 - let caller handle auth errors
#                 if response.status in [401, 403]:
#                     error_text = await response.text()
#                     logger.warning(f"         ⚠ Auth error {response.status} for {endpoint}: {error_text[:100]} (took {request_time:.2f}s)")
#                     error = aiohttp.ClientResponseError(
#                         request_info=response.request_info,
#                         history=response.history,
#                         status=response.status,
#                         message=error_text
#                     )
#                     raise error
                
#                 response.raise_for_status()
                
#                 # Try to parse JSON
#                 try:
#                     result = await response.json()
#                     logger.debug(f"         ✓ API GET: {endpoint} - {len(str(result))} bytes (took {request_time:.2f}s)")
#                     return result
#                 except Exception as json_error:
#                     text = await response.text()
#                     logger.error(f"         ✗ Failed to parse JSON response: {json_error}. Response: {text[:200]} (took {request_time:.2f}s)")
#                     raise aiohttp.ClientError(f"Invalid JSON response: {str(json_error)}")
                    
#         except aiohttp.ClientResponseError as e:
#             # Re-raise HTTP errors (401, 403, 404, etc.)
#             request_time = time.time() - request_start
#             logger.error(f"         ✗ HTTP {e.status} error for {endpoint} (took {request_time:.2f}s): {e.message[:100]}")
#             raise
#         except Exception as e:
#             request_time = time.time() - request_start
#             logger.error(f"         ✗ GET request failed for {endpoint} (took {request_time:.2f}s): {type(e).__name__}: {e}")
#             raise
    
#     @retry(
#         stop=stop_after_attempt(3),
#         wait=wait_exponential(multiplier=1, min=2, max=10),
#         retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
#     )
#     async def post(self, endpoint: str, json: Optional[Dict] = None, params: Optional[Dict] = None) -> Any:
#         """Make POST request with retry logic and rate limit tracking."""
#         await self._ensure_session()
#         url = f"{self.base_url}{endpoint}"
#         request_start = time.time()
        
#         # Check rate limit before making request
#         waited = await _rate_limit_tracker.check_and_wait_if_needed()
#         if waited:
#             logger.info(f"         Resumed after rate limit wait")
        
#         logger.debug(f"         API POST: {endpoint}")
        
#         try:
#             headers = self.headers.copy()
#             headers["Content-Type"] = "application/json"
            
#             async with self.session.post(url, headers=headers, json=json, params=params) as response:
#                 request_time = time.time() - request_start
                
#                 # Update rate limit tracker from response headers
#                 _rate_limit_tracker.update_from_headers(dict(response.headers))
                
#                 if response.status == 429:
#                     # Use X-Rate-Limit-Reset if available, otherwise fall back to Retry-After
#                     reset_timestamp = response.headers.get('X-Rate-Limit-Reset')
#                     if reset_timestamp:
#                         reset_time = int(reset_timestamp)
#                         current_time = time.time()
#                         wait_seconds = max(0, reset_time - current_time)
#                         logger.warning(f"         ⚠ Rate limited. Waiting {wait_seconds:.0f}s until reset at {datetime.fromtimestamp(reset_time).strftime('%H:%M:%S')} (request took {request_time:.2f}s)")
#                         if wait_seconds > 0:
#                             await asyncio.sleep(wait_seconds)
#                     else:
#                         retry_after = int(response.headers.get('Retry-After', 60))
#                         logger.warning(f"         ⚠ Rate limited. Waiting {retry_after}s (request took {request_time:.2f}s)")
#                         await asyncio.sleep(retry_after)
#                     raise aiohttp.ClientError("Rate limited")
                
#                 # Don't raise for 401/403 - let caller handle auth errors
#                 if response.status in [401, 403]:
#                     error_text = await response.text()
#                     logger.warning(f"         ⚠ Auth error {response.status} for {endpoint}: {error_text[:100]} (took {request_time:.2f}s)")
#                     error = aiohttp.ClientResponseError(
#                         request_info=response.request_info,
#                         history=response.history,
#                         status=response.status,
#                         message=error_text
#                     )
#                     raise error
                
#                 response.raise_for_status()
                
#                 # Try to parse JSON
#                 try:
#                     result = await response.json()
#                     logger.debug(f"         ✓ API POST: {endpoint} - {len(str(result))} bytes (took {request_time:.2f}s)")
#                     return result
#                 except Exception as json_error:
#                     text = await response.text()
#                     logger.error(f"         ✗ Failed to parse JSON response: {json_error}. Response: {text[:200]} (took {request_time:.2f}s)")
#                     raise aiohttp.ClientError(f"Invalid JSON response: {str(json_error)}")
                    
#         except aiohttp.ClientResponseError as e:
#             request_time = time.time() - request_start
#             logger.error(f"         ✗ HTTP {e.status} error for {endpoint} (took {request_time:.2f}s): {e.message[:100]}")
#             raise
#         except Exception as e:
#             request_time = time.time() - request_start
#             logger.error(f"         ✗ POST request failed for {endpoint} (took {request_time:.2f}s): {type(e).__name__}: {e}")
#             raise
    
#     async def generate_pdf_with_markups(self, project_id: int, revision_id: int, layer_ids: List[int]) -> str:
#         """
#         Generate and download a PDF with markups using Procore's API.
#         Returns the download URL.
#         """
#         endpoint = f"/rest/v1.0/projects/{project_id}/drawing_revisions/{revision_id}/pdf_download_pages"
        
#         payload = {
#             "pdf_download_page": {
#                 "markup_layer_ids": layer_ids,
#                 "filtered_types": [],
#                 "filtered_items": {
#                     "GenericToolItem": {
#                         "generic_tool_id": []
#                     }
#                 }
#             }
#         }
        
#         logger.info(f"         Requesting pre-rendered PDF from Procore (revision {revision_id}, {len(layer_ids)} layers)...")
#         response_data = await self.post(endpoint, json=payload)
#         download_url = response_data.get('url')
        
#         if not download_url:
#             raise Exception("Procore API returned success but no download URL found")
        
#         logger.debug(f"         ✓ Got PDF URL from Procore")
#         return download_url
    
#     async def download_file(self, url: str, destination: str) -> bool:
#         """Download a file from URL."""
#         await self._ensure_session()
#         download_start = time.time()
        
#         try:
#             async with self.session.get(url) as response:
#                 response.raise_for_status()
#                 content_length = response.headers.get('Content-Length')
#                 if content_length:
#                     logger.debug(f"         Downloading {int(content_length) / 1024:.1f} KB...")
                
#                 bytes_downloaded = 0
#                 with open(destination, 'wb') as f:
#                     while True:
#                         chunk = await response.content.read(8192)
#                         if not chunk:
#                             break
#                         f.write(chunk)
#                         bytes_downloaded += len(chunk)
                
#                 download_time = time.time() - download_start
#                 file_size_kb = bytes_downloaded / 1024
#                 logger.debug(f"         ✓ Downloaded {file_size_kb:.1f} KB in {download_time:.2f}s ({file_size_kb/download_time:.1f} KB/s)")
#                 return True
#         except Exception as e:
#             download_time = time.time() - download_start
#             logger.error(f"         ✗ Download failed after {download_time:.2f}s: {type(e).__name__}: {e}")
#             return False
    
#     async def close(self):
#         """Close the session."""
#         if self.session:
#             await self.session.close()


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


# =========================================================================
# LEGACY MARKUP OVERLAY FUNCTIONS (COMMENTED OUT - KEPT AS BACKUP)
# =========================================================================
# These functions are no longer used. The app now uses Procore's pre-rendered
# PDF generation API which handles all markup overlays server-side.
# Kept as backup in case fallback is needed in the future.
# =========================================================================

# def extract_drawing_number(text: str) -> Optional[str]:
#     """
#     Extract drawing number from text like "DR S1.19" -> "S1.19"
#     The format is always "DR XXXX" where XXXX is the drawing number.
#     """
#     if not text:
#         return None
#     
#     # Remove "DR " prefix if present (case insensitive)
#     cleaned = text.strip()
#     if cleaned.upper().startswith("DR "):
#         cleaned = cleaned[3:].strip()  # Remove "DR " (3 characters)
#     
#     # If there's still text, return it, otherwise try regex pattern
#     if cleaned:
#         return cleaned
#     
#     # Fallback: Pattern for letter(s) + number(s) + optional dot + number(s)
#     # Examples: S1.19, S001, A1.01, S1, S500, etc.
#     pattern = r'([A-Z]+\d+(?:\.\d+)?)'
#     match = re.search(pattern, text, re.IGNORECASE)
#     if match:
#         return match.group(1).upper()  # Return uppercase for consistency
#     
#     return None


# def get_anchor_text_info(page, markups: List[Dict], search_limit: int = 50) -> dict:
#     """
#     Get anchor text information for debugging using robust calibration.
#     Returns detailed info about anchor text candidates and the calibration results.
#     """
#     candidates = [m for m in markups if m.get('item_displayed_name')][:search_limit]
#     
#     anchor_info = {
#         "candidates": [],
#         "selected_anchor": None,
#         "mode": None,
#         "scale": None,
#         "offset_x": None,
#         "offset_y": None
#     }
#     
#     if not candidates:
#         return anchor_info
#     
#     page_width = page.rect.width
#     page_height = page.rect.height
#     page_rotation = page.rotation
#     
#     # Collect all candidates with their info
#     for item in candidates:
#         full_text = item.get('item_displayed_name', '').strip()
#         if not full_text or len(full_text) < 3:
#             continue
#         
#         # Extract drawing number from the text (remove "DR " prefix)
#         text = extract_drawing_number(full_text)
#         if not text or len(text) < 3 or text in ["000", "TYP", "NTS"]:
#             # Skip noise or invalid extractions
#             continue
#         
#         try:
#             # Search for the extracted drawing number ONLY (never search for "DR XXXX")
#             pdf_matches = page.search_for(text, quads=False)
#             if not pdf_matches:
#                 anchor_info["candidates"].append({
#                     "text": text,
#                     "full_text": full_text,
#                     "json_position": {"x": float(item.get('x', 0)), "y": float(item.get('y', 0))},
#                     "found_in_pdf": False
#                 })
#                 continue
#             
#             # Found in PDF - record all matches
#             jx = float(item.get('x', 0))
#             jy = float(item.get('y', 0))
#             
#             if jx <= 0 or jy <= 0:
#                 continue
#             
#             for pdf_rect in pdf_matches:
#                 px = pdf_rect.x0
#                 py = pdf_rect.y0
#                 
#                 candidate_info = {
#                     "text": text,
#                     "full_text": full_text,
#                     "json_position": {"x": jx, "y": jy},
#                     "pdf_position": {"x": px, "y": py},
#                     "found_in_pdf": True,
#                     "scale_x": px / jx if jx > 0 else None,
#                     "scale_y": py / jy if jy > 0 else None
#                 }
#                 
#                 anchor_info["candidates"].append(candidate_info)
#                     
#         except Exception as e:
#             anchor_info["candidates"].append({
#                 "text": text,
#                 "full_text": full_text,
#                 "error": str(e)
#             })
#             continue
#     
#     # Use robust calibration to get the actual transformation
#     transform = robust_calibrate_transform(page, markups, search_limit)
#     
#     anchor_info["mode"] = transform.get("mode", "fallback")
#     anchor_info["scale"] = transform.get("scale", 1.0)
#     anchor_info["offset_x"] = transform.get("offset_x", 0)
#     anchor_info["offset_y"] = transform.get("offset_y", 0)
#     anchor_info["var_std_x"] = transform.get("var_std_x", None)
#     anchor_info["var_rot_x"] = transform.get("var_rot_x", None)
#     
#     # Set selected anchor info based on the transform
#     if anchor_info["candidates"] and anchor_info["mode"] != "fallback":
#         # Use the first valid candidate as the "selected" one for display
#         for cand in anchor_info["candidates"]:
#             if cand.get("found_in_pdf"):
#                 anchor_info["selected_anchor"] = {
#                     "text": cand["text"],
#                     "full_text": cand["full_text"],
#                     "json_position": cand["json_position"],
#                     "pdf_position": cand["pdf_position"],
#                     "mode": anchor_info["mode"],
#                     "scale": anchor_info["scale"]
#                 }
#                 break
#     
#     anchor_info["page_info"] = {
#         "width": page_width,
#         "height": page_height,
#         "rotation": page_rotation
#     }
#     
#     return anchor_info

# def robust_calibrate_transform(page, markups: List[Dict], search_limit: int = 50) -> dict:
#     """
#     Robust calibration using 'Relative Distances' to ignore Origin Offsets.
#     
#     1. Finds all text matches.
#     2. Calculates Scale based on distances between pairs of points (cancels offset).
#     3. Determines Rotation/Offset based on the consensus scale.
#     """
#     
#     # --- 1. COLLECT POINTS ---
#     candidates = [m for m in markups if m.get('item_displayed_name')][:search_limit]
#     points = [] # List of {"text": str, "jx": float, "jy": float, "px": float, "py": float}
#     
#     # Gather valid text matches
#     for item in candidates:
#         full_text = item.get('item_displayed_name', '').strip()
#         text = extract_drawing_number(full_text)
#         
#         # Skip noise
#         if not text or len(text) < 3 or text in ["000", "TYP", "NTS"]: continue
#         
#         jx, jy = float(item.get('x', 0)), float(item.get('y', 0))
#         if jx <= 0 or jy <= 0: continue
# 
#         # Search in PDF
#         pdf_matches = page.search_for(text, quads=False)
#         for rect in pdf_matches:
#             points.append({
#                 "text": text,
#                 "jx": jx, "jy": jy,
#                 "px": rect.x0, "py": rect.y0
#             })
# 
#     if len(points) < 2:
#         logger.warning("[Calibration] Not enough points for distance calibration.")
#         return {"mode": "fallback", "scale": 1.0, "offset_x": 0, "offset_y": 0}
# 
#     # --- 2. VOTE ON SCALE (Using Distances) ---
#     scale_votes = []
#     
#     # Compare every point against every other point (Pairwise)
#     # We use a stride or sample if too many points to save time, but <50 is fine.
#     for p1, p2 in itertools.combinations(points, 2):
#         # Calculate Euclidean Distance in JSON
#         dist_j = math.hypot(p1['jx'] - p2['jx'], p1['jy'] - p2['jy'])
#         
#         # Calculate Euclidean Distance in PDF
#         dist_p = math.hypot(p1['px'] - p2['px'], p1['py'] - p2['py'])
#         
#         # Filter: Ignore points that are too close (noise amplification)
#         if dist_j < 50: continue 
#         
#         # Calculate Scale
#         scale = dist_p / dist_j
#         scale_votes.append(scale)
# 
#     if not scale_votes:
#         return {"mode": "fallback", "scale": 1.0}
# 
#     # Bin the scales to find the consensus
#     # We round to 3 decimals to group them (e.g. 0.291 and 0.292 -> 0.29)
#     bins = defaultdict(int)
#     for s in scale_votes:
#         bins[round(s, 3)] += 1
#     
#     # Winner takes all
#     best_scale_rounded = max(bins, key=bins.get)
#     
#     # Refine: Average all votes that fell into the winning bin for precision
#     winning_votes = [s for s in scale_votes if round(s, 3) == best_scale_rounded]
#     final_scale = statistics.mean(winning_votes)
#     
#     logger.info(f"         [Calibration] Consensus Scale: {final_scale:.5f} (Strength: {len(winning_votes)} pairs)")
# 
#     # --- 3. DETERMINE MODE & OFFSETS ---
#     # Now that we know scale, we check orientation and offset
#     
#     offset_votes = []
#     
#     for p in points:
#         # We test two hypotheses for every point:
#         
#         # Hypothesis A: Standard (PDF_X ~ JSON_X)
#         # Offset X = Px - (Jx * Scale)
#         std_off_x = p['px'] - (p['jx'] * final_scale)
#         std_off_y = p['py'] - (p['jy'] * final_scale)
#         
#         # Hypothesis B: Rotated 270 (PDF_X ~ JSON_Y inverted)
#         # Note: In 270, Jx maps to Py, Jy maps to Px (inverted)
#         # Py = (Jx * Scale) + Off_Y  => Off_Y = Py - (Jx * Scale)
#         # Px = Off_X - (Jy * Scale)  => Off_X = Px + (Jy * Scale)
#         rot_off_x = p['px'] + (p['jy'] * final_scale)
#         rot_off_y = p['py'] - (p['jx'] * final_scale)
#         
#         offset_votes.append({
#             "std_x": std_off_x, "std_y": std_off_y,
#             "rot_x": rot_off_x, "rot_y": rot_off_y
#         })
# 
#     # To pick the mode, we check variance. 
#     # The correct mode will have consistent offsets (Low Variance).
#     # The wrong mode will have offsets that drift wildly.
#     
#     try:
#         var_std_x = statistics.variance([v['std_x'] for v in offset_votes]) if len(offset_votes) > 1 else 0
#         var_rot_x = statistics.variance([v['rot_x'] for v in offset_votes]) if len(offset_votes) > 1 else 999999
#         
#         # If Standard variance is lower (or significantly low), pick Standard
#         if var_std_x < var_rot_x and var_std_x < 1000:
#             mode = "standard"
#             off_x = statistics.mean([v['std_x'] for v in offset_votes])
#             off_y = statistics.mean([v['std_y'] for v in offset_votes])
#         else:
#             mode = "rotated_270_calibrated"
#             off_x = statistics.mean([v['rot_x'] for v in offset_votes])
#             off_y = statistics.mean([v['rot_y'] for v in offset_votes])
#             
#     except Exception as e:
#         # Fallback for single point edge case
#         mode = "standard"
#         off_x = offset_votes[0]['std_x']
#         off_y = offset_votes[0]['std_y']
#         var_std_x = 0
#         var_rot_x = 999999
# 
#     logger.info(f"         [Calibration] Final Mode: {mode}, Offsets: ({off_x:.1f}, {off_y:.1f})")
#     logger.info(f"         [Calibration] Variance - Standard: {var_std_x:.2f}, Rotated: {var_rot_x:.2f}")
# 
#     return {
#         "mode": mode,
#         "scale": final_scale,
#         "offset_x": off_x,
#         "offset_y": off_y,
#         "var_std_x": var_std_x,
#         "var_rot_x": var_rot_x
#     }
# def auto_calibrate_transform(page, markups: List[Dict], search_limit: int = 20) -> dict:
#     """
#     Auto-calibrate transformation by finding anchor text in both PDF and markups.
    
#     Handles two modes:
#     - Standard: PDF = JSON * scale (no rotation)
#     - Rotated 270: PDF_X = offset_x - (JSON_Y * scale), PDF_Y = (JSON_X * scale) + offset_y
    
#     Returns: {"mode": "standard"|"rotated_270_calibrated"|"fallback", "scale": float, "offset_x": float, "offset_y": float}
#     """
#     # Collect potential text anchors from markups
#     candidates = [m for m in markups if m.get('item_displayed_name')][:search_limit]
    
#     if not candidates:
#         logger.info("         [Calibration] No text anchors found in markups")
#         return {"mode": "fallback", "scale": 1.0, "offset_x": 0, "offset_y": 0}
    
#     logger.info(f"         [Calibration] Searching for anchors in {len(candidates)} markups...")
    
#     page_width = page.rect.width
#     page_height = page.rect.height
#     page_rotation = page.rotation
    
#     # Try to find matching text between PDF and JSON
#     for item in candidates:
#         full_text = item.get('item_displayed_name', '').strip()
#         if not full_text or len(full_text) < 3:  # Skip very short text
#             continue
        
#         # Extract drawing number from the text (remove "DR " prefix)
#         text = extract_drawing_number(full_text)
#         if not text:
#             # If extraction fails, skip this candidate
#             continue
        
#         try:
#             # Search for the extracted drawing number ONLY (never search for "DR XXXX")
#             pdf_matches = page.search_for(text)
            
#             if not pdf_matches:
#                 continue
            
#             # Get PDF position (top-left of text, physical coordinates)
#             pdf_rect = pdf_matches[0]
#             px = pdf_rect.x0
#             py = pdf_rect.y0
            
#             # Get JSON position (top-left of markup)
#             jx = float(item.get('x', 0))
#             jy = float(item.get('y', 0))
            
#             if jx <= 0 or jy <= 0:
#                 continue
            
#             logger.info(f"         [Calibration] Testing anchor '{text}' (from '{full_text}')")
#             logger.info(f"           PDF (physical): ({px:.1f}, {py:.1f})")
#             logger.info(f"           JSON: ({jx:.1f}, {jy:.1f})")
#             logger.info(f"           Page: {page_width:.1f}x{page_height:.1f}, rotation: {page_rotation}°")
            
#             # Strategy A: Standard Scaling (Direct Map)
#             # PDF_X = JSON_X * Scale, PDF_Y = JSON_Y * Scale
#             scale_x = px / jx
#             scale_y = py / jy
            
#             if abs(scale_x - scale_y) / max(scale_x, scale_y) < 0.05:  # 5% tolerance
#                 scale = (scale_x + scale_y) / 2
#                 logger.info(f"         [Calibration] ✓ Standard mode detected")
#                 logger.info(f"           Scale: {scale:.4f} (X={scale_x:.4f}, Y={scale_y:.4f})")
#                 return {"mode": "standard", "scale": scale, "offset_x": 0, "offset_y": 0}
            
#             # Strategy B: Rotated 270 with calibrated offsets
#             # Use fixed scale typical for rotated drawings
#             fixed_scale = 0.4054
            
#             # Calculate offsets to force anchor match:
#             # PDF_Y = (JSON_X * Scale) + Offset_Y  => Offset_Y = PDF_Y - (JSON_X * Scale)
#             # PDF_X = Offset_X - (JSON_Y * Scale)  => Offset_X = PDF_X + (JSON_Y * Scale)
#             calc_offset_y = py - (jx * fixed_scale)
#             calc_offset_x = px + (jy * fixed_scale)
            
#             logger.info(f"         [Calibration] ✓ Rotated 270° mode with calibrated offsets")
#             logger.info(f"           Scale: {fixed_scale:.4f}")
#             logger.info(f"           Offset X: {calc_offset_x:.1f} (Page Width: {page_width})")
#             logger.info(f"           Offset Y: {calc_offset_y:.1f}")
            
#             return {
#                 "mode": "rotated_270_calibrated", 
#                 "scale": fixed_scale, 
#                 "offset_x": calc_offset_x, 
#                 "offset_y": calc_offset_y
#             }
                    
#         except Exception as e:
#             logger.debug(f"         [Calibration] Error processing '{text}': {e}")
#             continue
    
#     logger.info(f"         [Calibration] No matching anchor found for transformation")
#     return {"mode": "fallback", "scale": 1.0, "offset_x": 0, "offset_y": 0}

# def auto_calibrate_transform(page, markups: List[Dict], search_limit: int = 50) -> dict:
#     """
#     Auto-calibrate using a 'Consensus Voting' approach.
#     Instead of stopping at the first match, it collects all possible matches
#     and selects the transformation scale that appears most frequently.
#     """
    
#     # Configuration
#     tolerance = 0.05  # 5% tolerance for x/y aspect ratio check
#     bin_precision = 2  # Round scales to 2 decimals for voting bins (e.g. 0.40)
#     min_text_len = 3   # Ignore "00", "A1", etc.
    
#     # Bins to collect votes: Key = rounded_scale, Value = list of detailed matches
#     # We track standard and rotated votes separately
#     votes_standard = defaultdict(list)
#     votes_rotated = defaultdict(list)

#     candidates = [m for m in markups if m.get('item_displayed_name')][:search_limit]
    
#     page_w = page.rect.width
#     page_h = page.rect.height
    
#     logger.info(f"         [Calibration] Running Consensus Voting on {len(candidates)} candidates...")

#     for item in candidates:
#         full_text = item.get('item_displayed_name', '').strip()
        
#         # 1. Clean Text
#         text = extract_drawing_number(full_text)
#         if not text or len(text) < min_text_len:
#             continue
            
#         # 2. Skip "Noise" Text (Common false positives in drawings)
#         if text in ["000", "TYP", "NTS", "U.N.O", "NOTE"]:
#             continue

#         jx = float(item.get('x', 0))
#         jy = float(item.get('y', 0))
#         if jx <= 0 or jy <= 0: continue

#         # 3. Find ALL occurrences in PDF (not just the first one)
#         # using quads=False returns Rect objects
#         pdf_matches = page.search_for(text, quads=False)
        
#         for rect in pdf_matches:
#             px, py = rect.x0, rect.y0
            
#             # --- VOTE TYPE A: STANDARD (No Rotation) ---
#             # Check if X and Y scales are roughly equal
#             scale_x = px / jx
#             scale_y = py / jy
            
#             # Avoid division by zero or negative scales
#             if scale_x > 0 and scale_y > 0:
#                 diff = abs(scale_x - scale_y)
#                 avg_scale = (scale_x + scale_y) / 2
                
#                 if diff / avg_scale < tolerance:
#                     # It's a valid candidate for Standard Mode
#                     vote_key = round(avg_scale, bin_precision)
#                     votes_standard[vote_key].append({
#                         'scale': avg_scale,
#                         'error': diff,
#                         'text': text
#                     })

#             # --- VOTE TYPE B: ROTATED 270 ---
#             # In 270 rot: PDF_Y relates to JSON_X. PDF_X relates to JSON_Y.
#             # Scale = py / jx
#             scale_rot = py / jx
            
#             # Verify "X" alignment (which is Y in JSON)
#             # expected_px = page_width - (jy * scale)
#             # We check if the calculated scale makes sense for the other axis
#             if scale_rot > 0:
#                 calc_px = page_w - (jy * scale_rot)
#                 # Check if the calculated PDF X is close to the actual PDF X
#                 # We allow a looser tolerance for rotation offsets (50pts)
#                 if abs(calc_px - px) < 50:
#                     vote_key = round(scale_rot, bin_precision)
#                     votes_rotated[vote_key].append({
#                         'scale': scale_rot,
#                         'offset_x': calc_px, # Store purely for debugging
#                         'text': text
#                     })

#     # --- TALLY VOTES ---
    
#     best_mode = "fallback"
#     best_scale = 1.0
#     best_offset_x = 0
#     best_offset_y = 0
#     max_votes = 0
    
#     # Check Standard Winners
#     for scale_key, matches in votes_standard.items():
#         count = len(matches)
#         if count > max_votes:
#             max_votes = count
#             best_mode = "standard"
#             # distinct_text_count = len(set(m['text'] for m in matches)) # Optional robustness check
            
#             # Calculate precise average from the bin candidates
#             best_scale = statistics.mean([m['scale'] for m in matches])

#     # Check Rotated Winners
#     for scale_key, matches in votes_rotated.items():
#         count = len(matches)
#         # We prioritize Rotated only if it has strictly MORE votes, or equal votes but we suspect rotation
#         if count > max_votes:
#             max_votes = count
#             best_mode = "rotated_270_calibrated"
#             best_scale = statistics.mean([m['scale'] for m in matches])

#     # --- CALIBRATE OFFSETS (If Rotated) ---
#     if best_mode == "rotated_270_calibrated":
#         # If we picked rotated, we need to find the average offsets based on the winning scale
#         # Re-iterate the winning matches to average the offsets
#         # PDF_Y = (JSON_X * Scale) + Offset_Y
#         # PDF_X = Offset_X - (JSON_Y * Scale)
        
#         valid_offsets_x = []
#         valid_offsets_y = []
        
#         # Get the specific matches that contributed to the win
#         winning_bin = round(best_scale, bin_precision)
#         winning_matches = votes_rotated.get(winning_bin, [])
        
#         # Re-find the source markup data for these matches to calc offsets
#         # (This is a simplified lookup, in production passing objects is cleaner)
#         for cand in candidates:
#             c_text = extract_drawing_number(cand.get('item_displayed_name', ''))
#             # search matches for this text
#             if c_text in [m['text'] for m in winning_matches]:
#                  # Re-calculate specific offsets for this markup using the Global Best Scale
#                  jx, jy = float(cand['x']), float(cand['y'])
#                  # We need the PDF coords. We can't easily get them here without re-searching
#                  # or storing them in the vote. 
#                  # Let's assume we stored them or re-search quickly:
#                  p_matches = page.search_for(c_text, quads=False)
#                  for rect in p_matches:
#                      px, py = rect.x0, rect.y0
#                      # Check if this specific rect aligns with our Best Scale
#                      curr_scale = py / jx
#                      if abs(curr_scale - best_scale) < 0.05:
#                          valid_offsets_y.append(py - (jx * best_scale))
#                          valid_offsets_x.append(px + (jy * best_scale))

#         if valid_offsets_x and valid_offsets_y:
#             best_offset_x = statistics.mean(valid_offsets_x)
#             best_offset_y = statistics.mean(valid_offsets_y)
#         else:
#             # Fallback if averaging failed
#             best_offset_x = page_w
#             best_offset_y = 0

#     logger.info(f"         [Calibration] Result: {best_mode} with {max_votes} votes. Scale: {best_scale:.4f}")
    
#     if max_votes == 0:
#         return {"mode": "fallback", "scale": 1.0, "offset_x": 0, "offset_y": 0}

#     return {
#         "mode": best_mode,
#         "scale": best_scale,
#         "offset_x": best_offset_x,
#         "offset_y": best_offset_y
#     }


# def apply_markups_to_pdf(filename: str, markups: List[Dict], drawing: Dict, markup_layer_info: Optional[Dict] = None, nudge_up: float = 6.0):
#     """
#     Apply markups to PDF with improved calibrated offset handling.
#     
#     Args:
#         filename: Path to PDF file
#         markups: List of markup dictionaries
#         drawing: Drawing metadata
#         markup_layer_info: Optional markup layer metadata
#         nudge_up: Fine-tuning adjustment for rotated drawings (default 6.0)
#     """
#     try:
#         doc = pymupdf.open(filename)
#         if len(doc) == 0:
#             doc.close()
#             return
#         
#         page = doc[0]
#         d_num = drawing.get('number', 'Unknown')
#         
#         logger.info(f"--- Applying Markups to {d_num} ---")
#         
#         # 1. Try Robust Calibration (The preferred method)
#         transform = robust_calibrate_transform(page, markups)
#         mode = transform["mode"]
#         scale = transform["scale"]
#         offset_x = transform.get("offset_x", 0)
#         offset_y = transform.get("offset_y", 0)
#         
#         # 2. IMPROVED FALLBACK LOGIC
#         if mode == "fallback":
#             logger.info(f"         [Fallback] Auto-calibration failed. checking Metadata...")
#             
#             # Get dimensions from Procore Metadata (Safe handling for None)
#             if markup_layer_info:
#                 # Use (val or 0) to convert None to 0
#                 coord_w = float(markup_layer_info.get('width') or 0)
#                 coord_h = float(markup_layer_info.get('height') or 0)
#             else:
#                 coord_w = float(drawing.get('width') or 0)
#                 coord_h = float(drawing.get('height') or 0)
#             
#             page_w = page.rect.width
#             page_h = page.rect.height
# 
#             # Valid metadata available?
#             if coord_w > 0 and coord_h > 0:
#                 # Calculate Aspect Ratios
#                 pdf_aspect = page_w / page_h
#                 json_aspect = coord_w / coord_h
#                 
#                 # Check for Aspect Ratio mismatch (Orientation Mismatch)
#                 # e.g., PDF is Wide (1.4), JSON is Tall (0.7) -> implies Rotation
#                 is_pdf_landscape = pdf_aspect > 1.0
#                 is_json_landscape = json_aspect > 1.0
#                 
#                 if is_pdf_landscape != is_json_landscape:
#                     # Mismatch detected! Force Rotation.
#                     logger.warning(f"         [Fallback] Orientation Mismatch detected! Force-enabling Rotated 270 mode.")
#                     mode = "rotated_270_calibrated"
#                     
#                     # In rotated mode, JSON Width maps to PDF Height
#                     scale_x = page_h / coord_w  # Note the swap
#                     scale_y = page_w / coord_h
#                     scale = (scale_x + scale_y) / 2
#                     offset_x = page_w  # Use page width as default offset
#                     offset_y = 0
#                 else:
#                     # Orientation matches, use Standard
#                     logger.info(f"         [Fallback] Orientation matches. Using Standard mode.")
#                     mode = "standard"
#                     scale_x = page_w / coord_w
#                     scale_y = page_h / coord_h
#                     scale = (scale_x + scale_y) / 2
#             else:
#                 # No metadata? Use blind fit-to-width
#                 logger.warning(f"         [Fallback] No metadata dimensions. Using Blind Fit-Width.")
#                 max_jx = max([float(m.get('x', 0)) + float(m.get('w', 0)) for m in markups if 'x' in m], default=1)
#                 if max_jx > 1:
#                     scale = page_w / max_jx
#                     mode = "standard"
#                 else:
#                     scale = 1.0
#         
#         logger.info(f"         Final Transform: Mode={mode}, Scale={scale:.5f}")
#         if mode == "rotated_270_calibrated":
#             logger.info(f"         Offsets: X={offset_x:.1f}, Y={offset_y:.1f}, Nudge={nudge_up:.1f}")
# 
#         # --- DRAWING LOOP ---
#         shape = page.new_shape()
#         page_width = page.rect.width
# 
#         rectangle_markups = [m for m in markups if m.get('type') == 'Rectangle' or not m.get('type')]
#         
#         for m in rectangle_markups:
#             if not all(k in m for k in ("x", "y", "w", "h")): continue
#             
#             try:
#                 jx, jy = float(m['x']), float(m['y'])
#                 jw, jh = float(m['w']), float(m['h'])
#                 
#                 if mode == "rotated_270_calibrated":
#                     # Use calibrated offsets
#                     # 1. JSON X -> Physical Y
#                     # Formula: Py = (Jx * Scale) + Offset_Y
#                     p_y0 = (jx * scale) + offset_y
#                     p_y1 = ((jx + jw) * scale) + offset_y
#                     
#                     # 2. JSON Y -> Physical X (Inverted)
#                     # Formula: Px = Offset_X - (Jy * Scale)
#                     # Note: We subtract the END of the box to get the START of the physical rect
#                     # NUDGE UP: Increasing Px moves objects RIGHT physically -> UP visually on 270 deg rotation
#                     p_x0 = offset_x - ((jy + jh) * scale) + nudge_up
#                     p_x1 = offset_x - (jy * scale) + nudge_up
#                     
#                     rect = pymupdf.Rect(p_x0, p_y0, p_x1, p_y1)
#                     
#                 elif mode == "rotated_270":
#                     # Old rotated 270 logic (for backward compatibility)
#                     p1_x = page_width - (jy * scale)
#                     p1_y = jx * scale
#                     p2_x = page_width - ((jy + jh) * scale)
#                     p2_y = (jx + jw) * scale
#                     
#                     rx0, ry0 = min(p1_x, p2_x), min(p1_y, p2_y)
#                     rx1, ry1 = max(p1_x, p2_x), max(p1_y, p2_y)
#                     rect = pymupdf.Rect(rx0, ry0, rx1, ry1)
#                 else:
#                     # Standard
#                     x = jx * scale
#                     y = jy * scale
#                     w = jw * scale
#                     h = jh * scale
#                     rect = pymupdf.Rect(x, y, x + w, y + h)
# 
#                 if rect.is_empty: continue
#                 
#                 shape.draw_rect(rect)
#                 shape.finish(color=(0, 0, 1), fill=(0, 1, 1), fill_opacity=0.4, width=1.5)
#                 
#             except Exception:
#                 continue
#         
#         shape.commit()
#         
#         # Save logic
#         temp_f = filename.replace(".pdf", "_temp.pdf")
#         doc.save(temp_f, garbage=4, deflate=True)
#         doc.close()
#         if os.path.exists(temp_f): os.replace(temp_f, filename)
#         
#     except Exception as e:
#         logger.error(f"Error applying markups to {drawing.get('number', 'Unknown')}: {e}", exc_info=True)


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
    logger.info(f"═══════════════════════════════════════════════════════════")
    logger.info(f"🚀 STARTING PDF GENERATION JOB")
    logger.info(f"   Project ID: {project_id}")
    logger.info(f"   Disciplines: {', '.join(target_disciplines)}")
    logger.info(f"   Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
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
            
            # Small delay between batches
            if batch_end < total_files:
                logger.debug(f"         Waiting 0.5s before next batch...")
                await asyncio.sleep(0.5)

        if not files_to_merge:
            logger.error(f"         ✗ No PDFs generated successfully out of {total_files} attempts")
            raise Exception("No PDFs generated successfully")

        logger.info(f"         ✓ Successfully processed {len(files_to_merge)}/{total_files} drawings (took {time.time() - step_start:.2f}s)")

        # STEP 7: Merging phase
        step_start = time.time()
        logger.info(f"[MERGE] Merging {len(files_to_merge)} PDFs into final document...")
        if progress_callback:
            progress_callback(92, total_files, total_files)  # All drawings processed

        # Merge PDFs (CPU-intensive, run in thread)
        final_output_path, actual_page_count = await asyncio.to_thread(
            merge_pdfs,
            files_to_merge,
            project_name,
            temp_dir
        )
        
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
        logger.info(f"         ✓ Merge complete: {os.path.basename(final_output_path)} (took {merge_time:.2f}s)")
        logger.info(f"═══════════════════════════════════════════════════════════")
        logger.info(f"✅ JOB COMPLETED SUCCESSFULLY")
        logger.info(f"   Output: {final_output_path}")
        logger.info(f"   Total time: {total_time:.2f}s ({total_time/60:.1f} minutes)")
        logger.info(f"   Processed: {len(files_to_merge)}/{total_files} drawings")
        logger.info(f"   Pages: {actual_page_count} (verified)")
        logger.info(f"═══════════════════════════════════════════════════════════")

        return final_output_path

    except Exception as e:
        total_time = time.time() - start_time
        logger.error(f"═══════════════════════════════════════════════════════════")
        logger.error(f"❌ JOB FAILED after {total_time:.2f}s")
        logger.error(f"   Error: {type(e).__name__}: {str(e)}")
        logger.error(f"   Project ID: {project_id}")
        logger.error(f"   Disciplines: {', '.join(target_disciplines)}")
        logger.error(f"═══════════════════════════════════════════════════════════", exc_info=True)
        raise
    finally:
        await client.close()


def cleanup_old_pdfs(max_files: int = 20):
    """
    Clean up old PDF files in the output directory, keeping only the most recent ones.
    
    Args:
        max_files: Maximum number of files to keep (default: 20)
    """
    output_dir = "output"
    if not os.path.exists(output_dir):
        return
    
    try:
        # Get all PDF files with their modification times
        pdf_files = []
        for filename in os.listdir(output_dir):
            filepath = os.path.join(output_dir, filename)
            if os.path.isfile(filepath) and filename.lower().endswith('.pdf'):
                mtime = os.path.getmtime(filepath)
                pdf_files.append((filepath, mtime, filename))
        
        # Sort by modification time (newest first)
        pdf_files.sort(key=lambda x: x[1], reverse=True)
        
        # If we have more than max_files, delete the oldest ones
        if len(pdf_files) >= max_files:
            files_to_delete = pdf_files[max_files:]
            deleted_count = 0
            freed_space_mb = 0
            
            for filepath, _, filename in files_to_delete:
                try:
                    file_size = os.path.getsize(filepath) / (1024 * 1024)  # MB
                    os.remove(filepath)
                    deleted_count += 1
                    freed_space_mb += file_size
                    logger.debug(f"         Deleted old PDF: {filename} ({file_size:.2f} MB)")
                except Exception as e:
                    logger.warning(f"         Could not delete {filename}: {e}")
            
            if deleted_count > 0:
                logger.info(f"         Cleaned up {deleted_count} old PDF file(s), freed {freed_space_mb:.2f} MB")
    except Exception as e:
        logger.warning(f"         Error during PDF cleanup: {e}")


# def merge_pdfs(files_to_merge: List[Dict], project_name: str, temp_dir: str) -> tuple[str, int]:
#     """
#     Merge PDFs with TOC (memory-efficient for large batches).
#     Uses batch processing to stay within memory limits (e.g., Render.com 512MB).
#     Returns: (final_output_path, actual_page_count)
#     """
#     merge_start = time.time()
#     logger.info(f"         Starting PDF merge process...")
#     logger.info(f"         Files to merge: {len(files_to_merge)}")
    
#     # Clean up old PDFs before merging to ensure we have space
#     logger.debug(f"         Cleaning up old PDFs (keeping last 20)...")
#     cleanup_old_pdfs(max_files=20)
    
#     # Generate filename (without date - date will be added when archived)
#     clean_proj = re.sub(r'[^a-zA-Z0-9]', '_', project_name)
#     final_filename = f"{clean_proj}_Merged.pdf"
#     final_output_path = os.path.join("output", final_filename)
    
#     # For large batches (>50 files), use batch processing to save memory
#     # Process in chunks of 50 files at a time, saving intermediate results
#     BATCH_SIZE = 50
#     use_batch_processing = len(files_to_merge) > BATCH_SIZE
    
#     if use_batch_processing:
#         logger.info(f"         Using batch processing (chunks of {BATCH_SIZE}) to manage memory...")
#         return _merge_pdfs_batch(files_to_merge, final_output_path, temp_dir, BATCH_SIZE, merge_start)
#     else:
#         # For smaller batches, use the original method
#         return _merge_pdfs_single_pass(files_to_merge, final_output_path, temp_dir, merge_start)


# def _merge_pdfs_single_pass(files_to_merge: List[Dict], final_output_path: str, temp_dir: str, merge_start: float) -> tuple[str, int]:
#     """Original single-pass merge for smaller batches."""
#     merged_doc = pymupdf.open()
#     toc = []
#     current_discipline = None
#     pages_added = 0
#     files_merged = 0
    
#     for idx, item in enumerate(files_to_merge, start=1):
#         try:
#             logger.debug(f"         [{idx}/{len(files_to_merge)}] Merging: {item.get('title', 'Unknown')}")
#             file_start = time.time()
#             doc_single = pymupdf.open(item['path'])
#             page_count = len(doc_single)
            
#             # Add discipline header to TOC
#             if item['discipline'] != current_discipline:
#                 toc.append([1, item['discipline'], len(merged_doc) + 1])
#                 current_discipline = item['discipline']
#                 logger.debug(f"           Discipline section: {item['discipline']}")
            
#             # Add drawing to TOC
#             toc.append([2, item['title'], len(merged_doc) + 1])
            
#             # Insert pages
#             merged_doc.insert_pdf(doc_single)
#             pages_added += page_count
#             files_merged += 1
#             doc_single.close()
            
#             file_time = time.time() - file_start
#             logger.debug(f"           ✓ Added {page_count} page(s) (took {file_time:.2f}s)")
#         except Exception as e:
#             logger.warning(f"         [{idx}/{len(files_to_merge)}] ✗ Could not merge {item.get('path', 'Unknown')}: {type(e).__name__}: {str(e)}")
#             continue
    
#     logger.info(f"         Setting table of contents ({len(toc)} entries)...")
#     merged_doc.set_toc(toc)
    
#     logger.info(f"         Saving merged PDF...")
#     save_start = time.time()
#     merged_doc.save(final_output_path, garbage=4, deflate=True)
#     save_time = time.time() - save_start
    
#     # Verify the saved PDF page count
#     actual_page_count = len(merged_doc)
#     merged_doc.close()
    
#     # Double-check by reopening the saved file
#     verify_doc = pymupdf.open(final_output_path)
#     verified_page_count = len(verify_doc)
#     verify_doc.close()
    
#     if verified_page_count != actual_page_count:
#         logger.warning(f"         ⚠ Page count mismatch: Document shows {actual_page_count} but saved file has {verified_page_count}")
#         actual_page_count = verified_page_count
    
#     final_size = os.path.getsize(final_output_path) / (1024 * 1024)  # MB
#     logger.info(f"         ✓ Saved ({final_size:.2f} MB, {actual_page_count} pages, took {save_time:.2f}s)")
    
#     _cleanup_after_merge(temp_dir, files_merged, len(files_to_merge), actual_page_count, merge_start)
    
#     return final_output_path, actual_page_count


# def _merge_pdfs_batch(files_to_merge: List[Dict], final_output_path: str, temp_dir: str, batch_size: int, merge_start: float) -> tuple[str, int]:
#     """
#     Memory-efficient batch merging for large PDF collections.
#     Processes files in batches, saving intermediate results to disk.
#     """
#     import gc
    
#     # Create temporary directory for intermediate files
#     intermediate_dir = os.path.join(temp_dir, "merge_intermediate")
#     os.makedirs(intermediate_dir, exist_ok=True)
    
#     toc = []
#     current_discipline = None
#     total_pages = 0
#     files_merged = 0
#     batch_files = []
    
#     # Process files in batches
#     num_batches = (len(files_to_merge) + batch_size - 1) // batch_size
    
#     for batch_num in range(num_batches):
#         batch_start_idx = batch_num * batch_size
#         batch_end_idx = min(batch_start_idx + batch_size, len(files_to_merge))
#         batch_items = files_to_merge[batch_start_idx:batch_end_idx]
        
#         logger.info(f"         Processing batch {batch_num + 1}/{num_batches} (files {batch_start_idx + 1}-{batch_end_idx})...")
        
#         # Create a new document for this batch
#         batch_doc = pymupdf.open()
#         batch_toc = []
#         batch_current_discipline = current_discipline
#         batch_pages = 0
        
#         for idx, item in enumerate(batch_items, start=batch_start_idx + 1):
#             try:
#                 logger.debug(f"         [{idx}/{len(files_to_merge)}] Merging: {item.get('title', 'Unknown')}")
#                 file_start = time.time()
#                 doc_single = pymupdf.open(item['path'])
#                 page_count = len(doc_single)
                
#                 # Add discipline header to TOC (tracking offset from previous batches)
#                 if item['discipline'] != batch_current_discipline:
#                     batch_toc.append([1, item['discipline'], len(batch_doc) + total_pages + 1])
#                     batch_current_discipline = item['discipline']
#                     current_discipline = batch_current_discipline
#                     logger.debug(f"           Discipline section: {item['discipline']}")
                
#                 # Add drawing to TOC
#                 batch_toc.append([2, item['title'], len(batch_doc) + total_pages + 1])
                
#                 # Insert pages
#                 batch_doc.insert_pdf(doc_single)
#                 batch_pages += page_count
#                 files_merged += 1
#                 doc_single.close()
                
#                 file_time = time.time() - file_start
#                 logger.debug(f"           ✓ Added {page_count} page(s) (took {file_time:.2f}s)")
#             except Exception as e:
#                 logger.warning(f"         [{idx}/{len(files_to_merge)}] ✗ Could not merge {item.get('path', 'Unknown')}: {type(e).__name__}: {str(e)}")
#                 continue
        
#         # Save this batch to a temporary file
#         batch_filename = os.path.join(intermediate_dir, f"batch_{batch_num + 1}.pdf")
#         logger.debug(f"         Saving batch {batch_num + 1} to intermediate file...")
#         batch_doc.save(batch_filename, garbage=4, deflate=True)
#         batch_doc.close()
        
#         # Add batch TOC entries to main TOC
#         toc.extend(batch_toc)
#         total_pages += batch_pages
#         batch_files.append(batch_filename)
        
#         # Force garbage collection to free memory
#         gc.collect()
#         logger.info(f"         ✓ Batch {batch_num + 1} complete: {batch_pages} pages (total so far: {total_pages})")
    
#     # Now merge all batch files into final document
#     logger.info(f"         Merging {len(batch_files)} batch files into final document...")
#     final_doc = pymupdf.open()
    
#     for batch_idx, batch_file in enumerate(batch_files):
#         try:
#             logger.debug(f"         Merging batch file {batch_idx + 1}/{len(batch_files)}...")
#             batch_doc = pymupdf.open(batch_file)
#             final_doc.insert_pdf(batch_doc)
#             batch_doc.close()
            
#             # Clean up intermediate file immediately
#             try:
#                 os.remove(batch_file)
#             except:
#                 pass
            
#             # Force garbage collection
#             gc.collect()
#         except Exception as e:
#             logger.warning(f"         ✗ Could not merge batch file {batch_file}: {type(e).__name__}: {str(e)}")
#             continue
    
#     # Set final TOC
#     logger.info(f"         Setting table of contents ({len(toc)} entries)...")
#     final_doc.set_toc(toc)
    
#     # Save final document
#     logger.info(f"         Saving final merged PDF...")
#     save_start = time.time()
#     final_doc.save(final_output_path, garbage=4, deflate=True)
#     save_time = time.time() - save_start
    
#     # Verify the saved PDF page count
#     actual_page_count = len(final_doc)
#     final_doc.close()
    
#     # Double-check by reopening the saved file
#     verify_doc = pymupdf.open(final_output_path)
#     verified_page_count = len(verify_doc)
#     verify_doc.close()
    
#     if verified_page_count != actual_page_count:
#         logger.warning(f"         ⚠ Page count mismatch: Document shows {actual_page_count} but saved file has {verified_page_count}")
#         actual_page_count = verified_page_count
    
#     # Clean up any remaining intermediate files
#     try:
#         shutil.rmtree(intermediate_dir)
#     except:
#         pass
    
#     final_size = os.path.getsize(final_output_path) / (1024 * 1024)  # MB
#     logger.info(f"         ✓ Saved ({final_size:.2f} MB, {actual_page_count} pages, took {save_time:.2f}s)")
    
#     _cleanup_after_merge(temp_dir, files_merged, len(files_to_merge), actual_page_count, merge_start)
    
#     return final_output_path, actual_page_count

# def merge_pdfs(files_to_merge: List[Dict], project_name: str, temp_dir: str) -> tuple[str, int]:
#     """
#     Memory-Safe Merge (Fixed): Merges PDFs using an incremental disk-based approach.
#     Now correctly passes the filename to the save() method to prevent batch errors.
#     """
#     import gc
    
#     merge_start = time.time()
#     logger.info(f"          Starting Memory-Safe PDF merge...")
#     logger.info(f"          Files to merge: {len(files_to_merge)}")
    
#     # 1. Clean up old PDFs to ensure disk space
#     cleanup_old_pdfs(max_files=10)
    
#     # 2. Setup paths
#     clean_proj = re.sub(r'[^a-zA-Z0-9]', '_', project_name)
#     final_filename = f"{clean_proj}_Merged.pdf"
#     final_output_path = os.path.join("output", final_filename)
    
#     # 3. Create intermediate batches
#     BATCH_SIZE = 50
#     intermediate_dir = os.path.join(temp_dir, "batches")
#     os.makedirs(intermediate_dir, exist_ok=True)
    
#     batch_files = []
#     toc = []
#     total_pages = 0
#     current_discipline = None
    
#     # --- PHASE A: CREATE BATCHES (In-Memory -> Temp File) ---
#     # We build the TOC here assuming all batches will merge successfully
#     num_batches = (len(files_to_merge) + BATCH_SIZE - 1) // BATCH_SIZE
    
#     for i in range(0, len(files_to_merge), BATCH_SIZE):
#         batch_num = (i // BATCH_SIZE) + 1
#         batch_items = files_to_merge[i : i + BATCH_SIZE]
        
#         logger.info(f"          Processing Batch {batch_num}/{num_batches}...")
        
#         # Create a new document for just this batch
#         with pymupdf.open() as batch_doc:
#             for item in batch_items:
#                 try:
#                     with pymupdf.open(item['path']) as src_doc:
#                         batch_doc.insert_pdf(src_doc)
                        
#                         # Build TOC entries
#                         # Note: We track page numbers globally (total_pages + current batch offset)
#                         current_page_num = total_pages + len(batch_doc) - len(src_doc) + 1
                        
#                         if item['discipline'] != current_discipline:
#                             toc.append([1, item['discipline'], current_page_num])
#                             current_discipline = item['discipline']
                        
#                         toc.append([2, item['title'], current_page_num])
                        
#                 except Exception as e:
#                     logger.warning(f"          ⚠ Skipping corrupt file {item.get('title')}: {e}")

#             # Save this batch to disk
#             batch_path = os.path.join(intermediate_dir, f"batch_{batch_num}.pdf")
#             batch_doc.save(batch_path, garbage=4, deflate=True)
#             batch_files.append(batch_path)
            
#             # Update global page counter
#             total_pages += len(batch_doc)
            
#         gc.collect()

#     if not batch_files:
#         raise Exception("No valid batches were created.")

#     # --- PHASE B: INCREMENTAL MERGE (Disk -> Disk) ---
#     logger.info(f"          Assembling final PDF from {len(batch_files)} batches...")
    
#     # 1. Start by copying the first batch to the final destination
#     shutil.copy(batch_files[0], final_output_path)
    
#     # 2. Append subsequent batches
#     if len(batch_files) > 1:
#         for batch_path in batch_files[1:]:
#             try:
#                 # Open final doc
#                 doc_final = pymupdf.open(final_output_path)
#                 # Open batch doc
#                 doc_batch = pymupdf.open(batch_path)
                
#                 # Insert
#                 doc_final.insert_pdf(doc_batch)
                
#                 # FIX: Explicitly pass the filename to save()
#                 # incremental=True makes it just append data rather than rewrite
#                 doc_final.save(
#                     final_output_path, 
#                     incremental=True, 
#                     encryption=pymupdf.PDF_ENCRYPT_KEEP
#                 )
                
#                 doc_final.close()
#                 doc_batch.close()
                
#                 # Clean up immediately
#                 os.remove(batch_path)
#                 gc.collect()
                
#             except Exception as e:
#                 logger.error(f"          ✗ Error merging batch {batch_path}: {e}")
#                 # We stop here to prevent a corrupt TOC later
#                 raise Exception(f"Merge failed at {os.path.basename(batch_path)}: {e}")

#     # --- PHASE C: FINALIZE (TOC & CLEANUP) ---
#     logger.info(f"          Finalizing TOC and compressing...")
    
#     try:
#         doc_final = pymupdf.open(final_output_path)
        
#         # Verify page count matches TOC expectations
#         # If pages are missing, the TOC will throw errors, so we trim the TOC if needed
#         final_page_count = len(doc_final)
#         valid_toc = [entry for entry in toc if entry[2] <= final_page_count]
        
#         if len(valid_toc) < len(toc):
#             logger.warning(f"          ⚠ Trimmed {len(toc) - len(valid_toc)} TOC entries due to missing pages.")
            
#         doc_final.set_toc(valid_toc)
        
#         # Clean save (reconstruct file to remove incremental fragmentation)
#         temp_final = final_output_path + ".tmp"
#         doc_final.save(temp_final, garbage=4, deflate=True)
#         doc_final.close()
        
#         os.replace(temp_final, final_output_path)
        
#     except Exception as e:
#         logger.warning(f"          ⚠ Final optimization warning: {e}")

#     # Final Verification
#     doc_verify = pymupdf.open(final_output_path)
#     actual_page_count = len(doc_verify)
#     doc_verify.close()
    
#     # Cleanup temp dir
#     try:
#         shutil.rmtree(temp_dir)
#     except:
#         pass
    
#     total_time = time.time() - merge_start
#     final_size_mb = os.path.getsize(final_output_path) / (1024 * 1024)
    
#     logger.info(f"          ✓ MERGE COMPLETE: {actual_page_count} pages, {final_size_mb:.2f} MB")
#     logger.info(f"          Total processing time: {total_time:.2f}s")
    
#     return final_output_path, actual_page_count

# def merge_pdfs(files_to_merge: List[Dict], project_name: str, temp_dir: str) -> tuple[str, int]:
#     """
#     Memory-Safe Merge (Low RAM Mode): Optimized for Render.com Free Tier (512MB).
#     Changes:
#     1. BATCH_SIZE reduced to 10 (was 50) to prevent OOM kills.
#     2. Intermediate compression disabled to save RAM.
#     """
#     import gc
    
#     merge_start = time.time()
#     logger.info(f"          Starting Low-RAM PDF merge...")
#     logger.info(f"          Files to merge: {len(files_to_merge)}")
    
#     # 1. Clean up old PDFs to ensure disk space
#     cleanup_old_pdfs(max_files=5) # Keep fewer files to save disk space
    
#     # 2. Setup paths
#     clean_proj = re.sub(r'[^a-zA-Z0-9]', '_', project_name)
#     final_filename = f"{clean_proj}_Merged.pdf"
#     final_output_path = os.path.join("output", final_filename)
    
#     # 3. Create intermediate batches
#     # CRITICAL: Reduced to 10 to fit in 512MB RAM
#     BATCH_SIZE = 10 
#     intermediate_dir = os.path.join(temp_dir, "batches")
#     os.makedirs(intermediate_dir, exist_ok=True)
    
#     batch_files = []
#     toc = []
#     total_pages = 0
#     current_discipline = None
    
#     # --- PHASE A: CREATE BATCHES (In-Memory -> Temp File) ---
#     num_batches = (len(files_to_merge) + BATCH_SIZE - 1) // BATCH_SIZE
    
#     for i in range(0, len(files_to_merge), BATCH_SIZE):
#         batch_num = (i // BATCH_SIZE) + 1
#         batch_items = files_to_merge[i : i + BATCH_SIZE]
        
#         logger.info(f"          Processing Batch {batch_num}/{num_batches} (Size: {len(batch_items)})...")
        
#         # Create a new document for just this batch
#         with pymupdf.open() as batch_doc:
#             for item in batch_items:
#                 try:
#                     with pymupdf.open(item['path']) as src_doc:
#                         batch_doc.insert_pdf(src_doc)
                        
#                         # Build TOC entries
#                         current_page_num = total_pages + len(batch_doc) - len(src_doc) + 1
                        
#                         if item['discipline'] != current_discipline:
#                             toc.append([1, item['discipline'], current_page_num])
#                             current_discipline = item['discipline']
                        
#                         toc.append([2, item['title'], current_page_num])
                        
#                 except Exception as e:
#                     logger.warning(f"          ⚠ Skipping corrupt file {item.get('title')}: {e}")

#             # Save this batch to disk
#             batch_path = os.path.join(intermediate_dir, f"batch_{batch_num}.pdf")
            
#             # CRITICAL: deflate=False saves RAM/CPU during batch creation
#             # We don't need compressed batches, only the final file needs compression.
#             batch_doc.save(batch_path, garbage=0, deflate=False)
#             batch_files.append(batch_path)
            
#             # Update global page counter
#             total_pages += len(batch_doc)
            
#         # Force memory release immediately
#         gc.collect()

#     if not batch_files:
#         raise Exception("No valid batches were created.")

#     # --- PHASE B: INCREMENTAL MERGE (Disk -> Disk) ---
#     logger.info(f"          Assembling final PDF from {len(batch_files)} batches...")
    
#     # 1. Start by copying the first batch
#     shutil.copy(batch_files[0], final_output_path)
    
#     # 2. Append subsequent batches
#     if len(batch_files) > 1:
#         for idx, batch_path in enumerate(batch_files[1:], start=2):
#             try:
#                 # Open final doc
#                 doc_final = pymupdf.open(final_output_path)
#                 # Open batch doc
#                 doc_batch = pymupdf.open(batch_path)
                
#                 # Insert
#                 doc_final.insert_pdf(doc_batch)
                
#                 # Save incrementally
#                 doc_final.save(
#                     final_output_path, 
#                     incremental=True, 
#                     encryption=pymupdf.PDF_ENCRYPT_KEEP
#                 )
                
#                 doc_final.close()
#                 doc_batch.close()
                
#                 # Clean up batch file immediately to free disk space
#                 os.remove(batch_path)
                
#                 # Log progress periodically
#                 if idx % 5 == 0:
#                     logger.info(f"          ... Merged batch {idx}/{len(batch_files)}")
#                     gc.collect()
                
#             except Exception as e:
#                 logger.error(f"          ✗ Error merging batch {batch_path}: {e}")
#                 raise Exception(f"Merge failed at {os.path.basename(batch_path)}: {e}")

#     # Remove the first batch file (it was copied, not removed in loop)
#     if os.path.exists(batch_files[0]):
#         os.remove(batch_files[0])

#     # --- PHASE C: FINALIZE (TOC & COMPRESS) ---
#     logger.info(f"          Finalizing TOC and Compressing (High RAM Step)...")
    
#     try:
#         # Re-open for final polish
#         doc_final = pymupdf.open(final_output_path)
        
#         # Validate TOC
#         final_page_count = len(doc_final)
#         valid_toc = [entry for entry in toc if entry[2] <= final_page_count]
        
#         if len(valid_toc) < len(toc):
#             logger.warning(f"          ⚠ Trimmed {len(toc) - len(valid_toc)} TOC entries.")
            
#         doc_final.set_toc(valid_toc)
        
#         # Clean save with Compression (Deflate)
#         # This is the only step that uses high RAM, but we cleared everything else
#         temp_final = final_output_path + ".tmp"
#         doc_final.save(
#             temp_final, 
#             garbage=4, 
#             deflate=True, # Compress ONLY at the very end
#             deflate_images=False # Skip image re-compression to save RAM/Time
#         )
#         doc_final.close()
        
#         os.replace(temp_final, final_output_path)
        
#     except Exception as e:
#         logger.warning(f"          ⚠ Optimization warning: {e}")

#     # Final Verification
#     doc_verify = pymupdf.open(final_output_path)
#     actual_page_count = len(doc_verify)
#     doc_verify.close()
    
#     # Cleanup temp dir
#     try:
#         shutil.rmtree(temp_dir)
#     except:
#         pass

#     total_time = time.time() - merge_start
#     final_size_mb = os.path.getsize(final_output_path) / (1024 * 1024)
    
#     logger.info(f"          ✓ MERGE COMPLETE: {actual_page_count} pages, {final_size_mb:.2f} MB")
#     logger.info(f"          Total processing time: {total_time:.2f}s")
    
#     return final_output_path, actual_page_count

def merge_pdfs(files_to_merge: List[Dict], project_name: str, temp_dir: str) -> tuple[str, int]:
    """
    Standard Tier Merge: Optimized for 2GB RAM.
    - Uses Batching for safety.
    - DISABLES final compression to prevent memory crashes.
    """
    merge_start = time.time()
    logger.info(f"          Starting PDF merge (Low Memory Mode: {settings.LOW_MEMORY_MODE})...")
    logger.info(f"          Files to merge: {len(files_to_merge)}")
    
    # 1. Clean up old PDFs
    cleanup_old_pdfs(max_files=10)
    
    # 2. Setup paths
    clean_proj = re.sub(r'[^a-zA-Z0-9]', '_', project_name)
    final_filename = f"{clean_proj}_Merged.pdf"
    final_output_path = os.path.join("output", final_filename)
    
    # 3. Setup Batches - use configurable batch size (default 15 for low memory)
    BATCH_SIZE = settings.MERGE_BATCH_SIZE
    logger.info(f"          Merge batch size: {BATCH_SIZE}")
    intermediate_dir = os.path.join(temp_dir, "batches")
    os.makedirs(intermediate_dir, exist_ok=True)
    
    batch_files = []
    toc = []
    total_pages = 0
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
                        batch_doc.insert_pdf(src_doc)
                        
                        # Build TOC
                        current_page_num = total_pages + len(batch_doc) - len(src_doc) + 1
                        if item['discipline'] != current_discipline:
                            toc.append([1, item['discipline'], current_page_num])
                            current_discipline = item['discipline']
                        toc.append([2, item['title'], current_page_num])
                        
                    # Mark source file for deletion (progressive cleanup)
                    if settings.LOW_MEMORY_MODE:
                        source_files_to_delete.append(item['path'])
                        
                except Exception as e:
                    logger.warning(f"          ⚠ Skipping corrupt file {item.get('title')}: {e}")

            # Save batch
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
                
                os.remove(batch_path)
                if idx % 5 == 0: gc.collect()
                
            except Exception as e:
                logger.error(f"          ✗ Error merging batch {batch_path}: {e}")
                raise Exception(f"Merge failed at batch {os.path.basename(batch_path)}")

    if os.path.exists(batch_files[0]):
        os.remove(batch_files[0])

    # --- PHASE C: FINALIZE (NO COMPRESSION) ---
    logger.info(f"          Finalizing TOC (No Compression)...")
    
    try:
        doc_final = pymupdf.open(final_output_path)
        
        # Safe TOC set
        final_page_count = len(doc_final)
        valid_toc = [entry for entry in toc if entry[2] <= final_page_count]
        doc_final.set_toc(valid_toc)
        
        # CRITICAL FIX: deflate=False ensures we don't spike RAM
        # garbage=3 cleans unused objects without deep inspection
        temp_final = final_output_path + ".tmp"
        doc_final.save(
            temp_final, 
            garbage=3,      # Clean unused objects
            deflate=False   # DO NOT COMPRESS (Prevents OOM Crash)
        )
        doc_final.close()
        
        os.replace(temp_final, final_output_path)
        
    except Exception as e:
        logger.warning(f"          ⚠ Final save warning: {e}")

    # Verify
    doc_verify = pymupdf.open(final_output_path)
    actual_page_count = len(doc_verify)
    doc_verify.close()
    
    try:
        shutil.rmtree(temp_dir)
    except:
        pass

    total_time = time.time() - merge_start
    final_size_mb = os.path.getsize(final_output_path) / (1024 * 1024)
    
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
