import os
import re
import time
import shutil
import asyncio
import aiohttp
import pymupdf
from datetime import datetime
from typing import List, Dict, Any, Optional, Callable
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import logging

from config import settings

logger = logging.getLogger(__name__)


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

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def _make_request(self, method: str, url: str, **kwargs):
        """Make HTTP request with retry logic."""
        if not self.session:
            connector = aiohttp.TCPConnector(ssl=self.ssl_context)
            self.session = aiohttp.ClientSession(connector=connector)
        
        try:
            async with self.session.request(method, url, **kwargs) as response:
                if response.status == 429:  # Rate limit
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limited. Waiting {retry_after}s")
                    await asyncio.sleep(retry_after)
                    raise aiohttp.ClientError("Rate limited, retrying")
                
                response.raise_for_status()
                return await response.json() if response.content_type == 'application/json' else await response.text()
        except Exception as e:
            logger.error(f"Request failed: {method} {url} - {e}")
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
        """Renames a file (adds timestamp) and moves it to Archive."""
        endpoint = f"{self.base_url}/rest/v1.0/files/{file_id}"
        params = {"project_id": self.project_id}
        
        timestamp = int(time.time())
        name_part, ext_part = os.path.splitext(current_name)
        clean_name = name_part.replace(" old", "")
        new_name = f"{clean_name}_archived_{timestamp}{ext_part}"

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

    async def upload_file(self, local_file_path: str, target_folder_id: int) -> str:
        """Upload a file to Procore."""
        endpoint = f"{self.base_url}/rest/v1.0/files"
        params = {"project_id": self.project_id}
        file_name = os.path.basename(local_file_path)

        try:
            with open(local_file_path, 'rb') as f:
                data = aiohttp.FormData()
                data.add_field('file[data]', f, filename=file_name, content_type='application/pdf')
                data.add_field('file[parent_id]', str(target_folder_id))
                data.add_field('file[name]', file_name)
                
                if not self.session:
                    self.session = aiohttp.ClientSession()
                
                async with self.session.post(endpoint, headers=self.headers, params=params, data=data) as response:
                    if response.status not in [200, 201]:
                        text = await response.text()
                        return f"Error: {text}"
                    
                    logger.info(f"Successfully uploaded '{file_name}'")
                    return "Success"
        except Exception as e:
            logger.error(f"Error uploading file: {e}")
            return str(e)

    async def process_file_update(self, file_path: str, target_folder_id: int, archive_folder_id: Optional[int]) -> str:
        """Clean up existing files and upload new one."""
        logger.info("--- Processing Clean Up & Upload ---")

        # 1. Clean Sweep: Move ALL existing files in target -> Archive
        existing_files = await self.get_all_files_in_folder(target_folder_id)
        
        if existing_files:
            logger.info(f"Found {len(existing_files)} file(s) to archive.")
            
            # FAILSAFE: If Archive ID is missing, try to CREATE it
            if not archive_folder_id:
                logger.warning("Archive folder not found. Creating 'Archive' folder now...")
                archive_folder_id = await self._create_archive_folder(target_folder_id)

            if archive_folder_id:
                # Archive files concurrently
                archive_tasks = [
                    self.move_and_rename_file(f['id'], f['name'], archive_folder_id)
                    for f in existing_files
                ]
                await asyncio.gather(*archive_tasks)
            else:
                logger.error("Critical: Could not find or create Archive folder. Old files stay.")
        else:
            logger.info("Target folder is empty. Clean start.")

        # 2. Upload New
        return await self.upload_file(file_path, target_folder_id)

    async def _create_archive_folder(self, parent_folder_id: int) -> Optional[int]:
        """Create Archive subfolder."""
        try:
            create_url = f"{self.base_url}/rest/v1.0/projects/{self.project_id}/folders"
            create_data = {"folder": {"name": "Archive", "parent_id": parent_folder_id}}
            create_head = self.headers.copy()
            create_head["Content-Type"] = "application/json"
            
            data = await self._make_request('POST', create_url, headers=create_head, json=create_data)
            archive_id = data.get('id')
            logger.info(f"Created Archive Folder: {archive_id}")
            return archive_id
        except:
            return None

    async def close(self):
        """Close the session."""
        if self.session:
            await self.session.close()


# ---------------------------------------------------------
# SECTION 2: ASYNC PROCORE API CLIENT
# ---------------------------------------------------------

class AsyncProcoreClient:
    """Async client for Procore API calls."""
    
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
    
    async def _ensure_session(self):
        """Ensure session is created."""
        if not self.session:
            import ssl
            timeout = aiohttp.ClientTimeout(total=settings.DOWNLOAD_TIMEOUT)
            
            # Create SSL context with proper certificate handling
            ssl_context = ssl.create_default_context()
            
            # Try to use certifi certificates if available
            try:
                import certifi
                ssl_context = ssl.create_default_context(cafile=certifi.where())
                logger.info("Using certifi SSL certificates")
            except ImportError:
                logger.warning("certifi not available, using system certificates")
            except Exception as ssl_error:
                logger.error(f"SSL configuration error: {ssl_error}")
                
                # Development fallback: disable SSL verification
                if settings.ENVIRONMENT == "development":
                    logger.warning("⚠️  SSL VERIFICATION DISABLED (DEVELOPMENT ONLY)")
                    logger.warning("⚠️  Fix by running: /Applications/Python\\ 3.14/Install\\ Certificates.command")
                    ssl_context.check_hostname = False
                    ssl_context.verify_mode = ssl.CERT_NONE
                else:
                    # Production: don't disable SSL
                    raise Exception("SSL certificate verification failed. Install certificates: pip install --upgrade certifi")
            
            connector = aiohttp.TCPConnector(ssl=ssl_context)
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
        """Make GET request with retry logic."""
        await self._ensure_session()
        url = f"{self.base_url}{endpoint}"
        
        try:
            async with self.session.get(url, headers=self.headers, params=params) as response:
                if response.status == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limited. Waiting {retry_after}s")
                    await asyncio.sleep(retry_after)
                    raise aiohttp.ClientError("Rate limited")
                
                # Don't raise for 401/403 - let caller handle auth errors
                if response.status in [401, 403]:
                    error_text = await response.text()
                    logger.warning(f"Auth error {response.status}: {error_text}")
                    error = aiohttp.ClientResponseError(
                        request_info=response.request_info,
                        history=response.history,
                        status=response.status,
                        message=error_text
                    )
                    raise error
                
                response.raise_for_status()
                
                # Try to parse JSON
                try:
                    return await response.json()
                except Exception as json_error:
                    text = await response.text()
                    logger.error(f"Failed to parse JSON response: {json_error}. Response: {text[:200]}")
                    raise aiohttp.ClientError(f"Invalid JSON response: {str(json_error)}")
                    
        except aiohttp.ClientResponseError as e:
            # Re-raise HTTP errors (401, 403, 404, etc.)
            logger.error(f"HTTP {e.status} error for {url}: {e.message}")
            raise
        except Exception as e:
            logger.error(f"GET request failed: {url} - {type(e).__name__}: {e}")
            raise
    
    async def download_file(self, url: str, destination: str) -> bool:
        """Download a file from URL."""
        await self._ensure_session()
        
        try:
            async with self.session.get(url) as response:
                response.raise_for_status()
                
                with open(destination, 'wb') as f:
                    while True:
                        chunk = await response.content.read(8192)
                        if not chunk:
                            break
                        f.write(chunk)
                
                return True
        except Exception as e:
            logger.error(f"Download failed: {url} - {e}")
            return False
    
    async def close(self):
        """Close the session."""
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
        try:
            data = await client.get(f"/rest/v1.0/projects/{project_id}/folders")
            root_folders = data if isinstance(data, list) else data.get('folders', [])
        except:
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
    """Download a drawing and apply markups."""
    try:
        current_rev = drawing.get('current_revision', {})
        d_num = drawing.get('number', 'NoNum')
        d_title = drawing.get('title', 'NoTitle')
        d_rev = current_rev.get('revision_number', '0')
        pdf_url = current_rev.get('pdf_url')
        
        if not pdf_url:
            logger.warning(f"No PDF URL for drawing {d_num}")
            return None

        # Create clean filename
        clean_num = re.sub(r'[^\w\.-]', '', d_num)
        clean_title = re.sub(r'[^a-zA-Z0-9]', '-', d_title).strip('-')
        clean_rev = re.sub(r'[^\w]', '', str(d_rev))
        final_name = f"{clean_num}-{clean_title}-Rev.{clean_rev}.pdf"
        filename = os.path.join(temp_dir, final_name)

        # Download PDF
        success = await client.download_file(pdf_url, filename)
        if not success:
            return None

        # Fetch markups
        revision_id = current_rev.get('id')
        if revision_id:
            m_params = {
                "project_id": project_id,
                "holder_id": revision_id,
                "holder_type": "DrawingRevision",
                "page": 1,
                "page_size": 200
            }
            
            try:
                markup_data = await client.get("/rest/v1.0/markup_layers", params=m_params)
                markups = []
                for layer in markup_data:
                    if "markup" in layer:
                        markups.extend(layer["markup"])
                
                # Apply markups if any exist
                if markups:
                    await asyncio.to_thread(apply_markups_to_pdf, filename, markups, drawing)
            except Exception as e:
                logger.warning(f"Could not fetch/apply markups for {d_num}: {e}")
        
        return {
            "path": filename,
            "title": final_name.replace(".pdf", ""),
            "discipline": discipline_name
        }
    except Exception as e:
        logger.error(f"Error processing drawing {drawing.get('number', 'unknown')}: {e}")
        return None


def apply_markups_to_pdf(filename: str, markups: List[Dict], drawing: Dict):
    """Apply markups to PDF (CPU-intensive, run in thread)."""
    try:
        doc = pymupdf.open(filename)
        if len(doc) == 0:
            doc.close()
            return
        
        page = doc[0]
        coord_w = drawing.get('width', 6400) or 6400
        coord_h = drawing.get('height', 4524) or 4524
        scale_x = page.rect.width / float(coord_w)
        scale_y = page.rect.height / float(coord_h)
        
        shape = page.new_shape()
        for m in markups:
            if all(k in m for k in ("x", "y", "w", "h")):
                px = float(m["x"]) * scale_x
                py = float(m["y"]) * scale_y
                pw = float(m["w"]) * scale_x
                ph = float(m["h"]) * scale_y
                rect = pymupdf.Rect(px, py, px + pw, py + ph)
                shape.draw_rect(rect)
                shape.finish(color=None, fill=(0, 1, 1), fill_opacity=0.4)
        
        shape.commit()
        temp_f = filename.replace(".pdf", "_temp.pdf")
        doc.save(temp_f, garbage=4, deflate=True)
        doc.close()
        
        if os.path.exists(temp_f):
            os.replace(temp_f, filename)
    except Exception as e:
        logger.error(f"Error applying markups: {e}")


# ---------------------------------------------------------
# SECTION 5: MAIN JOB RUNNER (ASYNC)
# ---------------------------------------------------------

async def run_job_async(
    project_id: int,
    target_disciplines: List[str],
    access_token: str,
    progress_callback: Optional[Callable[[int], None]] = None
) -> str:
    """Main async job runner with parallel downloads."""
    logger.info(f"--- GENERATING PDF: Project {project_id} ---")
    
    client = AsyncProcoreClient(access_token, settings.PROCORE_COMPANY_ID)
    
    try:
        # Fetch project info
        project_data = await client.get(f"/rest/v1.0/projects/{project_id}", params={"company_id": settings.PROCORE_COMPANY_ID})
        project_name = project_data.get('name', 'Project')

        # Find IFC drawing area
        areas_data = await client.get(f"/rest/v1.1/projects/{project_id}/drawing_areas")
        area_id = None
        for a in areas_data:
            if a['name'].strip().upper() == "IFC":
                area_id = a['id']
                break
        
        if not area_id:
            raise Exception("No IFC Area found")

        # Fetch all drawings (paginated)
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

        # Filter by discipline
        final_batch = []
        for dwg in all_drawings:
            d_name = "Unknown"
            if dwg.get("drawing_discipline"):
                d_name = dwg["drawing_discipline"].get("name", "Unknown")
            elif dwg.get("discipline"):
                d_name = dwg["discipline"]
            
            if d_name in target_disciplines:
                final_batch.append((dwg, d_name))

        if not final_batch:
            raise Exception("No drawings found for selected disciplines")

        logger.info(f"Processing {len(final_batch)} drawings")

        # Create temp directory
        job_timestamp = int(time.time())
        temp_dir = f"temp_job_{project_id}_{job_timestamp}"
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        os.makedirs(temp_dir)
        if not os.path.exists("output"):
            os.makedirs("output")

        # Download and process drawings in parallel batches
        files_to_merge = []
        total_files = len(final_batch)
        
        # Process in batches to respect rate limits
        batch_size = settings.MAX_CONCURRENT_DOWNLOADS
        for batch_start in range(0, total_files, batch_size):
            batch_end = min(batch_start + batch_size, total_files)
            batch = final_batch[batch_start:batch_end]
            
            # Progress update
            if progress_callback:
                percent = int((batch_start / total_files) * 90)
                progress_callback(percent)
            
            # Download batch concurrently
            tasks = [
                download_and_process_drawing(client, dwg, d_name, temp_dir, project_id)
                for dwg, d_name in batch
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if result and not isinstance(result, Exception):
                    files_to_merge.append(result)
            
            # Small delay between batches
            if batch_end < total_files:
                await asyncio.sleep(0.5)

        if not files_to_merge:
            raise Exception("No PDFs generated successfully")

        logger.info(f"Successfully processed {len(files_to_merge)}/{total_files} drawings")

        # Merging phase (90-95%)
        if progress_callback:
            progress_callback(92)

        # Merge PDFs (CPU-intensive, run in thread)
        final_output_path = await asyncio.to_thread(
            merge_pdfs,
            files_to_merge,
            project_name,
            temp_dir
        )

        return final_output_path

    finally:
        await client.close()


def merge_pdfs(files_to_merge: List[Dict], project_name: str, temp_dir: str) -> str:
    """Merge PDFs with TOC (CPU-intensive operation)."""
    logger.info("Merging PDFs...")
    
    merged_doc = pymupdf.open()
    toc = []
    current_discipline = None
    
    for item in files_to_merge:
        try:
            doc_single = pymupdf.open(item['path'])
            
            # Add discipline header to TOC
            if item['discipline'] != current_discipline:
                toc.append([1, item['discipline'], len(merged_doc) + 1])
                current_discipline = item['discipline']
            
            # Add drawing to TOC
            toc.append([2, item['title'], len(merged_doc) + 1])
            
            # Insert pages
            merged_doc.insert_pdf(doc_single)
            doc_single.close()
        except Exception as e:
            logger.warning(f"Could not merge {item['path']}: {e}")
            continue
    
    merged_doc.set_toc(toc)
    
    # Generate filename
    clean_proj = re.sub(r'[^a-zA-Z0-9]', '_', project_name)
    date_str = datetime.now().strftime("%d_%m_%y")
    final_filename = f"{clean_proj}_Merged_{date_str}.pdf"
    
    final_output_path = os.path.join("output", final_filename)
    merged_doc.save(final_output_path, garbage=4, deflate=True)
    merged_doc.close()
    
    # Cleanup temp directory
    try:
        shutil.rmtree(temp_dir)
    except:
        pass
    
    logger.info(f"PDF merge complete: {final_output_path}")
    return final_output_path


# ---------------------------------------------------------
# SYNC WRAPPER (for backward compatibility)
# ---------------------------------------------------------

def run_job_api(
    project_id: int,
    target_disciplines: List[str],
    access_token: str,
    progress_callback: Optional[Callable[[int], None]] = None
) -> str:
    """Synchronous wrapper for async job runner."""
    return asyncio.run(run_job_async(project_id, target_disciplines, access_token, progress_callback))


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
