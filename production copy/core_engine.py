import os
import re
import time
import shutil
import requests
import pymupdf 
from datetime import datetime

# --- CONFIGURATION ---
BASE_URL = "https://api.procore.com/rest/v1.0"      
BASE_URL_V1_1 = "https://api.procore.com/rest/v1.1" 
FIXED_COMPANY_ID = 4907 

def get_headers(access_token):
    return {
        "Authorization": f"Bearer {access_token}",
        "Procore-Company-Id": str(FIXED_COMPANY_ID),
        "Accept": "application/json"
    }

# ---------------------------------------------------------
# SECTION 1: THE PROCORE MANAGER
# ---------------------------------------------------------

class ProcoreDocManager:
    def __init__(self, company_id, project_id, access_token):
        self.company_id = company_id
        self.project_id = project_id
        self.base_url = "https://api.procore.com"
        self.access_token = access_token
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Procore-Company-Id": str(self.company_id),
            "Accept": "application/json"
        }

    def get_all_files_in_folder(self, folder_id):
        """Returns a list of ALL files by requesting the folder contents."""
        # FIX: Remove '/files' from the end. We query the folder directly.
        endpoint = f"{self.base_url}/rest/v1.0/folders/{folder_id}"
        params = {"project_id": self.project_id}
        
        try:
            response = requests.get(endpoint, headers=self.headers, params=params)
            if response.status_code == 200:
                data = response.json()
                # The response object has 'files': [...] inside it
                return data.get('files', []) 
            elif response.status_code == 404:
                print(f"⚠️ Folder {folder_id} not found (404).")
                return []
            else:
                print(f"⚠️ Could not list files (Status {response.status_code})")
                return []
        except Exception as e:
            print(f"❌ Error listing files: {e}")
            return []

    def move_and_rename_file(self, file_id, current_name, destination_folder_id):
        """Renames a file (adds timestamp) and moves it to Archive."""
        endpoint = f"{self.base_url}/rest/v1.0/files/{file_id}"
        params = {"project_id": self.project_id}
        
        timestamp = int(time.time())
        name_part, ext_part = os.path.splitext(current_name)
        clean_name = name_part.replace(" old", "") 
        new_name = f"{clean_name}_archived_{timestamp}{ext_part}"

        payload = { "file": { "name": new_name, "parent_id": destination_folder_id } }
        headers = self.headers.copy()
        headers["Content-Type"] = "application/json"

        try:
            response = requests.patch(endpoint, headers=headers, params=params, json=payload)
            response.raise_for_status()
            print(f"✅ Archived: {current_name} -> {new_name}")
            return True
        except Exception as e:
            print(f"❌ Error archiving file {current_name}: {e}")
            return False

    def upload_file(self, local_file_path, target_folder_id):
        endpoint = f"{self.base_url}/rest/v1.0/files"
        params = {"project_id": self.project_id}
        file_name = os.path.basename(local_file_path)

        try:
            with open(local_file_path, 'rb') as f:
                files = { 'file[data]': (file_name, f, 'application/pdf') }
                data = { 'file[parent_id]': target_folder_id, 'file[name]': file_name }
                response = requests.post(endpoint, headers=self.headers, params=params, files=files, data=data)
                
                if response.status_code not in [200, 201]: return f"Error: {response.text}"
                print(f"✅ Successfully uploaded '{file_name}'")
                return "Success"
        except Exception as e:
            print(f"❌ Error uploading file: {e}")
            return str(e)

    def process_file_update(self, file_path, target_folder_id, archive_folder_id):
        print(f"--- Processing Clean Up & Upload ---")

        # 1. Clean Sweep: Move ALL existing files in target -> Archive
        existing_files = self.get_all_files_in_folder(target_folder_id)
        
        if existing_files:
            print(f"Found {len(existing_files)} file(s) to archive.")
            
            # FAILSAFE: If Archive ID is missing, try to CREATE it
            if not archive_folder_id:
                print("⚠️ Archive folder not found. Creating 'Archive' folder now...")
                try:
                    create_url = f"{self.base_url}/rest/v1.0/projects/{self.project_id}/folders"
                    create_data = { "folder": { "name": "Archive", "parent_id": target_folder_id } }
                    create_head = self.headers.copy(); create_head["Content-Type"] = "application/json"
                    r_create = requests.post(create_url, headers=create_head, json=create_data)
                    if r_create.status_code in [200, 201]:
                        archive_folder_id = r_create.json()['id']
                        print(f"✅ Created Archive Folder: {archive_folder_id}")
                except: pass

            if archive_folder_id:
                for f in existing_files:
                    self.move_and_rename_file(f['id'], f['name'], archive_folder_id)
            else:
                print("❌ Critical: Could not find or create Archive folder. Old files stay.")
        else:
            print("Target folder is empty. Clean start.")

        # 2. Upload New
        return self.upload_file(file_path, target_folder_id)

# ---------------------------------------------------------
# SECTION 2: FOLDER FINDERS
# ---------------------------------------------------------

def find_folder_by_name_pattern(folders_list, search_terms):
    search_terms = [term.lower() for term in search_terms]
    for folder in folders_list:
        folder_name = folder.get('name', '').lower()
        if all(term in folder_name for term in search_terms):
            return folder
    return None

def get_target_combined_folder(project_id, access_token):
    headers = get_headers(access_token)
    
    # 1. Get Root (Handle both List and Dict returns)
    try:
        r = requests.get(f"{BASE_URL}/projects/{project_id}/folders", headers=headers)
        if r.status_code != 200: r = requests.get(f"{BASE_URL}/folders", headers=headers, params={"project_id": project_id})
        data = r.json()
        root_folders = data if isinstance(data, list) else data.get('folders', [])
    except: return None, None

    # 2. Find Drawing & Specs
    drawing_specs = find_folder_by_name_pattern(root_folders, ["drawing", "specs"])
    if not drawing_specs: return None, None

    # 3. Find Combined IFC
    try:
        r = requests.get(f"{BASE_URL}/folders/{drawing_specs['id']}", headers=headers, params={"project_id": project_id})
        data = r.json()
        subfolders = data.get('folders', []) if isinstance(data, dict) else []
    except: return None, None

    combined_ifc = find_folder_by_name_pattern(subfolders, ["combined", "ifc"])
    if not combined_ifc: return None, None
    
    # 4. Find Archive (Look for "Archive" OR "archive")
    try:
        r_comb = requests.get(f"{BASE_URL}/folders/{combined_ifc['id']}", headers=headers, params={"project_id": project_id})
        data_c = r_comb.json()
        comb_subs = data_c.get('folders', []) if isinstance(data_c, dict) else []
        
        archive = find_folder_by_name_pattern(comb_subs, ["archive"])
        archive_id = archive['id'] if archive else None
        
        return combined_ifc['id'], archive_id
    except:
        return combined_ifc['id'], None

def handle_procore_upload(project_id, local_filepath, access_token):
    print("--- STARTING PROCORE UPLOAD SEQUENCE ---")
    target_id, archive_id = get_target_combined_folder(project_id, access_token)
    
    if not target_id: return "Upload Skipped: Could not find 'Combined IFC' folder."
    
    manager = ProcoreDocManager(FIXED_COMPANY_ID, project_id, access_token)
    return manager.process_file_update(local_filepath, target_id, archive_id)

# ---------------------------------------------------------
# SECTION 3: DATA FETCHING
# ---------------------------------------------------------

def api_get_projects(access_token):
    headers = get_headers(access_token)
    try:
        r = requests.get(f"{BASE_URL}/projects", headers=headers, params={"company_id": FIXED_COMPANY_ID})
        if r.status_code == 401: return None
        if r.status_code == 200:
            projs = r.json(); projs.sort(key=lambda x: x['name']); return projs
    except: pass
    return []

def api_get_disciplines(project_id, access_token):
    headers = get_headers(access_token)
    try:
        r_area = requests.get(f"{BASE_URL_V1_1}/projects/{project_id}/drawing_areas", headers=headers)
        area_id = None
        for a in r_area.json():
            if a['name'].strip().upper() == "IFC": area_id = a['id']; break
        if not area_id: return {"error": "No 'IFC' Drawing Area found."}

        endpoint = f"{BASE_URL_V1_1}/drawing_areas/{area_id}/drawings"
        all_drawings = []
        page = 1
        while True:
            r = requests.get(endpoint, headers=headers, params={"project_id": project_id, "page": page, "per_page": 100})
            data = r.json()
            if not data: break
            all_drawings.extend(data)
            if len(data) < 100: break
            page += 1

        disciplines = set(); 
        for dwg in all_drawings:
            d_name = "Unknown"
            if dwg.get("drawing_discipline"): d_name = dwg["drawing_discipline"].get("name", "Unknown")
            elif dwg.get("discipline"): d_name = dwg["discipline"]
            disciplines.add(d_name)
        return {"area_id": area_id, "disciplines": sorted(list(disciplines))}
    except Exception as e: return {"error": str(e)}

# ---------------------------------------------------------
# SECTION 4: GENERATION ENGINE
# ---------------------------------------------------------

def run_job_api(project_id, target_disciplines, access_token, progress_callback=None):
    print(f"--- GENERATING PDF: Project {project_id} ---")
    headers = get_headers(access_token)
    
    # ... (Fetching Logic remains the same) ...
    r_p = requests.get(f"{BASE_URL}/projects/{project_id}", headers=headers, params={"company_id": FIXED_COMPANY_ID})
    project_name = r_p.json().get('name', 'Project')

    r_area = requests.get(f"{BASE_URL_V1_1}/projects/{project_id}/drawing_areas", headers=headers)
    area_id = None
    for a in r_area.json():
        if a['name'].strip().upper() == "IFC": area_id = a['id']; break
    if not area_id: raise Exception("No IFC Area found")

    endpoint = f"{BASE_URL_V1_1}/drawing_areas/{area_id}/drawings"
    all_drawings = []
    page = 1
    while True:
        r = requests.get(endpoint, headers=headers, params={"project_id": project_id, "page": page, "per_page": 100})
        data = r.json()
        if not data: break
        all_drawings.extend(data)
        if len(data) < 100: break
        page += 1

    final_batch = []
    for dwg in all_drawings:
        d_name = "Unknown"
        if dwg.get("drawing_discipline"): d_name = dwg["drawing_discipline"].get("name", "Unknown")
        elif dwg.get("discipline"): d_name = dwg["discipline"]
        if d_name in target_disciplines: final_batch.append((dwg, d_name))

    if not final_batch: raise Exception("No drawings found")

    # --- PROCESSING LOOP ---
    job_timestamp = int(time.time())
    temp_dir = f"temp_job_{project_id}_{job_timestamp}"
    if os.path.exists(temp_dir): shutil.rmtree(temp_dir)
    os.makedirs(temp_dir)
    if not os.path.exists("output"): os.makedirs("output")
    files_to_merge = []

    total_files = len(final_batch) # Total count

    for i, (dwg, d_group_name) in enumerate(final_batch):
        # *** NEW: REPORT PROGRESS ***
        if progress_callback:
            # We map the download loop to 0-90% of the total bar
            # (Saving the last 10% for the upload step)
            percent = int((i / total_files) * 90)
            progress_callback(percent)

        time.sleep(0.1)
        try:
            # ... (Download/Markup Logic remains the same) ...
            current_rev = dwg.get('current_revision', {})
            d_num = dwg.get('number', 'NoNum')
            d_title = dwg.get('title', 'NoTitle')
            d_rev = current_rev.get('revision_number', '0')
            pdf_url = current_rev.get('pdf_url')
            if not pdf_url: continue

            clean_num = re.sub(r'[^\w\.-]', '', d_num)
            clean_title = re.sub(r'[^a-zA-Z0-9]', '-', d_title).strip('-')
            clean_rev = re.sub(r'[^\w]', '', str(d_rev))
            final_name = f"{clean_num}-{clean_title}-Rev.{clean_rev}.pdf"
            filename = os.path.join(temp_dir, final_name)

            r_pdf = requests.get(pdf_url)
            with open(filename, "wb") as f: f.write(r_pdf.content)

            revision_id = current_rev.get('id')
            m_params = {"project_id": project_id, "holder_id": revision_id, "holder_type": "DrawingRevision", "page": 1, "page_size": 200}
            r_mark = requests.get(f"{BASE_URL}/markup_layers", headers=headers, params=m_params)
            markups = []
            if r_mark.status_code == 200:
                for layer in r_mark.json():
                    if "markup" in layer: markups.extend(layer["markup"])

            if markups:
                doc = pymupdf.open(filename)
                page = doc[0]
                coord_w = dwg.get('width', 6400) or 6400
                coord_h = dwg.get('height', 4524) or 4524
                scale_x = page.rect.width / float(coord_w)
                scale_y = page.rect.height / float(coord_h)
                shape = page.new_shape()
                for m in markups:
                    if all(k in m for k in ("x", "y", "w", "h")):
                        px = float(m["x"]) * scale_x
                        py = float(m["y"]) * scale_y
                        pw = float(m["w"]) * scale_x
                        ph = float(m["h"]) * scale_y
                        rect = pymupdf.Rect(px, py, px+pw, py+ph)
                        shape.draw_rect(rect)
                        shape.finish(color=None, fill=(0, 1, 1), fill_opacity=0.4)
                shape.commit()
                temp_f = filename.replace(".pdf", "_temp.pdf")
                doc.save(temp_f)
                doc.close()
                if os.path.exists(temp_f): os.replace(temp_f, filename)
            
            files_to_merge.append({"path": filename, "title": final_name.replace(".pdf", ""), "discipline": d_group_name})
        except: pass

    if not files_to_merge: raise Exception("No PDFs generated")
    
    # Merging (90-95%)
    if progress_callback: progress_callback(92)
    
    merged_doc = pymupdf.open()
    toc = []
    current_discipline = None
    for item in files_to_merge:
        try:
            doc_single = pymupdf.open(item['path'])
            if item['discipline'] != current_discipline:
                toc.append([1, item['discipline'], len(merged_doc) + 1])
                current_discipline = item['discipline']
            toc.append([2, item['title'], len(merged_doc) + 1])
            merged_doc.insert_pdf(doc_single)
            doc_single.close()
        except: continue
        
    merged_doc.set_toc(toc)
    clean_proj = re.sub(r'[^a-zA-Z0-9]', '_', project_name)
    date_str = datetime.now().strftime("%d_%m_%y")
    final_filename = f"{clean_proj}_Merged_{date_str}.pdf"
    
    final_output_path = os.path.join("output", final_filename)
    merged_doc.save(final_output_path)
    merged_doc.close()
    try: shutil.rmtree(temp_dir)
    except: pass
    
    return final_output_path