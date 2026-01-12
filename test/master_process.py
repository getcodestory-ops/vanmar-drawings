import os
import json
import time
import re
import requests
import pymupdf
import shutil
from datetime import datetime
from dotenv import load_dotenv

# --- CONFIGURATION ---
BASE_URL = "https://api.procore.com/rest/v1.0"      
BASE_URL_V1_1 = "https://api.procore.com/rest/v1.1" 
FIXED_COMPANY_ID = 4907  # VanMar Constructors

def get_tokens():
    if not os.path.exists("tokens.json"):
        print("ERROR: tokens.json missing. Run the login script first.")
        return None
    with open("tokens.json", "r") as f:
        return json.load(f)

def get_headers(access_token):
    # We now always use the fixed company ID
    return {
        "Authorization": f"Bearer {access_token}",
        "Procore-Company-Id": str(FIXED_COMPANY_ID)
    }

def get_discipline_name(dwg):
    if dwg.get("drawing_discipline"):
        return dwg["drawing_discipline"].get("name", "Unknown")
    elif dwg.get("discipline"):
        return dwg["discipline"]
    return "Unknown"

def run():
    # 1. AUTHENTICATION
    tokens = get_tokens()
    if not tokens: return
    access_token = tokens['access_token']
    
    print(f"--- VANMAR CONSTRUCTORS BOT (Company ID: {FIXED_COMPANY_ID}) ---")

    # ---------------------------------------------------------
    # STEP 1: SELECT PROJECT
    # ---------------------------------------------------------
    print("\n--- 1. SELECT PROJECT ---")
    r_proj = requests.get(f"{BASE_URL}/projects", headers=get_headers(access_token), params={"company_id": FIXED_COMPANY_ID})
    
    if r_proj.status_code != 200:
        print(f"Error fetching projects: {r_proj.text}")
        return

    projects = r_proj.json()
    projects.sort(key=lambda x: x['name'])
    
    for i, p in enumerate(projects[:30]): 
        print(f"[{i}] {p['name']}")
    
    try:
        p_idx = int(input("Select Project #: "))
        project_data = projects[p_idx]
        project_id = project_data['id']
        project_name = project_data['name']
    except: return

    # ---------------------------------------------------------
    # STEP 2: AUTO-SELECT 'IFC' AREA
    # ---------------------------------------------------------
    print("\n--- 2. LOCATING 'IFC' DRAWING AREA ---")
    area_url = f"{BASE_URL_V1_1}/projects/{project_id}/drawing_areas"
    r_area = requests.get(area_url, headers=get_headers(access_token))
    
    if r_area.status_code != 200:
        print(f"Error fetching areas: {r_area.text}")
        return

    areas = r_area.json()
    area_id = None
    
    # Logic: Look for exact match "IFC", if not found, let user pick
    for a in areas:
        if a['name'].strip().upper() == "IFC":
            area_id = a['id']
            print(f"-> Found IFC Area automatically (ID: {area_id})")
            break
    
    if not area_id:
        print("Warning: Could not find an area named exactly 'IFC'. Please select one:")
        for i, a in enumerate(areas):
            print(f"[{i}] {a['name']}")
        try:
            a_idx = int(input("Select Area #: "))
            area_id = areas[a_idx]['id']
        except: return

    # ---------------------------------------------------------
    # STEP 3: FETCH DRAWINGS & GROUP
    # ---------------------------------------------------------
    print("\n--- 3. FETCHING DRAWING LIST ---")
    endpoint = f"{BASE_URL_V1_1}/drawing_areas/{area_id}/drawings"
    all_drawings = []
    page = 1
    
    print("Downloading list (this might take a moment)...")
    while True:
        params = {"project_id": project_id, "page": page, "per_page": 100}
        r_list = requests.get(endpoint, headers=get_headers(access_token), params=params)
        data = r_list.json()
        if not data: break
        all_drawings.extend(data)
        if len(data) < 100: break
        page += 1
        print(f"  Fetched {len(all_drawings)}...")

    disciplines = {}
    for dwg in all_drawings:
        d_name = get_discipline_name(dwg)
        if d_name not in disciplines:
            disciplines[d_name] = []
        disciplines[d_name].append(dwg)

    # ---------------------------------------------------------
    # STEP 4: MULTI-DISCIPLINE SELECTION
    # ---------------------------------------------------------
    print("\n--- 4. SELECT DISCIPLINES ---")
    disc_names = list(disciplines.keys())
    disc_names.sort() 
    
    for i, d in enumerate(disc_names):
        print(f"[{i}] {d} ({len(disciplines[d])} drawings)")
    
    print("\nSelect disciplines (comma separated, e.g., '0, 2')")
    try:
        d_input = input("Selection: ")
        d_indices = [int(x.strip()) for x in d_input.split(',')]
        selected_disciplines = [disc_names[i] for i in d_indices]
    except:
        print("Invalid selection.")
        return

    # ---------------------------------------------------------
    # STEP 5: SELECT DRAWINGS
    # ---------------------------------------------------------
    final_batch = []
    
    for d_name in selected_disciplines:
        print(f"\n--- SELECTING DRAWINGS FOR: {d_name} ---")
        dwgs_in_disc = disciplines[d_name]
        dwgs_in_disc.sort(key=lambda x: x.get('number', ''))
        
        for i, dwg in enumerate(dwgs_in_disc):
            d_num = dwg.get('number', 'NoNum')
            d_title = dwg.get('title', 'NoTitle')
            m_count = dwg.get('markup_count', '?')
            print(f"[{i}] {d_num} - {d_title} (Markups: {m_count})")
            
        sel_str = input(f"Enter numbers for {d_name} (e.g. 0, 3): ")
        try:
            indices = [int(x.strip()) for x in sel_str.split(',')]
            for idx in indices:
                final_batch.append((dwgs_in_disc[idx], d_name))
        except:
            continue

    if not final_batch:
        print("No drawings selected. Exiting.")
        return

    # Create Temp Directory for processing
    temp_dir = "temp_processing"
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir) 
    os.makedirs(temp_dir)
    
    if not os.path.exists("output"): os.makedirs("output")
    
    files_to_merge = []

    # ---------------------------------------------------------
    # STEP 6: PROCESSING LOOP
    # ---------------------------------------------------------
    print(f"\n--- PROCESSING {len(final_batch)} DRAWINGS ---")
    
    for i, (dwg, d_group_name) in enumerate(final_batch):
        current_rev = dwg.get('current_revision', {})
        d_num = dwg.get('number', 'NoNum')
        d_title = dwg.get('title', 'NoTitle')
        d_rev = current_rev.get('revision_number', '0')
        
        print(f"\nDownloading [{i+1}/{len(final_batch)}]: {d_num}...")

        # 1. Get PDF URL
        pdf_url = current_rev.get('pdf_url')
        if not pdf_url:
            print("  Skipping: No PDF URL.")
            continue

        # 2. CONSTRUCT FILENAME
        clean_num = re.sub(r'[^\w\.-]', '', d_num)
        clean_title = re.sub(r'[^a-zA-Z0-9]', '-', d_title)
        clean_title = re.sub(r'-+', '-', clean_title).strip('-')
        clean_rev = re.sub(r'[^\w]', '', str(d_rev))
        
        final_name = f"{clean_num}-{clean_title}-Rev.{clean_rev}.pdf"
        # SAVE TO TEMP DIR
        filename = os.path.join(temp_dir, final_name)
        
        try:
            r_pdf = requests.get(pdf_url)
            with open(filename, "wb") as f:
                f.write(r_pdf.content)
        except Exception as e:
            print(f"  Download Failed: {e}")
            continue

        # 3. Get Markups
        revision_id = current_rev.get('id')
        markup_url = f"{BASE_URL}/markup_layers"
        m_params = {
            "project_id": project_id,
            "holder_id": revision_id,
            "holder_type": "DrawingRevision",
            "page": 1, 
            "page_size": 200
        }

        r_mark = requests.get(markup_url, headers=get_headers(access_token), params=m_params)
        markups = []
        if r_mark.status_code == 200:
            for layer in r_mark.json():
                if "markup" in layer:
                    markups.extend(layer["markup"])

        # 4. OVERLAY MARKUPS
        if markups:
            print(f"  Applying {len(markups)} markups...")
            try:
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
                
                temp_filename = filename.replace(".pdf", "_temp.pdf")
                doc.save(temp_filename)
                doc.close()
                if os.path.exists(temp_filename):
                    os.replace(temp_filename, filename)
            except Exception as e:
                print(f"  Markups failed ({e}), keeping clean file.")

        files_to_merge.append({
            "path": filename,
            "title": final_name.replace(".pdf", ""),
            "discipline": d_group_name 
        })
        
        time.sleep(0.5)

    # ---------------------------------------------------------
    # STEP 7: MERGING & CLEANUP
    # ---------------------------------------------------------
    if not files_to_merge:
        return

    print("\n--- MERGING FINAL PDF ---")
    merged_doc = pymupdf.open()
    toc = []
    current_discipline = None
    
    for item in files_to_merge:
        path = item['path']
        title = item['title']
        disc = item['discipline']
        
        print(f"Merging: {title}")
        try:
            doc_single = pymupdf.open(path)
        except:
            continue
            
        if disc != current_discipline:
            toc.append([1, disc, len(merged_doc) + 1]) 
            current_discipline = disc
            
        toc.append([2, title, len(merged_doc) + 1])
        merged_doc.insert_pdf(doc_single)
        doc_single.close()

    merged_doc.set_toc(toc)
    
    # ---------------------------------------------------------
    # FINAL NAMING CONVENTION
    # ---------------------------------------------------------
    clean_proj_name = re.sub(r'[^a-zA-Z0-9]', '_', project_name)
    date_str = datetime.now().strftime("%d_%m_%y")
    
    final_output_name = f"{clean_proj_name}_Merged Drawings_{date_str}.pdf"
    output_path = os.path.join("output", final_output_name)
    
    merged_doc.save(output_path)
    merged_doc.close()
    
    # CLEANUP TEMP FILES
    try:
        shutil.rmtree(temp_dir)
        print("\nCleaned up temp files.")
    except Exception as e:
        print(f"Warning: Could not delete temp folder: {e}")
    
    print("-" * 30)
    print(f"SUCCESS! Final Binder: {output_path}")
    print("-" * 30)

if __name__ == "__main__":
    run()