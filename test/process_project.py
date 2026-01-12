import os
import json
import time
import re
import requests
import pymupdf 
from dotenv import load_dotenv

# --- CONFIGURATION ---
# Use the same base URL that worked for your token script
BASE_URL = "https://api.procore.com/rest/v1.0"
# BASE_URL = "https://sandbox.procore.com/rest/v1.0" 

def get_tokens():
    if not os.path.exists("tokens.json"):
        print("ERROR: tokens.json missing. Run the login script first.")
        return None
    with open("tokens.json", "r") as f:
        return json.load(f)

def run():
    # 1. AUTHENTICATION
    tokens = get_tokens()
    if not tokens: return
    
    access_token = tokens['access_token']
    # Initial header only has the token
    headers = {"Authorization": f"Bearer {access_token}"}

    # ---------------------------------------------------------
    # NEW STEP: SELECT COMPANY
    # ---------------------------------------------------------
    print("--- FETCHING COMPANIES ---")
    r_comp = requests.get(f"{BASE_URL}/companies", headers=headers)
    
    if r_comp.status_code != 200:
        print(f"Error fetching companies: {r_comp.status_code}")
        print(r_comp.text)
        return

    companies = r_comp.json()
    print(f"Found {len(companies)} companies:")
    for i, c in enumerate(companies):
        print(f"[{i}] {c['name']} (ID: {c['id']})")

    try:
        c_selection = int(input("\nSelect a COMPANY number: "))
        selected_company = companies[c_selection]
        company_id = selected_company['id']
        
        # CRITICAL FIX: Add the Company ID to headers for ALL future calls
        headers["Procore-Company-Id"] = str(company_id)
        print(f"Selected Company: {selected_company['name']}")
    except:
        print("Invalid selection.")
        return

    # ---------------------------------------------------------
    # STEP 2: SELECT PROJECT
    # ---------------------------------------------------------
    print("\n--- FETCHING PROJECTS ---")
    # Now we request projects specifically for this company
    params = {"company_id": company_id}
    r = requests.get(f"{BASE_URL}/projects", headers=headers, params=params)
    
    if r.status_code != 200:
        print(f"Error: {r.status_code} - {r.text}")
        return

    projects = r.json()
    # Sort projects by name to make them easier to find
    projects.sort(key=lambda x: x['name'])
    
    print(f"Found {len(projects)} projects in this company.")
    # Show first 15 for brevity
    for i, p in enumerate(projects[:15]):
        print(f"[{i}] {p['name']} (ID: {p['id']})")
    
    try:
        p_selection = int(input("\nSelect a PROJECT number: "))
        project = projects[p_selection]
        project_id = project['id']
        print(f"Selected Project: {project['name']}")
    except:
        print("Invalid selection.")
        return

    # ---------------------------------------------------------
    # STEP 3: FETCH DRAWINGS
    # ---------------------------------------------------------
    print("\n--- FETCHING DRAWINGS ---")
    drawings_url = f"{BASE_URL}/projects/{project_id}/drawings"
    
    resp_drawings = requests.get(drawings_url, headers=headers)
    if resp_drawings.status_code != 200:
        print("Failed to fetch drawings.")
        print(resp_drawings.text)
        return

    drawings_list = resp_drawings.json()
    print(f"Found {len(drawings_list)} drawings.")

    if not os.path.exists("output"):
        os.makedirs("output")

    # ---------------------------------------------------------
    # STEP 4: PROCESSING LOOP (Test first 3)
    # ---------------------------------------------------------
    print("Processing the first 3 drawings for testing...")
    
    for idx, dwg in enumerate(drawings_list[:3]): 
        print(f"\n--- Drawing {idx+1} ---")
        
        current_rev = dwg.get('current_revision', {})
        d_num = dwg.get('number', 'NoNum')
        d_title = dwg.get('title', 'NoTitle')
        d_rev = current_rev.get('revision_number', '0')
        
        # Get PDF URL
        pdf_url = current_rev.get('pdf_url')
        if not pdf_url:
            print(f"Skipping {d_num} - No PDF URL.")
            continue

        # Download PDF
        clean_num = re.sub(r'[^a-zA-Z0-9]', '', d_num)
        clean_title = re.sub(r'[^a-zA-Z0-9]', '-', d_title).strip('-')
        filename = f"output/{clean_num}_{clean_title}_Rev{d_rev}.pdf"
        
        print(f"Downloading to: {filename}")
        r_pdf = requests.get(pdf_url)
        with open(filename, "wb") as f:
            f.write(r_pdf.content)

        # Fetch Markups
        revision_id = current_rev.get('id')
        markup_url = f"{BASE_URL}/markup_layers"
        m_params = {
            "project_id": project_id,
            "holder_id": revision_id,
            "holder_type": "DrawingRevision",
            "page": 1,
            "page_size": 200
        }
        
        resp_m = requests.get(markup_url, headers=headers, params=m_params)
        markups = []
        if resp_m.status_code == 200:
            layers = resp_m.json()
            for layer in layers:
                if "markup" in layer:
                    markups.extend(layer["markup"])

        # Overlay Markups
        if markups:
            print(f"Found {len(markups)} markups. Rendering...")
            doc = pymupdf.open(filename)
            page = doc[0]

            # Coordinate Scaling
            coord_w = dwg.get('width', 6400)
            coord_h = dwg.get('height', 4524)
            if not coord_w: coord_w = 6400
            if not coord_h: coord_h = 4524
            
            pdf_w = page.rect.width
            pdf_h = page.rect.height
            scale_x = pdf_w / float(coord_w)
            scale_y = pdf_h / float(coord_h)

            shape = page.new_shape()
            
            for m in markups:
                if all(k in m for k in ("x", "y", "w", "h")):
                    px = float(m["x"]) * scale_x
                    py = float(m["y"]) * scale_y
                    pw = float(m["w"]) * scale_x
                    ph = float(m["h"]) * scale_y
                    
                    rect = pymupdf.Rect(px, py, px+pw, py+ph)
                    
                    shape.draw_rect(rect)
                    shape.finish(color=(1, 0, 0), width=2) 
                    
                    if "item_displayed_name" in m:
                        page.insert_text((px, py-2), m["item_displayed_name"], fontsize=8, color=(1,0,0))

            shape.commit()
            doc.saveIncr()
            doc.close()
            print("Render complete.")
        else:
            print("No markups found.")

        time.sleep(1) 

if __name__ == "__main__":
    run()