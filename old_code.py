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