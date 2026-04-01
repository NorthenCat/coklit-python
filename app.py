from flask import Flask, render_template, request, jsonify, session, redirect, url_for, send_file
import pandas as pd
import numpy as np
import os
import json
from datetime import datetime
import uuid
from fuzzywuzzy import fuzz
import tempfile
from io import BytesIO
import openpyxl

app = Flask(__name__)
app.secret_key = 'your_secret_key_here'  # Change this to a random secret key
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB max file size
app.config['SESSIONS_DB'] = 'sessions_db.json'

# Progress tracking storage (in-memory for real-time updates)
progress_tracker = {}

# Add CORS headers
@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE')
    return response

# Ensure upload directory exists
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

def load_sessions_db():
    """Load sessions database from JSON file"""
    db_path = app.config['SESSIONS_DB']
    if os.path.exists(db_path):
        try:
            with open(db_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading sessions DB: {e}")
            return {"sessions": []}
    return {"sessions": []}

def save_sessions_db(data):
    """Save sessions database to JSON file"""
    db_path = app.config['SESSIONS_DB']
    try:
        with open(db_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        print(f"Error saving sessions DB: {e}")
        return False

def add_session_to_db(session_info):
    """Add a new session to the database"""
    db = load_sessions_db()
    
    # Check if session already exists
    existing_idx = next((i for i, s in enumerate(db['sessions']) if s['session_id'] == session_info['session_id']), None)
    
    if existing_idx is not None:
        # Update existing session
        db['sessions'][existing_idx].update(session_info)
    else:
        # Add new session
        db['sessions'].insert(0, session_info)  # Add at beginning (newest first)
    
    # Keep only last 50 sessions
    db['sessions'] = db['sessions'][:50]
    
    save_sessions_db(db)
    return session_info

def update_session_in_db(session_id, updates):
    """Update a session in the database"""
    db = load_sessions_db()
    
    for session in db['sessions']:
        if session['session_id'] == session_id:
            session.update(updates)
            session['updated_at'] = datetime.now().isoformat()
            break
    
    save_sessions_db(db)

def delete_session_from_db(session_id):
    """Delete a session from the database"""
    db = load_sessions_db()
    db['sessions'] = [s for s in db['sessions'] if s['session_id'] != session_id]
    save_sessions_db(db)

def load_data(filepath):
    """Load data from CSV or Excel file with robust error handling"""
    try:
        file_extension = filepath.lower().split('.')[-1]
        
        if file_extension == 'csv':
            # Try different encodings for CSV
            encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
            for encoding in encodings:
                try:
                    df = pd.read_csv(filepath, encoding=encoding)
                    if not df.empty:
                        # Clean column names (remove extra spaces)
                        df.columns = df.columns.str.strip()
                        return df, None
                except UnicodeDecodeError:
                    continue
                except Exception:
                    continue
            return None, "Could not read CSV file with any encoding"
            
        elif file_extension in ['xlsx', 'xls']:
            try:
                # Try to read Excel file
                df = pd.read_excel(filepath, engine='openpyxl' if file_extension == 'xlsx' else None)
                if not df.empty:
                    # Clean column names (remove extra spaces)
                    df.columns = df.columns.str.strip()
                    return df, None
                else:
                    return None, "Excel file is empty"
            except Exception as e:
                return None, f"Error reading Excel file: {str(e)}"
        else:
            return None, f"Unsupported file format: {file_extension}. Please use CSV or Excel files."
            
    except Exception as e:
        return None, f"Unexpected error loading file: {str(e)}"

def calculate_field_similarity(value1, value2):
    """Calculate similarity between two values using fuzzy matching"""
    # Handle None/NaN/empty values
    if pd.isna(value1) or pd.isna(value2) or value1 is None or value2 is None:
        # If both are empty/None, consider them as a match
        if (pd.isna(value1) or value1 is None or str(value1).strip() == '') and \
           (pd.isna(value2) or value2 is None or str(value2).strip() == ''):
            return 100.0
        # If one is empty and other is not, no match
        else:
            return 0.0
    
    # Convert to string for comparison and clean whitespace
    str1 = str(value1).strip()
    str2 = str(value2).strip()
    
    # Remove leading apostrophe (') if present - Excel formatting issue
    if str1.startswith("'"):
        str1 = str1[1:]
    if str2.startswith("'"):
        str2 = str2[1:]
    
    # Strip again after removing apostrophe
    str1 = str1.strip()
    str2 = str2.strip()
    
    # If both are empty strings after cleaning
    if str1 == '' and str2 == '':
        return 100.0
    
    # If one is empty and other is not
    if str1 == '' or str2 == '':
        return 0.0
    
    # Case insensitive exact match
    if str1.lower() == str2.lower():
        return 100.0
    
    # Use fuzzy matching for similarity
    return fuzz.ratio(str1.lower(), str2.lower())

def clean_data_value(value):
    """Clean data value by removing leading apostrophe and formatting issues"""
    if pd.isna(value) or value is None:
        return None
    
    # Convert to string and strip whitespace
    clean_value = str(value).strip()
    
    # Remove leading apostrophe (Excel formatting issue)
    if clean_value.startswith("'"):
        clean_value = clean_value[1:]
    
    # Strip again after cleaning
    clean_value = clean_value.strip()
    
    # Return None for empty strings
    if clean_value == '':
        return None
    
    return clean_value

def save_checkpoint(session_id, current_index, results, matched_indices_df2):
    """Save progress checkpoint to disk"""
    try:
        session_dir = os.path.join(app.config['UPLOAD_FOLDER'], session_id)
        checkpoint_file = os.path.join(session_dir, 'progress_checkpoint.json')
        
        checkpoint_data = {
            'last_processed_index': current_index,
            'total_records': results['summary']['total_file1'],
            'matched_data': results['matched_data'],
            'unmatched_data': results['unmatched_data'],
            'matched_count': results['summary']['matched_count'],
            'unmatched_count': results['summary']['unmatched_count'],
            'matched_indices_df2': list(matched_indices_df2),
            'timestamp': datetime.now().isoformat()
        }
        
        with open(checkpoint_file, 'w', encoding='utf-8') as f:
            json.dump(checkpoint_data, f, ensure_ascii=False, default=str)
        
        print(f"DEBUG: Checkpoint saved at index {current_index}")
        
        # Update session in database
        update_session_in_db(session_id, {
            'status': 'processing',
            'progress_current': current_index,
            'progress_total': results['summary']['total_file1']
        })
        
    except Exception as e:
        print(f"DEBUG: Error saving checkpoint: {e}")

def delete_checkpoint(session_id):
    """Delete checkpoint file after successful completion"""
    try:
        session_dir = os.path.join(app.config['UPLOAD_FOLDER'], session_id)
        checkpoint_file = os.path.join(session_dir, 'progress_checkpoint.json')
        if os.path.exists(checkpoint_file):
            os.remove(checkpoint_file)
            print(f"DEBUG: Checkpoint deleted for session {session_id}")
    except Exception as e:
        print(f"DEBUG: Error deleting checkpoint: {e}")

def normalize_result_data(row1, row2, field_mappings):
    """Normalize data to ensure consistent structure across all results"""
    file1_data = {}
    file2_data = {}
    
    for mapping in field_mappings:
        field1 = mapping['field1']
        field2 = mapping['field2']
        
        # Add field1 data with proper cleaning and null handling
        if row1 is not None and not row1.empty and field1 in row1.index:
            raw_value1 = row1[field1]
            file1_data[field1] = clean_data_value(raw_value1)
        else:
            file1_data[field1] = None
            
        # Add field2 data with proper cleaning and null handling
        if row2 is not None and not row2.empty and field2 in row2.index:
            raw_value2 = row2[field2]
            file2_data[field2] = clean_data_value(raw_value2)
        else:
            file2_data[field2] = None
    
    return file1_data, file2_data

def match_data_with_mapping(df1, df2, field_mappings, similarity_threshold=50, session_id=None, start_index=0):
    """
    Match data between two dataframes based on specified field mappings
    File 1 is the primary reference, File 2 is for comparison
    field_mappings: list of dicts with 'field1', 'field2' and 'min_accuracy' keys
    session_id: optional session ID for progress tracking
    start_index: index to resume from (for checkpoint recovery)
    Returns matched data with accuracy scores and unmatched data
    """
    print(f"DEBUG: Starting matching with {len(df1)} records in File 1 and {len(df2)} records in File 2")
    
    total_records = len(df1)
    
    results = {
        'matched_data': [],
        'unmatched_data': [],
        'summary': {
            'total_file1': len(df1),
            'total_file2': len(df2),
            'matched_count': 0,
            'unmatched_count': 0
        }
    }
    
    # Load checkpoint data if resuming
    checkpoint_data = None
    if session_id and start_index > 0:
        session_dir = os.path.join(app.config['UPLOAD_FOLDER'], session_id)
        checkpoint_file = os.path.join(session_dir, 'progress_checkpoint.json')
        if os.path.exists(checkpoint_file):
            try:
                with open(checkpoint_file, 'r', encoding='utf-8') as f:
                    checkpoint_data = json.load(f)
                    results['matched_data'] = checkpoint_data.get('matched_data', [])
                    results['unmatched_data'] = checkpoint_data.get('unmatched_data', [])
                    results['summary']['matched_count'] = checkpoint_data.get('matched_count', 0)
                    results['summary']['unmatched_count'] = checkpoint_data.get('unmatched_count', 0)
                    print(f"DEBUG: Loaded checkpoint data, resuming from index {start_index}")
            except Exception as e:
                print(f"DEBUG: Error loading checkpoint: {e}")
    
    matched_indices_df2 = set()
    
    # Restore matched indices from checkpoint
    if checkpoint_data:
        for item in results['matched_data']:
            if item.get('file2_index') is not None:
                matched_indices_df2.add(item['file2_index'])
    
    # Initialize progress tracker
    if session_id:
        progress_tracker[session_id] = {
            'current': start_index,
            'total': total_records,
            'status': 'processing',
            'message': f'Memproses baris {start_index + 1} dari {total_records}',
            'matched_count': results['summary']['matched_count'],
            'unmatched_count': results['summary']['unmatched_count']
        }
    
    # Process each row in File 1 (our primary reference)
    for idx1, row1 in df1.iterrows():
        # Skip already processed rows when resuming
        if idx1 < start_index:
            continue
        
        # Update progress
        if session_id:
            progress_tracker[session_id] = {
                'current': idx1 + 1,
                'total': total_records,
                'status': 'processing',
                'message': f'Memproses baris {idx1 + 1} dari {total_records}',
                'matched_count': results['summary']['matched_count'],
                'unmatched_count': results['summary']['unmatched_count']
            }
            
            # Save checkpoint every 50 rows
            if idx1 > 0 and idx1 % 50 == 0:
                save_checkpoint(session_id, idx1, results, matched_indices_df2)
        
        print(f"DEBUG: Processing File 1 row {idx1 + 1}/{len(df1)}")
        
        best_match = None
        best_score = -1
        best_idx2 = None
        best_field_scores = {}
        found_exact_match = False
        
        # PHASE 1: Look for EXACT MATCHES or 100% PRIORITY FIELDS first
        print(f"DEBUG: PHASE 1 - Looking for exact/priority matches...")
        for idx2, row2 in df2.iterrows():
            if idx2 in matched_indices_df2:
                continue
                
            # Calculate similarity for each field mapping
            field_scores = {}
            total_score = 0
            valid_mappings = 0
            has_exact_or_priority_100 = False
            
            for mapping in field_mappings:
                field1 = mapping['field1']
                field2 = mapping['field2']
                is_priority = mapping.get('is_priority', False)
                
                if field1 in df1.columns and field2 in df2.columns:
                    score = calculate_field_similarity(row1[field1], row2[field2])
                    mapping_key = f"{field1}_{field2}"
                    field_scores[mapping_key] = score
                    total_score += score
                    valid_mappings += 1
                    
                    # Check for exact match (100%) especially on priority fields
                    if score == 100:
                        if is_priority:
                            print(f"DEBUG: Found 100% PRIORITY match for {field1}: '{row1[field1]}' ↔ '{row2[field2]}'")
                            has_exact_or_priority_100 = True
                        else:
                            print(f"DEBUG: Found 100% match for {field1}: '{row1[field1]}' ↔ '{row2[field2]}'")
                else:
                    print(f"DEBUG: Warning - Field {field1} or {field2} not found in columns")
            
            avg_score = total_score / valid_mappings if valid_mappings > 0 else 0
            
            # If we find a record with 100% priority field, prioritize it immediately
            if has_exact_or_priority_100:
                if avg_score > best_score or not found_exact_match:
                    best_score = avg_score
                    best_match = row2.copy()
                    best_idx2 = idx2
                    best_field_scores = field_scores.copy()
                    found_exact_match = True
                    print(f"DEBUG: PHASE 1 - Updated best match with exact/priority score: {avg_score:.2f}%")
        
        # PHASE 2: If no exact/priority match found, do regular best match search
        if not found_exact_match:
            print(f"DEBUG: PHASE 2 - No exact/priority match found, searching for best overall match...")
            for idx2, row2 in df2.iterrows():
                if idx2 in matched_indices_df2:
                    continue
                    
                # Calculate similarity for each field mapping
                field_scores = {}
                total_score = 0
                valid_mappings = 0
                
                for mapping in field_mappings:
                    field1 = mapping['field1']
                    field2 = mapping['field2']
                    
                    if field1 in df1.columns and field2 in df2.columns:
                        score = calculate_field_similarity(row1[field1], row2[field2])
                        mapping_key = f"{field1}_{field2}"
                        field_scores[mapping_key] = score
                        total_score += score
                        valid_mappings += 1
                
                avg_score = total_score / valid_mappings if valid_mappings > 0 else 0
                
                # Keep track of the best overall match
                if avg_score > best_score:
                    best_score = avg_score
                    best_match = row2.copy()
                    best_idx2 = idx2
                    best_field_scores = field_scores.copy()
        
        match_type = "EXACT/PRIORITY" if found_exact_match else "BEST_OVERALL"
        print(f"DEBUG: {match_type} match for File 1 row {idx1}: score={best_score:.2f}%")
        
        # Now we have the best possible match for this File 1 row
        # Create result entry with proper data structure
        file1_data, file2_data = normalize_result_data(row1, best_match, field_mappings)
        
        # Check if this match meets all criteria
        all_fields_pass = True
        has_priority_match = False
        
        if best_match is not None and best_score > 0:
            for mapping in field_mappings:
                field1 = mapping['field1']
                field2 = mapping['field2']
                min_accuracy = mapping.get('min_accuracy', 50)
                is_priority = mapping.get('is_priority', False)
                mapping_key = f"{field1}_{field2}"
                
                if mapping_key in best_field_scores:
                    field_accuracy = best_field_scores[mapping_key]
                    
                    # Check for priority field with 100% accuracy
                    if is_priority and field_accuracy == 100:
                        has_priority_match = True
                        print(f"DEBUG: Priority field match found! {field1}↔{field2}: {field_accuracy}%")
                    
                    # Regular field accuracy check
                    if field_accuracy < min_accuracy:
                        all_fields_pass = False
        else:
            all_fields_pass = False
        
        # A match is valid if:
        # 1. Overall score >= threshold AND all fields meet their requirements, OR
        # 2. At least one priority field has 100% accuracy (regardless of other fields)
        is_valid_match = (best_match is not None and 
                         ((best_score >= similarity_threshold and all_fields_pass) or 
                          has_priority_match))
        
        print(f"DEBUG: Match validation - Overall: {best_score:.1f}%, All fields pass: {all_fields_pass}, Priority match: {has_priority_match}, Valid: {is_valid_match}")
        
        # Create the result entry
        result_entry = {
            'file1_data': file1_data,
            'file2_data': file2_data if best_match is not None else {mapping['field2']: None for mapping in field_mappings},
            'overall_accuracy': round(best_score, 2) if best_score >= 0 else 0.0,
            'field_accuracies': best_field_scores,
            'is_match': is_valid_match,
            'priority_match': has_priority_match,
            'file1_index': idx1,
            'file2_index': best_idx2 if best_match is not None else None
        }
        
        if is_valid_match:
            # Valid match - add to matched data and mark File 2 row as used
            matched_indices_df2.add(best_idx2)
            results['matched_data'].append(result_entry)
            results['summary']['matched_count'] += 1
            print(f"DEBUG: Added to matched data - overall accuracy: {best_score:.2f}%")
        else:
            # No valid match found - add to unmatched with best candidate
            results['unmatched_data'].append(result_entry)
            results['summary']['unmatched_count'] += 1
            print(f"DEBUG: Added to unmatched data - best score: {best_score:.2f}%")
    
    # Mark progress as complete
    if session_id:
        progress_tracker[session_id] = {
            'current': total_records,
            'total': total_records,
            'status': 'completed',
            'message': f'Selesai! {total_records} baris diproses',
            'matched_count': results['summary']['matched_count'],
            'unmatched_count': results['summary']['unmatched_count']
        }
        # Delete checkpoint file on successful completion
        delete_checkpoint(session_id)
    
    print(f"DEBUG: Matching complete. Matched: {results['summary']['matched_count']}, Unmatched: {results['summary']['unmatched_count']}")
    return results

def match_data_within_single_file(df, field_mappings, similarity_threshold=50, session_id=None, start_index=0):
    """
    Match data within a single dataframe to find duplicates or similar records
    Each record is compared against all other records based on field mappings
    field_mappings: list of dicts with 'field1' (used for both comparison sides) and 'min_accuracy' keys
    session_id: optional session ID for progress tracking
    start_index: index to resume from (for checkpoint recovery)
    Returns matched data with accuracy scores and unmatched data
    """
    print(f"DEBUG: Starting self-matching with {len(df)} records in single file")
    
    total_records = len(df)
    
    results = {
        'matched_data': [],
        'unmatched_data': [],
        'summary': {
            'total_file1': len(df),
            'total_file2': len(df),
            'matched_count': 0,
            'unmatched_count': 0
        }
    }
    
    # Track which records have been matched
    matched_indices = set()
    processed_pairs = set()  # Track pairs that we've already processed (to avoid duplicates)
    
    # Initialize progress tracker
    if session_id:
        progress_tracker[session_id] = {
            'current': start_index,
            'total': total_records,
            'status': 'processing',
            'message': f'Memproses baris {start_index + 1} dari {total_records}',
            'matched_count': 0,
            'unmatched_count': 0
        }
    
    # Process each row in the dataframe
    processed_count = 0
    for idx1, row1 in df.iterrows():
        # Skip if this record has already been matched
        if idx1 in matched_indices:
            continue
        
        # Skip already processed rows when resuming
        if processed_count < start_index:
            processed_count += 1
            continue
        
        # Update progress
        if session_id:
            progress_tracker[session_id] = {
                'current': processed_count + 1,
                'total': total_records,
                'status': 'processing',
                'message': f'Memproses baris {processed_count + 1} dari {total_records}',
                'matched_count': results['summary']['matched_count'],
                'unmatched_count': results['summary']['unmatched_count']
            }
            
        print(f"DEBUG: Processing row {idx1 + 1}/{len(df)}")
        
        best_match = None
        best_score = -1
        best_idx2 = None
        best_field_scores = {}
        found_exact_match = False
        
        # Compare with all other records
        for idx2, row2 in df.iterrows():
            # Skip self-comparison
            if idx1 == idx2:
                continue
            
            # Skip if already matched
            if idx2 in matched_indices:
                continue
            
            # Skip if we've already processed this pair (avoid A-B and B-A duplicate)
            pair_key = tuple(sorted([idx1, idx2]))
            if pair_key in processed_pairs:
                continue
            
            # Calculate similarity for each field mapping
            field_scores = {}
            total_score = 0
            valid_mappings = 0
            has_exact_or_priority_100 = False
            
            for mapping in field_mappings:
                field1 = mapping['field1']
                is_priority = mapping.get('is_priority', False)
                
                if field1 in df.columns:
                    score = calculate_field_similarity(row1[field1], row2[field1])
                    mapping_key = f"{field1}_{field1}"
                    field_scores[mapping_key] = score
                    total_score += score
                    valid_mappings += 1
                    
                    # Check for exact match (100%) especially on priority fields
                    if score == 100 and is_priority:
                        has_exact_or_priority_100 = True
                        print(f"DEBUG: Found 100% PRIORITY match for {field1}")
                else:
                    print(f"DEBUG: Warning - Field {field1} not found in columns")
            
            avg_score = total_score / valid_mappings if valid_mappings > 0 else 0
            
            # Prioritize exact/priority matches
            if has_exact_or_priority_100:
                if avg_score > best_score or not found_exact_match:
                    best_score = avg_score
                    best_match = row2.copy()
                    best_idx2 = idx2
                    best_field_scores = field_scores.copy()
                    found_exact_match = True
            elif not found_exact_match and avg_score > best_score:
                best_score = avg_score
                best_match = row2.copy()
                best_idx2 = idx2
                best_field_scores = field_scores.copy()
        
        # Mark this pair as processed
        if best_idx2 is not None:
            pair_key = tuple(sorted([idx1, best_idx2]))
            processed_pairs.add(pair_key)
        
        match_type = "EXACT/PRIORITY" if found_exact_match else "BEST_OVERALL"
        print(f"DEBUG: {match_type} match for row {idx1}: score={best_score:.2f}%")
        
        # Create result entry with proper data structure
        # For single file, field1 and field2 are the same
        single_file_mappings = [{'field1': m['field1'], 'field2': m['field1']} for m in field_mappings]
        file1_data, file2_data = normalize_result_data(row1, best_match, single_file_mappings)
        
        # Check if this match meets all criteria
        all_fields_pass = True
        has_priority_match = False
        
        if best_match is not None and best_score > 0:
            for mapping in field_mappings:
                field1 = mapping['field1']
                min_accuracy = mapping.get('min_accuracy', 50)
                is_priority = mapping.get('is_priority', False)
                mapping_key = f"{field1}_{field1}"
                
                if mapping_key in best_field_scores:
                    field_accuracy = best_field_scores[mapping_key]
                    
                    # Check for priority field with 100% accuracy
                    if is_priority and field_accuracy == 100:
                        has_priority_match = True
                        print(f"DEBUG: Priority field match found! {field1}: {field_accuracy}%")
                    
                    # Regular field accuracy check
                    if field_accuracy < min_accuracy:
                        all_fields_pass = False
        else:
            all_fields_pass = False
        
        # A match is valid if:
        # 1. Overall score >= threshold AND all fields meet their requirements, OR
        # 2. At least one priority field has 100% accuracy (regardless of other fields)
        is_valid_match = (best_match is not None and 
                         ((best_score >= similarity_threshold and all_fields_pass) or 
                          has_priority_match))
        
        print(f"DEBUG: Match validation - Overall: {best_score:.1f}%, All fields pass: {all_fields_pass}, Priority match: {has_priority_match}, Valid: {is_valid_match}")
        
        # Create the result entry
        result_entry = {
            'file1_data': file1_data,
            'file2_data': file2_data if best_match is not None else {mapping['field1']: None for mapping in field_mappings},
            'overall_accuracy': round(best_score, 2) if best_score >= 0 else 0.0,
            'field_accuracies': best_field_scores,
            'is_match': is_valid_match,
            'priority_match': has_priority_match,
            'file1_index': idx1,
            'file2_index': best_idx2 if best_match is not None else None
        }
        
        if is_valid_match:
            # Valid match - add to matched data and mark both records as matched
            matched_indices.add(idx1)
            matched_indices.add(best_idx2)
            results['matched_data'].append(result_entry)
            results['summary']['matched_count'] += 2  # Both records are considered matched
            print(f"DEBUG: Added to matched data - overall accuracy: {best_score:.2f}%")
        else:
            # No valid match found - add to unmatched
            results['unmatched_data'].append(result_entry)
            results['summary']['unmatched_count'] += 1
            print(f"DEBUG: Added to unmatched data - best score: {best_score:.2f}%")
        
        processed_count += 1
    
    # Mark progress as complete
    if session_id:
        progress_tracker[session_id] = {
            'current': total_records,
            'total': total_records,
            'status': 'completed',
            'message': f'Selesai! {total_records} baris diproses',
            'matched_count': results['summary']['matched_count'],
            'unmatched_count': results['summary']['unmatched_count']
        }
    
    print(f"DEBUG: Self-matching complete. Matched pairs: {len(results['matched_data'])}, Unmatched: {results['summary']['unmatched_count']}")
    return results

def match_data(df1, df2, fields_to_match, similarity_threshold=50):
    """
    Match data between two dataframes based on specified fields
    Returns matched data with accuracy scores and unmatched data
    """
    results = {
        'matched_data': [],
        'unmatched_data': [],
        'summary': {
            'total_file1': len(df1),
            'total_file2': len(df2),
            'matched_count': 0,
            'unmatched_count': 0
        }
    }
    
    matched_indices_df2 = set()
    
    for idx1, row1 in df1.iterrows():
        best_match = None
        best_score = 0
        best_idx2 = None
        
        for idx2, row2 in df2.iterrows():
            if idx2 in matched_indices_df2:
                continue
                
            # Calculate average similarity across specified fields
            field_scores = {}
            total_score = 0
            valid_fields = 0
            
            for field in fields_to_match:
                if field in df1.columns and field in df2.columns:
                    score = calculate_field_similarity(row1[field], row2[field])
                    field_scores[field] = score
                    total_score += score
                    valid_fields += 1
            
            avg_score = total_score / valid_fields if valid_fields > 0 else 0
            
            if avg_score > best_score:
                best_score = avg_score
                best_match = row2
                best_idx2 = idx2
                best_field_scores = field_scores
        
        # Create result entry
        result_entry = {
            'file1_data': row1.to_dict(),
            'file2_data': best_match.to_dict() if best_match is not None else {},
            'overall_accuracy': round(best_score, 2),
            'field_accuracies': best_field_scores if best_match is not None else {},
            'is_match': best_score >= similarity_threshold
        }
        
        if best_score >= similarity_threshold and best_match is not None:
            matched_indices_df2.add(best_idx2)
            results['matched_data'].append(result_entry)
            results['summary']['matched_count'] += 1
        else:
            results['unmatched_data'].append(result_entry)
            results['summary']['unmatched_count'] += 1
    
    # Add unmatched entries from df2
    for idx2, row2 in df2.iterrows():
        if idx2 not in matched_indices_df2:
            unmatched_entry = {
                'file1_data': {},
                'file2_data': row2.to_dict(),
                'overall_accuracy': 0.0,
                'field_accuracies': {},
                'is_match': False
            }
            results['unmatched_data'].append(unmatched_entry)
            results['summary']['unmatched_count'] += 1
    
    return results

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/get_sessions_db', methods=['GET'])
def get_sessions_db():
    """Get all sessions from the JSON database"""
    try:
        db = load_sessions_db()
        
        # Filter out old sessions (older than 30 days) and update with file system info
        updated_sessions = []
        for session in db['sessions']:
            session_dir = os.path.join(app.config['UPLOAD_FOLDER'], session['session_id'])
            
            # Check if session folder still exists
            if os.path.exists(session_dir):
                # Update with current file info
                files = os.listdir(session_dir)
                session['has_results'] = 'matching_results.json' in files
                
                # Check for progress checkpoint
                checkpoint_file = os.path.join(session_dir, 'progress_checkpoint.json')
                session['has_checkpoint'] = os.path.exists(checkpoint_file)
                
                updated_sessions.append(session)
        
        # Sort by created_at (newest first)
        updated_sessions.sort(key=lambda x: x.get('created_at', ''), reverse=True)
        
        return jsonify({'sessions': updated_sessions})
    except Exception as e:
        print(f"Error getting sessions DB: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/delete_session/<session_id>', methods=['DELETE'])
def delete_session(session_id):
    """Delete a session from database and file system"""
    try:
        # Delete from database
        delete_session_from_db(session_id)
        
        # Delete from file system
        session_dir = os.path.join(app.config['UPLOAD_FOLDER'], session_id)
        if os.path.exists(session_dir):
            import shutil
            shutil.rmtree(session_dir)
        
        return jsonify({'message': 'Session deleted successfully'})
    except Exception as e:
        print(f"Error deleting session: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/get_progress/<session_id>', methods=['GET'])
def get_progress(session_id):
    """Get current progress for a session"""
    try:
        if session_id in progress_tracker:
            return jsonify(progress_tracker[session_id])
        
        # Check for saved checkpoint
        session_dir = os.path.join(app.config['UPLOAD_FOLDER'], session_id)
        checkpoint_file = os.path.join(session_dir, 'progress_checkpoint.json')
        
        if os.path.exists(checkpoint_file):
            with open(checkpoint_file, 'r', encoding='utf-8') as f:
                checkpoint = json.load(f)
                return jsonify({
                    'current': checkpoint.get('last_processed_index', 0),
                    'total': checkpoint.get('total_records', 0),
                    'status': 'paused',
                    'message': 'Proses tertunda - dapat dilanjutkan'
                })
        
        return jsonify({
            'current': 0,
            'total': 0,
            'status': 'idle',
            'message': 'Belum ada proses'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/list_sessions', methods=['GET'])
def list_sessions():
    """List all available sessions sorted by creation time (newest first)"""
    try:
        upload_folder = app.config['UPLOAD_FOLDER']
        
        if not os.path.exists(upload_folder):
            return jsonify({'sessions': []})
        
        sessions = []
        
        # Get all directories in upload folder
        for item in os.listdir(upload_folder):
            item_path = os.path.join(upload_folder, item)
            
            # Check if it's a directory (session folder)
            if os.path.isdir(item_path):
                try:
                    # Get creation time
                    created_at = os.path.getctime(item_path)
                    
                    # Count files in session
                    files = [f for f in os.listdir(item_path) if os.path.isfile(os.path.join(item_path, f))]
                    file_count = len([f for f in files if f.startswith('file') and not f.endswith('.json')])
                    
                    # Check if results exist
                    has_results = 'matching_results.json' in files
                    
                    sessions.append({
                        'session_id': item,
                        'created_at': datetime.fromtimestamp(created_at).isoformat(),
                        'file_count': file_count,
                        'has_results': has_results
                    })
                except Exception as e:
                    print(f"Error processing session {item}: {str(e)}")
                    continue
        
        # Sort by creation time (newest first)
        sessions.sort(key=lambda x: x['created_at'], reverse=True)
        
        return jsonify({'sessions': sessions})
        
    except Exception as e:
        print(f"Error listing sessions: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/restore_session/<session_id>', methods=['GET'])
def restore_session(session_id):
    """Restore a previous session by session ID"""
    try:
        session_dir = os.path.join(app.config['UPLOAD_FOLDER'], session_id)
        
        if not os.path.exists(session_dir):
            return jsonify({'error': 'Session not found'}), 404
        
        # Get all files in session directory
        files = os.listdir(session_dir)
        data_files = [f for f in files if f.startswith('file') and not f.endswith('.json')]
        
        if not data_files:
            return jsonify({'error': 'No data files found in session'}), 404
        
        file_count = len(data_files)
        
        # Check if matching results exist
        results_file = os.path.join(session_dir, 'matching_results.json')
        has_results = os.path.exists(results_file)
        
        # Load data from each file
        files_data = {}
        for i in range(1, file_count + 1):
            # Find the file for this index
            file_pattern = f'file{i}_'
            matching_files = [f for f in data_files if f.startswith(file_pattern)]
            
            if not matching_files:
                continue
            
            filepath = os.path.join(session_dir, matching_files[0])
            df, error = load_data(filepath)
            
            if error or df is None or df.empty:
                continue
            
            files_data[f'filepath{i}'] = filepath
            files_data[f'columns{i}'] = df.columns.tolist()
            files_data[f'sample_data{i}'] = clean_sample_data(df)
        
        # Set session variables
        session['session_id'] = session_id
        session['file_count'] = file_count
        for i in range(1, file_count + 1):
            if f'filepath{i}' in files_data:
                session[f'filepath{i}'] = files_data[f'filepath{i}']
        
        # Find common columns
        all_columns_sets = [set(files_data[f'columns{i}']) for i in range(1, file_count + 1) if f'columns{i}' in files_data]
        common_columns = list(set.intersection(*all_columns_sets)) if len(all_columns_sets) > 1 else []
        
        response_data = {
            'message': f'Session restored successfully',
            'file_count': file_count,
            'common_columns': common_columns,
            'has_results': has_results,
            **files_data
        }
        
        return jsonify(response_data)
        
    except Exception as e:
        print(f"Error restoring session: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/get_matching_results/<session_id>', methods=['GET'])
def get_matching_results(session_id):
    """Get saved matching results for a session"""
    try:
        session_dir = os.path.join(app.config['UPLOAD_FOLDER'], session_id)
        results_file = os.path.join(session_dir, 'matching_results.json')
        
        if not os.path.exists(results_file):
            return jsonify({'error': 'No matching results found for this session'}), 404
        
        with open(results_file, 'r', encoding='utf-8') as f:
            results = json.load(f)
        
        # Get file count from session directory
        files = os.listdir(session_dir)
        data_files = [f for f in files if f.startswith('file') and not f.endswith('.json')]
        file_count = len(data_files)
        
        # Add file_count to results
        results['file_count'] = file_count
        
        return jsonify(results)
        
    except Exception as e:
        print(f"Error getting matching results: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/upload', methods=['POST'])
def upload_file():
    try:
        print("=== DEBUG: Upload route called ===")
        
        # Get file count from request
        file_count = int(request.form.get('file_count', 2))
        print(f"DEBUG: File count: {file_count}")
        
        if file_count < 1 or file_count > 5:
            return jsonify({'error': 'File count must be between 1 and 5'}), 400
        
        # Check if all required files are present
        for i in range(1, file_count + 1):
            if f'file{i}' not in request.files:
                return jsonify({'error': f'File {i} is required'}), 400
        
        # Validate and process all files
        files_data = {}
        dataframes = {}
        allowed_extensions = {'.csv', '.xlsx', '.xls'}
        max_size = 50 * 1024 * 1024  # 50MB
        
        # Generate unique session ID
        session_id = str(uuid.uuid4())
        session_dir = os.path.join(app.config['UPLOAD_FOLDER'], session_id)
        os.makedirs(session_dir, exist_ok=True)
        
        # Process each file
        for i in range(1, file_count + 1):
            file = request.files[f'file{i}']
            
            print(f"DEBUG: Processing File{i}: {file.filename}")
            
            if file.filename == '':
                return jsonify({'error': f'File {i}: Please select a file'}), 400
            
            # Validate file extension
            file_ext = os.path.splitext(file.filename.lower())[1]
            if file_ext not in allowed_extensions:
                return jsonify({'error': f'File {i}: Only CSV, XLSX, and XLS files are allowed'}), 400
            
            # Check file size
            file.seek(0, 2)
            file_size = file.tell()
            file.seek(0)
            
            if file_size > max_size:
                return jsonify({'error': f'File {i}: File size too large. Maximum 50MB allowed.'}), 400
            
            # Save file
            safe_filename = f"file{i}_{session_id[:8]}{file_ext}"
            filepath = os.path.join(session_dir, safe_filename)
            file.save(filepath)
            
            print(f"DEBUG: File{i} saved to {filepath}")
            
            # Load and analyze data
            df, error = load_data(filepath)
            
            if error:
                return jsonify({'error': f'File {i}: Error loading file - {error}'}), 400
            
            if df is None or df.empty:
                return jsonify({'error': f'File {i}: File is empty or has no data'}), 400
            
            print(f"DEBUG: File{i} loaded successfully - {len(df)} rows, {len(df.columns)} columns")
            
            files_data[f'filepath{i}'] = filepath
            files_data[f'columns{i}'] = df.columns.tolist()
            files_data[f'sample_data{i}'] = clean_sample_data(df)
            dataframes[f'df{i}'] = df
        
        # Store file info in session
        session['session_id'] = session_id
        session['file_count'] = file_count
        for i in range(1, file_count + 1):
            session[f'filepath{i}'] = files_data[f'filepath{i}']
        
        # Find common columns across all files (if any)
        all_columns_sets = [set(files_data[f'columns{i}']) for i in range(1, file_count + 1)]
        common_columns = list(set.intersection(*all_columns_sets)) if len(all_columns_sets) > 1 else []
        
        # Get original filenames for database
        original_filenames = []
        total_rows = 0
        for i in range(1, file_count + 1):
            file = request.files[f'file{i}']
            original_filenames.append(file.filename)
            df = dataframes.get(f'df{i}')
            if df is not None:
                total_rows += len(df)
        
        # Save session to database
        session_info = {
            'session_id': session_id,
            'created_at': datetime.now().isoformat(),
            'file_count': file_count,
            'filenames': original_filenames,
            'total_rows': total_rows,
            'status': 'uploaded',
            'progress_current': 0,
            'progress_total': 0,
            'has_results': False
        }
        add_session_to_db(session_info)
        
        response_data = {
            'message': f'{file_count} file(s) uploaded successfully',
            'file_count': file_count,
            'common_columns': common_columns,
            'session_id': session_id,
            **files_data
        }
        
        print("DEBUG: Upload successful")
        
        response = jsonify(response_data)
        response.headers['Content-Type'] = 'application/json'
        response.headers['Cache-Control'] = 'no-cache'
        return response
        
    except Exception as e:
        print(f"DEBUG: Exception occurred: {str(e)}")
        import traceback
        traceback.print_exc()
        
        error_response = jsonify({'error': f'Server error: {str(e)}'})
        error_response.headers['Content-Type'] = 'application/json'
        return error_response, 500

def clean_sample_data(df):
    """Clean dataframe for JSON serialization"""
    try:
        # Get first 10 rows for preview
        sample_df = df.head(10).copy()
        
        # Replace NaN, None, inf values with None for proper JSON serialization
        sample_df = sample_df.where(pd.notna(sample_df), None)
        
        # Convert to dict and ensure all values are JSON serializable
        records = []
        for _, row in sample_df.iterrows():
            record = {}
            for col, val in row.items():
                # Keep serialization resilient per-cell so one bad value doesn't drop the whole preview.
                try:
                    if val is None:
                        record[str(col)] = None
                        continue

                    if isinstance(val, (float, np.floating)):
                        if pd.isna(val) or not np.isfinite(val):
                            record[str(col)] = None
                        else:
                            record[str(col)] = float(val)
                        continue

                    if isinstance(val, (int, np.integer)):
                        record[str(col)] = int(val)
                        continue

                    if isinstance(val, (bool, np.bool_)):
                        record[str(col)] = bool(val)
                        continue

                    if isinstance(val, str):
                        record[str(col)] = val
                        continue

                    # Safe NaN/NA check for remaining scalar-like values
                    is_missing = False
                    try:
                        is_missing = bool(pd.isna(val))
                    except Exception:
                        is_missing = False

                    if is_missing:
                        record[str(col)] = None
                    else:
                        record[str(col)] = str(val)
                except Exception:
                    # Fallback to string representation if a single cell is problematic
                    record[str(col)] = str(val) if val is not None else None
            records.append(record)
        return records
    except Exception as e:
        print(f"DEBUG: Error cleaning sample data: {str(e)}")
        # Return empty list if cleaning fails
        return []

@app.route('/process_matching', methods=['POST'])
def process_matching():
    try:
        data = request.json
        
        # Get file count and paths
        file_count = data.get('file_count') or session.get('file_count', 2)
        file_count = int(file_count)
        
        # Get session ID for progress tracking
        session_id = data.get('session_id') or session.get('session_id')
        
        # Check if resuming from checkpoint - ensure it's an integer
        resume_from = data.get('resume_from', 0)
        if isinstance(resume_from, dict):
            resume_from = 0
        resume_from = int(resume_from) if resume_from else 0
        
        print(f"DEBUG: Processing matching for {file_count} file(s), session: {session_id}, resume_from: {resume_from}")
        
        # Ensure similarity_threshold is an integer
        similarity_threshold = data.get('similarity_threshold', 50)
        if isinstance(similarity_threshold, dict):
            similarity_threshold = 50
        similarity_threshold = int(similarity_threshold) if similarity_threshold else 50
        
        # Update session status to processing
        if session_id:
            update_session_in_db(session_id, {'status': 'processing'})
        
        # Handle single file mode (self-matching)
        if file_count == 1:
            filepath1 = data.get('filepaths', {}).get('filepath1') or session.get('filepath1')
            if not filepath1:
                return jsonify({'error': 'File path not found. Please upload file first.'}), 400
            
            df1, error1 = load_data(filepath1)
            if error1:
                return jsonify({'error': f'Error loading file: {error1}'}), 400
            
            print(f"DEBUG: Single file matching mode")
            
            # Handle field mappings for single file
            if 'field_mappings' in data and data['field_mappings']:
                field_mappings = data['field_mappings']
                single_file_mappings = []
                for mapping in field_mappings:
                    # Extract field1 from the fields dict
                    field1 = mapping.get('fields', {}).get('field1')
                    if field1:
                        single_file_mappings.append({
                            'field1': field1,
                            'min_accuracy': mapping.get('min_accuracy', 50),
                            'is_priority': mapping.get('is_priority', False)
                        })
                
                session['field_mappings'] = field_mappings
                session['similarity_threshold'] = similarity_threshold
                
                results = match_data_within_single_file(df1, single_file_mappings, similarity_threshold, session_id, resume_from)
            else:
                return jsonify({'error': 'No fields specified for matching'}), 400
        
        # Handle two files mode
        elif file_count == 2:
            filepath1 = data.get('filepaths', {}).get('filepath1') or session.get('filepath1')
            filepath2 = data.get('filepaths', {}).get('filepath2') or session.get('filepath2')
            
            if not filepath1 or not filepath2:
                return jsonify({'error': 'File paths not found. Please upload files first.'}), 400
            
            df1, error1 = load_data(filepath1)
            df2, error2 = load_data(filepath2)
            
            if error1:
                return jsonify({'error': f'Error loading file 1: {error1}'}), 400
            if error2:
                return jsonify({'error': f'Error loading file 2: {error2}'}), 400
            
            print(f"DEBUG: Two files matching mode")
            
            # Handle field mappings
            if 'field_mappings' in data and data['field_mappings']:
                field_mappings = data['field_mappings']
                # Convert to old format for compatibility
                converted_mappings = []
                for mapping in field_mappings:
                    fields = mapping.get('fields', {})
                    field1 = fields.get('field1')
                    field2 = fields.get('field2')
                    if field1 and field2:
                        converted_mappings.append({
                            'field1': field1,
                            'field2': field2,
                            'min_accuracy': mapping.get('min_accuracy', 50),
                            'is_priority': mapping.get('is_priority', False)
                        })
                
                session['field_mappings'] = field_mappings
                session['similarity_threshold'] = similarity_threshold
                
                results = match_data_with_mapping(df1, df2, converted_mappings, similarity_threshold, session_id, resume_from)
            else:
                return jsonify({'error': 'No fields specified for matching'}), 400
        
        # Handle multiple files mode (3-5 files)
        # Logic: File1 is the reference, compare all other files against File1
        else:
            # Load all files
            dataframes = {}
            for i in range(1, file_count + 1):
                filepath = data.get('filepaths', {}).get(f'filepath{i}') or session.get(f'filepath{i}')
                if not filepath:
                    return jsonify({'error': f'File path {i} not found. Please upload all files first.'}), 400
                
                df, error = load_data(filepath)
                if error:
                    return jsonify({'error': f'Error loading file {i}: {error}'}), 400
                
                dataframes[f'df{i}'] = df
            
            print(f"DEBUG: Multiple files matching mode - {file_count} files")
            
            # Handle field mappings - compare each file with file1
            if 'field_mappings' in data and data['field_mappings']:
                field_mappings = data['field_mappings']
                session['field_mappings'] = field_mappings
                session['similarity_threshold'] = similarity_threshold
                
                # Perform matching for each file pair (file1 vs fileN)
                all_results = {
                    'matched_data': [],
                    'unmatched_data': [],
                    'summary': {
                        'total_file1': len(dataframes['df1']),
                        'matched_count': 0,
                        'unmatched_count': 0
                    }
                }
                
                df1 = dataframes['df1']
                
                # For each comparison file (file2, file3, file4, file5...)
                for i in range(2, file_count + 1):
                    dfN = dataframes[f'df{i}']
                    
                    # Convert field mappings for this pair (file1 vs fileN)
                    pair_mappings = []
                    for mapping in field_mappings:
                        fields = mapping.get('fields', {})
                        field1 = fields.get('field1')
                        fieldN = fields.get(f'field{i}')
                        if field1 and fieldN:
                            pair_mappings.append({
                                'field1': field1,
                                'field2': fieldN,  # Use fieldN as field2
                                'min_accuracy': mapping.get('min_accuracy', 50),
                                'is_priority': mapping.get('is_priority', False)
                            })
                    
                    # Match this pair
                    pair_results = match_data_with_mapping(df1, dfN, pair_mappings, similarity_threshold, session_id, resume_from)
                    
                    # Append results with file number indicator
                    for item in pair_results['matched_data']:
                        item['comparison_file'] = i
                        all_results['matched_data'].append(item)
                    
                    for item in pair_results['unmatched_data']:
                        item['comparison_file'] = i
                        all_results['unmatched_data'].append(item)
                    
                    all_results['summary']['matched_count'] += pair_results['summary']['matched_count']
                    all_results['summary']['unmatched_count'] += pair_results['summary']['unmatched_count']
                
                results = all_results
            else:
                return jsonify({'error': 'No fields specified for matching'}), 400
        
        # Save results to JSON file for faster export
        session_id = session.get('session_id')
        if session_id:
            session_dir = os.path.join(app.config['UPLOAD_FOLDER'], session_id)
            results_file = os.path.join(session_dir, 'matching_results.json')
            
            # Determine matching mode for backward compatibility
            if file_count == 1:
                matching_mode = 'self'
            elif file_count == 2:
                matching_mode = 'compare'
            else:
                matching_mode = 'multi'
            
            # Prepare results for JSON serialization
            json_results = {
                'matched_data': results['matched_data'],
                'unmatched_data': results['unmatched_data'],
                'summary': results['summary'],
                'field_mappings': field_mappings if 'field_mappings' in locals() else [],
                'similarity_threshold': similarity_threshold,
                'matching_mode': matching_mode,
                'file_count': file_count,
                'timestamp': datetime.now().isoformat()
            }
            
            try:
                with open(results_file, 'w', encoding='utf-8') as f:
                    json.dump(json_results, f, ensure_ascii=False, indent=2, default=str)
                
                session['results_file'] = results_file
                print(f"DEBUG: Results saved to {results_file}")
                
                # Update session in database as completed
                update_session_in_db(session_id, {
                    'status': 'completed',
                    'has_results': True,
                    'file_count': file_count,
                    'matched_count': results['summary']['matched_count'],
                    'unmatched_count': results['summary']['unmatched_count']
                })
                
            except Exception as save_error:
                print(f"DEBUG: Failed to save results: {save_error}")
                # Continue without saving, don't fail the request
        
        # Add file_count to response
        results['file_count'] = file_count
        return jsonify(results)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/export', methods=['POST'])
def export_data():
    try:
        # Check dependencies first
        try:
            import pandas as pd
            import openpyxl
            print("DEBUG: Required libraries available")
        except ImportError as import_error:
            return jsonify({'error': f'Missing required library: {str(import_error)}'}), 500
            
        data = request.json
        export_type = data.get('type')  # 'matched' or 'unmatched'
        session_id = data.get('session_id')
        file_count = data.get('file_count', 2)
        
        print(f"DEBUG: Export request - Type: {export_type}, Session: {session_id}, FileCount: {file_count}")
        
        # Try to load from saved JSON first (faster)
        results_file = None
        matching_results = None
        
        # First try to find results file using session_id
        if session_id:
            session_folder = os.path.join(app.config['UPLOAD_FOLDER'], session_id)
            potential_results_file = os.path.join(session_folder, 'matching_results.json')
            if os.path.exists(potential_results_file):
                results_file = potential_results_file
                print(f"DEBUG: Found results file via session_id: {results_file}")
        
        # Fallback to session variable
        if not results_file:
            results_file = session.get('results_file')
        
        if results_file and os.path.exists(results_file):
            try:
                with open(results_file, 'r', encoding='utf-8') as f:
                    matching_results = json.load(f)
                print(f"DEBUG: Loaded results from JSON cache - Matched: {len(matching_results.get('matched_data', []))}, Unmatched: {len(matching_results.get('unmatched_data', []))}")
                # Update file_count from results if available
                file_count = matching_results.get('file_count', file_count)
            except Exception as load_error:
                print(f"DEBUG: Failed to load cached results: {load_error}")
        else:
            print(f"DEBUG: No cached results file found at: {results_file}")
        if not matching_results:
            print("DEBUG: Regenerating matching results for export")
            filepath1 = data.get('filepath1') or session.get('filepath1')
            filepath2 = data.get('filepath2') or session.get('filepath2')
            
            if not filepath1 or not filepath2:
                return jsonify({'error': 'File paths not found. Please upload files first.'}), 400
            
            # Load the original data
            df1, error1 = load_data(filepath1)
            df2, error2 = load_data(filepath2)
            
            if error1 or error2:
                return jsonify({'error': f'Error loading files: {error1 or error2}'}), 400
            
            # Get the matching parameters from session
            field_mappings = session.get('field_mappings', [])
            similarity_threshold = session.get('similarity_threshold', 50)
            
            if not field_mappings:
                return jsonify({'error': 'No field mappings found. Please process matching first.'}), 400
            
            # Perform matching to get the results
            results = match_data_with_mapping(df1, df2, field_mappings, similarity_threshold)
            matching_results = {
                'matched_data': results['matched_data'],
                'unmatched_data': results['unmatched_data'],
                'summary': results['summary']
            }
        
        # Determine if this is multi-file mode
        is_multi_file = file_count > 2 or any(item.get('comparison_file') for item in matching_results.get('matched_data', []))
        
        # Create Excel file in memory
        output = BytesIO()
        filename = ""
        
        try:
            if export_type == 'matched':
                # For matched data, export File 1 and File 2 data side by side with accuracy scores
                matched_data = matching_results['matched_data']
                if not matched_data:
                    return jsonify({'error': 'No matched data to export'}), 400
                
                # Prepare export data with File 1 and File 2 data organized side by side
                export_records = []
                for item in matched_data:
                    record = {}
                    
                    # For multi-file mode, add comparison file indicator first
                    if is_multi_file:
                        comparison_file = item.get('comparison_file', 2)
                        record['File_Pembanding'] = f"File {comparison_file}"
                    
                    # Add File 1 data first
                    if item.get('file1_data'):
                        for key, value in item['file1_data'].items():
                            # Format all values as text to prevent scientific notation
                            if value is not None:
                                record[f'File1_{str(key)}'] = str(value)
                            else:
                                record[f'File1_{str(key)}'] = ""
                    
                    # Add comparison file data (file2_data contains the comparison file's data)
                    if item.get('file2_data'):
                        # Label based on whether multi-file or not
                        prefix = 'FilePembanding' if is_multi_file else 'File2'
                        for key, value in item['file2_data'].items():
                            # Format all values as text to prevent scientific notation
                            if value is not None:
                                record[f'{prefix}_{str(key)}'] = str(value)
                            else:
                                record[f'{prefix}_{str(key)}'] = ""
                    
                    # Add overall accuracy
                    record['Overall_Accuracy'] = f"{round(item.get('overall_accuracy', 0), 2)}%"
                    
                    # Add individual field accuracies
                    if item.get('field_accuracies'):
                        for field_pair, accuracy in item['field_accuracies'].items():
                            record[f'FieldAccuracy_{field_pair}'] = f"{round(accuracy, 2)}%"
                    
                    # Add priority match indicator if available
                    if item.get('priority_match'):
                        record['Priority_Match'] = "YES"
                    else:
                        record['Priority_Match'] = "NO"
                    
                    export_records.append(record)
                
                df_export = pd.DataFrame(export_records)
                
                # Reorder columns for better readability
                columns = list(df_export.columns)
                file1_cols = [col for col in columns if col.startswith('File1_')]
                file_pembanding_cols = [col for col in columns if col.startswith('FilePembanding_')]
                file2_cols = [col for col in columns if col.startswith('File2_')]
                accuracy_cols = [col for col in columns if col.startswith('FieldAccuracy_')]
                
                # Build ordered columns based on mode
                if is_multi_file:
                    # Multi-file: File_Pembanding indicator, File1, FilePembanding, accuracies
                    ordered_columns = ['File_Pembanding'] + file1_cols + file_pembanding_cols + ['Overall_Accuracy'] + accuracy_cols + ['Priority_Match']
                else:
                    # Standard 2-file mode
                    ordered_columns = file1_cols + file2_cols + ['Overall_Accuracy'] + accuracy_cols + ['Priority_Match']
                
                # Filter to only existing columns
                ordered_columns = [col for col in ordered_columns if col in df_export.columns]
                df_export = df_export[ordered_columns]
                
                filename = f"data_cocok_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                
                # Simple Excel export without complex formatting to avoid errors
                df_export.to_excel(output, index=False, sheet_name='Data Cocok', engine='openpyxl')
                
            elif export_type == 'unmatched':
                # For unmatched data, export File 1 data with best candidate
                unmatched_data = matching_results['unmatched_data']
                if not unmatched_data:
                    return jsonify({'error': 'No unmatched data to export'}), 400
                
                export_records = []
                for item in unmatched_data:
                    record = {}
                    
                    # Add File 1 data first
                    # For multi-file mode, add comparison file indicator first
                    if is_multi_file:
                        comparison_file = item.get('comparison_file', 2)
                        record['File_Pembanding'] = f"File {comparison_file}"
                    
                    # Add File 1 data first
                    if item.get('file1_data'):
                        for key, value in item['file1_data'].items():
                            # Format all values as text to prevent scientific notation
                            if value is not None:
                                record[f'File1_{str(key)}'] = str(value)
                            else:
                                record[f'File1_{str(key)}'] = ""
                    
                    # Add comparison file data (best candidate)
                    if item.get('file2_data'):
                        prefix = 'BestCandidate' if is_multi_file else 'File2_BestCandidate'
                        for key, value in item['file2_data'].items():
                            # Format all values as text to prevent scientific notation
                            if value is not None:
                                record[f'{prefix}_{str(key)}'] = str(value)
                            else:
                                record[f'{prefix}_{str(key)}'] = ""
                    
                    # Add overall accuracy of best candidate
                    record['Best_Candidate_Accuracy'] = f"{round(item.get('overall_accuracy', 0), 2)}%"
                    
                    # Add individual field accuracies of best candidate
                    if item.get('field_accuracies'):
                        for field_pair, accuracy in item['field_accuracies'].items():
                            record[f'FieldAccuracy_{field_pair}'] = f"{round(accuracy, 2)}%"
                    
                    # Add reason for not matching
                    if item.get('overall_accuracy', 0) > 0:
                        record['Reason_Not_Matched'] = "Did not meet accuracy requirements"
                    else:
                        record['Reason_Not_Matched'] = "No suitable candidate found"
                    
                    export_records.append(record)
                
                df_export = pd.DataFrame(export_records)
                
                # Reorder columns for better readability
                columns = list(df_export.columns)
                file1_cols = [col for col in columns if col.startswith('File1_')]
                best_candidate_cols = [col for col in columns if col.startswith('BestCandidate_')]
                file2_best_cols = [col for col in columns if col.startswith('File2_BestCandidate_')]
                accuracy_cols = [col for col in columns if col.startswith('FieldAccuracy_')]
                
                # Build ordered columns based on mode
                if is_multi_file:
                    ordered_columns = ['File_Pembanding'] + file1_cols + best_candidate_cols + ['Best_Candidate_Accuracy'] + accuracy_cols + ['Reason_Not_Matched']
                else:
                    ordered_columns = file1_cols + file2_best_cols + ['Best_Candidate_Accuracy'] + accuracy_cols + ['Reason_Not_Matched']
                
                # Filter to only existing columns
                ordered_columns = [col for col in ordered_columns if col in df_export.columns]
                df_export = df_export[ordered_columns]
                
                filename = f"data_tidak_cocok_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                
                # Simple Excel export without complex formatting to avoid errors
                df_export.to_excel(output, index=False, sheet_name='Data Tidak Cocok', engine='openpyxl')
                
            else:
                return jsonify({'error': 'Invalid export type'}), 400
            
            print(f"DEBUG: Excel file created successfully: {filename}")
            
        except Exception as excel_error:
            print(f"DEBUG: Excel creation error: {str(excel_error)}")
            import traceback
            traceback.print_exc()
            return jsonify({'error': f'Error creating Excel file: {str(excel_error)}'}), 500
        
        output.seek(0)
        
        print(f"DEBUG: Sending file: {filename}, Size: {len(output.getvalue())} bytes")
        
        try:
            response = send_file(
                output,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name=filename
            )
            print("DEBUG: File response created successfully")
            return response
        except Exception as send_error:
            print(f"DEBUG: Error sending file: {str(send_error)}")
            return jsonify({'error': f'Error sending file: {str(send_error)}'}), 500
        
    except Exception as e:
        print(f"DEBUG: Export error: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'Export error: {str(e)}'}), 500

@app.route('/reset')
def reset():
    # Clear session data
    session.clear()
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True, port=5001)