from flask import Flask, render_template, request, jsonify, session, redirect, url_for
import pandas as pd
import numpy as np
import os
from datetime import datetime
import uuid
from fuzzywuzzy import fuzz
import tempfile

app = Flask(__name__)
app.secret_key = 'your_secret_key_here'  # Change this to a random secret key
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB max file size

# Add CORS headers
@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE')
    return response

# Ensure upload directory exists
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

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
    
    # If both are empty strings after stripping
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

def normalize_result_data(row1, row2, field_mappings):
    """Normalize data to ensure consistent structure across all results"""
    file1_data = {}
    file2_data = {}
    
    for mapping in field_mappings:
        field1 = mapping['field1']
        field2 = mapping['field2']
        
        # Add field1 data with proper null handling
        if row1 is not None and not row1.empty and field1 in row1.index:
            value1 = row1[field1]
            file1_data[field1] = None if pd.isna(value1) else value1
        else:
            file1_data[field1] = None
            
        # Add field2 data with proper null handling
        if row2 is not None and not row2.empty and field2 in row2.index:
            value2 = row2[field2]
            file2_data[field2] = None if pd.isna(value2) else value2
        else:
            file2_data[field2] = None
    
    return file1_data, file2_data

def match_data_with_mapping(df1, df2, field_mappings, similarity_threshold=50):
    """
    Match data between two dataframes based on specified field mappings
    field_mappings: list of dicts with 'field1' and 'field2' keys
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
        best_field_scores = {}
        
        for idx2, row2 in df2.iterrows():
            if idx2 in matched_indices_df2:
                continue
                
            # Calculate average similarity across all specified field mappings
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
            
            if avg_score > best_score:
                best_score = avg_score
                best_match = row2
                best_idx2 = idx2
                best_field_scores = field_scores
        
        # Create result entry using normalized data to ensure consistent structure
        file1_data, file2_data = normalize_result_data(row1, best_match, field_mappings)
        
        result_entry = {
            'file1_data': file1_data,
            'file2_data': file2_data,
            'overall_accuracy': round(best_score, 2),
            'field_accuracies': best_field_scores,
            'is_match': best_score >= similarity_threshold
        }
        
        if best_score >= similarity_threshold and best_match is not None:
            # Mark this File 2 row as used (PENTING!)
            matched_indices_df2.add(best_idx2)
            results['matched_data'].append(result_entry)
            results['summary']['matched_count'] += 1
        else:
            # PERBAIKAN: Jangan assign best_match jika tidak memenuhi threshold
            # Set file2_data to None untuk data yang tidak cocok
            file1_data, _ = normalize_result_data(row1, None, field_mappings)
            empty_file2_data = {mapping['field2']: None for mapping in field_mappings}
            
            unmatched_result = {
                'file1_data': file1_data,
                'file2_data': empty_file2_data,
                'overall_accuracy': 0.0,
                'field_accuracies': {},
                'is_match': False
            }
            results['unmatched_data'].append(unmatched_result)
            results['summary']['unmatched_count'] += 1
    
    # Add unmatched entries from df2
    for idx2, row2 in df2.iterrows():
        if idx2 not in matched_indices_df2:
            # Normalize unmatched data from file2
            empty_row = pd.Series()
            file1_data, file2_data = normalize_result_data(empty_row, row2, field_mappings)
            
            unmatched_entry = {
                'file1_data': file1_data,
                'file2_data': file2_data,
                'overall_accuracy': 0.0,
                'field_accuracies': {},
                'is_match': False
            }
            results['unmatched_data'].append(unmatched_entry)
            results['summary']['unmatched_count'] += 1
    
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

@app.route('/upload', methods=['POST'])
def upload_file():
    try:
        print("=== DEBUG: Upload route called ===")
        
        if 'file1' not in request.files or 'file2' not in request.files:
            print("DEBUG: Missing files in request")
            return jsonify({'error': 'Both files are required'}), 400
        
        file1 = request.files['file1']
        file2 = request.files['file2']
        
        print(f"DEBUG: File1: {file1.filename}, File2: {file2.filename}")
        
        if file1.filename == '' or file2.filename == '':
            print("DEBUG: Empty filename detected")
            return jsonify({'error': 'Please select both files'}), 400
        
        # Validate file extensions
        allowed_extensions = {'.csv', '.xlsx', '.xls'}
        file1_ext = os.path.splitext(file1.filename.lower())[1]
        file2_ext = os.path.splitext(file2.filename.lower())[1]
        
        if file1_ext not in allowed_extensions or file2_ext not in allowed_extensions:
            return jsonify({'error': 'Only CSV, XLSX, and XLS files are allowed'}), 400
        
        # Check file size (max 50MB)
        file1.seek(0, 2)  # Seek to end
        file1_size = file1.tell()
        file1.seek(0)  # Reset to beginning
        
        file2.seek(0, 2)
        file2_size = file2.tell()
        file2.seek(0)
        
        max_size = 50 * 1024 * 1024  # 50MB
        if file1_size > max_size or file2_size > max_size:
            return jsonify({'error': 'File size too large. Maximum 50MB allowed.'}), 400
        
        print(f"DEBUG: File sizes - File1: {file1_size} bytes, File2: {file2_size} bytes")
        
        # Generate unique session ID
        session_id = str(uuid.uuid4())
        session['session_id'] = session_id
        
        # Create session directory
        session_dir = os.path.join(app.config['UPLOAD_FOLDER'], session_id)
        os.makedirs(session_dir, exist_ok=True)
        
        # Save files with safer filenames
        safe_filename1 = f"file1_{session_id[:8]}{file1_ext}"
        safe_filename2 = f"file2_{session_id[:8]}{file2_ext}"
        
        filepath1 = os.path.join(session_dir, safe_filename1)
        filepath2 = os.path.join(session_dir, safe_filename2)
        
        print(f"DEBUG: Saving to {filepath1} and {filepath2}")
        
        file1.save(filepath1)
        file2.save(filepath2)
        
        print("DEBUG: Files saved successfully")
        
        # Load and analyze data
        print("DEBUG: Loading data...")
        df1, error1 = load_data(filepath1)
        df2, error2 = load_data(filepath2)
        
        print(f"DEBUG: Load results - Error1: {error1}, Error2: {error2}")
        
        if error1:
            return jsonify({'error': f'Error loading File 1: {error1}'}), 400
        if error2:
            return jsonify({'error': f'Error loading File 2: {error2}'}), 400
        
        if df1 is None or df2 is None:
            return jsonify({'error': 'Failed to load one or both files'}), 400
            
        if df1.empty:
            return jsonify({'error': 'File 1 is empty or has no data'}), 400
        if df2.empty:
            return jsonify({'error': 'File 2 is empty or has no data'}), 400
        
        print(f"DEBUG: Data loaded successfully - DF1: {len(df1)} rows, DF2: {len(df2)} rows")
        print(f"DEBUG: DF1 columns: {df1.columns.tolist()}")
        print(f"DEBUG: DF2 columns: {df2.columns.tolist()}")
        
        # Store file info in session
        session['filepath1'] = filepath1
        session['filepath2'] = filepath2
        
        # Find common columns for better UX
        common_columns = list(set(df1.columns.tolist()) & set(df2.columns.tolist()))
        
        # Clean sample data for JSON serialization
        def clean_sample_data(df):
            """Clean dataframe for JSON serialization"""
            try:
                # Get first 3 rows
                sample_df = df.head(3).copy()
                
                # Replace NaN, None, inf values with None for proper JSON serialization
                sample_df = sample_df.where(pd.notna(sample_df), None)
                
                # Convert to dict and ensure all values are JSON serializable
                records = []
                for _, row in sample_df.iterrows():
                    record = {}
                    for col, val in row.items():
                        if pd.isna(val) or val is None:
                            record[str(col)] = None
                        elif isinstance(val, (int, float, str, bool)):
                            if isinstance(val, float):
                                # Check for NaN, infinity, or -infinity
                                if pd.isna(val) or not np.isfinite(val):
                                    record[str(col)] = None
                                else:
                                    record[str(col)] = val
                            else:
                                record[str(col)] = val
                        else:
                            # Convert other types to string
                            record[str(col)] = str(val) if val is not None else None
                    records.append(record)
                return records
            except Exception as e:
                print(f"DEBUG: Error cleaning sample data: {str(e)}")
                # Return empty list if cleaning fails
                return []
        
        sample_data1 = clean_sample_data(df1)
        sample_data2 = clean_sample_data(df2)
        
        response_data = {
            'message': 'Files uploaded successfully',
            'filepath1': filepath1,
            'filepath2': filepath2,
            'columns1': df1.columns.tolist(),
            'columns2': df2.columns.tolist(),
            'common_columns': common_columns,
            'sample_data1': sample_data1,
            'sample_data2': sample_data2,
        }
        
        print("DEBUG: Response data prepared")
        
        # Test JSON serialization before returning
        try:
            import json
            json_test = json.dumps(response_data)
            print(f"DEBUG: JSON serialization successful, size: {len(json_test)} bytes")
        except Exception as json_error:
            print(f"DEBUG: JSON serialization error: {str(json_error)}")
            # Return minimal response if serialization fails
            return jsonify({
                'message': 'Files uploaded successfully',
                'filepath1': filepath1,
                'filepath2': filepath2,
                'columns1': df1.columns.tolist(),
                'columns2': df2.columns.tolist(),
                'common_columns': common_columns,
                'sample_data1': [],
                'sample_data2': [],
            })
        
        print("DEBUG: Upload successful")
        
        # Create response with explicit content type
        response = jsonify(response_data)
        response.headers['Content-Type'] = 'application/json'
        response.headers['Cache-Control'] = 'no-cache'
        return response
        
    except Exception as e:
        print(f"DEBUG: Exception occurred: {str(e)}")
        import traceback
        traceback.print_exc()
        
        # Ensure error response is also proper JSON
        error_response = jsonify({'error': f'Server error: {str(e)}'})
        error_response.headers['Content-Type'] = 'application/json'
        return error_response, 500

@app.route('/process_matching', methods=['POST'])
def process_matching():
    try:
        data = request.json
        
        # Get file paths from request or session
        filepath1 = data.get('filepath1') or session.get('filepath1')
        filepath2 = data.get('filepath2') or session.get('filepath2')
        
        if not filepath1 or not filepath2:
            return jsonify({'error': 'File paths not found. Please upload files first.'}), 400
        
        similarity_threshold = data.get('similarity_threshold', 50)
        
        # Load data
        df1, error1 = load_data(filepath1)
        df2, error2 = load_data(filepath2)
        
        if error1 or error2:
            return jsonify({'error': f'Error loading files: {error1 or error2}'}), 400
        
        # Handle both field mapping and legacy field selection
        if 'field_mappings' in data and data['field_mappings']:
            # New field mapping approach
            field_mappings = data['field_mappings']
            results = match_data_with_mapping(df1, df2, field_mappings, similarity_threshold)
        elif 'fields_to_match' in data and data['fields_to_match']:
            # Legacy approach for backward compatibility
            fields_to_match = data['fields_to_match']
            results = match_data(df1, df2, fields_to_match, similarity_threshold)
        else:
            return jsonify({'error': 'No fields specified for matching'}), 400
        
        return jsonify(results)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/reset')
def reset():
    # Clear session data
    session.clear()
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True, port=5001)