from flask import Flask, request, send_file, render_template, abort, jsonify, redirect, url_for, after_this_request
import ffmpeg
import os
import uuid
import google.auth
import sys
from google.cloud import storage
from google.cloud.exceptions import NotFound # Import NotFound exception
import logging # Use logging for better diagnostics

# --- Basic Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration ---
# GCS Bucket Names (IMPORTANT: Replace with your actual bucket names)
INPUT_BUCKET_NAME = 'video-image-input-bucket'
OUTPUT_BUCKET_NAME = 'video-image-output-bucket'
TEMP_DOWNLOAD_DIR = '/tmp/videoconverter_downloads' # Dedicated temp dir for downloads served to user
TEMP_PROCESSING_DIR = '/tmp/videoconverter_processing' # Temp dir for ffmpeg processing

# Initialize GCS Client
storage_client = None
input_bucket = None
output_bucket = None
try:
    storage_client = storage.Client()
    input_bucket = storage_client.bucket(INPUT_BUCKET_NAME)
    output_bucket = storage_client.bucket(OUTPUT_BUCKET_NAME)
    logging.info(f"GCS client initialized. Input: gs://{INPUT_BUCKET_NAME}, Output: gs://{OUTPUT_BUCKET_NAME}")
    # Ensure the temporary directories exist
    os.makedirs(TEMP_DOWNLOAD_DIR, exist_ok=True)
    os.makedirs(TEMP_PROCESSING_DIR, exist_ok=True)
    logging.info(f"Temporary download directory: {TEMP_DOWNLOAD_DIR}")
    logging.info(f"Temporary processing directory: {TEMP_PROCESSING_DIR}")
except Exception as e:
    logging.critical(f"FATAL: Error initializing GCS client or creating temp dirs: {e}")
    # Application might not function correctly without GCS client.

# --- Credentials Check (for debugging) ---
try:
    credentials, project_id = google.auth.default()
    logging.info(f"DEBUG: Autodetected Project ID: {project_id}")
    if hasattr(credentials, 'service_account_email'):
        logging.info(f"DEBUG: Using Service Account: {credentials.service_account_email}")
    else:
        logging.info("DEBUG: Credentials type might be user credentials or other.")
except Exception as e:
    logging.warning(f"DEBUG: Error getting default credentials: {e}")

app = Flask(__name__)

# --- Helper Function for GCS Download ---
def download_blob_and_serve(bucket, blob_name, download_filename):
    """Downloads a blob from GCS, serves it, and cleans up."""
    if not storage_client:
        logging.error("GCS client not initialized. Aborting download.")
        abort(500, description="Server configuration error: Storage client unavailable.")

    # Use the dedicated download temp directory
    local_temp_download_path = os.path.join(TEMP_DOWNLOAD_DIR, download_filename)

    logging.info(f"Download request for GCS file: gs://{bucket.name}/{blob_name}")
    logging.info(f"Attempting to download to temporary path: {local_temp_download_path}")

    try:
        # Get the blob object from GCS
        blob = bucket.blob(blob_name)

        # Check if blob exists before attempting download
        if not blob.exists():
            logging.warning(f"Requested download file not found in GCS: gs://{bucket.name}/{blob_name}")
            abort(404, description="File not found.")

        # Ensure the directory exists before downloading
        os.makedirs(os.path.dirname(local_temp_download_path), exist_ok=True)
        blob.download_to_filename(local_temp_download_path)
        logging.info(f"Successfully downloaded gs://{bucket.name}/{blob_name} to {local_temp_download_path}")

        # Prepare cleanup action: This function will run after the request is completed
        @after_this_request
        def remove_temp_file(response):
            try:
                os.remove(local_temp_download_path)
                logging.info(f"Cleaned up temporary download file: {local_temp_download_path}")
            except OSError as e:
                logging.error(f"Error removing temporary download file {local_temp_download_path}: {e}")
            return response # Important: return the response object

        # Send the file to the user
        logging.info(f"Sending file {local_temp_download_path} to user as attachment '{download_filename}'.")
        return send_file(
            local_temp_download_path,
            as_attachment=True,
            download_name=download_filename # Set the filename the user sees
        )

    except NotFound: # This case is covered by blob.exists() now, but keep for robustness
        logging.warning(f"NotFound exception caught for GCS file: gs://{bucket.name}/{blob_name} (should have been caught by exists())")
        abort(404, description="File not found.")
    except Exception as e:
        logging.error(f"Error during GCS download or sending file (gs://{bucket.name}/{blob_name}): {e}", exc_info=True)
        # Clean up if a partial file was somehow created locally
        if os.path.exists(local_temp_download_path):
            try:
                os.remove(local_temp_download_path)
            except OSError:
                pass # Ignore error if cleanup fails here
        abort(500, description=f"An error occurred while processing the download: {e}")

# --- Main Page Route ---
@app.route('/')
def index():
    """Renders the main upload form."""
    return render_template('index.html')

# --- Conversion Route ---
@app.route('/convert', methods=['POST'])
def convert_video():
    """
    Handles video upload, GCS upload, conversion, GCS output upload,
    and redirects to the download options endpoint.
    """
    if not storage_client:
        logging.error("GCS client not initialized. Aborting request.")
        abort(500, description="Server configuration error: Storage client unavailable.")

    if 'video' not in request.files:
        logging.warning("Convert request missing 'video' file part.")
        abort(400, description="Missing file.")

    video_file = request.files['video']
    target_format = request.form.get('format')

    if video_file.filename == '' or not target_format:
        logging.warning(f"Convert request with invalid file ('{video_file.filename}') or format ('{target_format}').")
        abort(400, description="Invalid file or format selected.")

    original_filename = video_file.filename
    base_name, _ = os.path.splitext(original_filename)
    logging.info(f"Received request to convert '{original_filename}' to '{target_format}'")

    # Generate unique identifiers for this job
    job_id = str(uuid.uuid4())
    # Keep original extension for clarity in input filename, use target for output
    gcs_input_filename = f"{job_id}_{original_filename}"
    gcs_output_filename = f"{job_id}_{base_name}.{target_format}" # Filename WITHIN the bucket

    # Define temporary local paths on the VM using the processing temp dir
    local_input_path = os.path.join(TEMP_PROCESSING_DIR, f"input_{gcs_input_filename}")
    local_output_path = os.path.join(TEMP_PROCESSING_DIR, f"output_{gcs_output_filename}")

    # Define GCS object paths (including prefixes/folders)
    input_blob_name = f"uploads/{gcs_input_filename}"
    output_blob_name = f"converted/{gcs_output_filename}" # Full path in the bucket

    # --- Uploading Input File to GCS ---
    try:
        logging.info(f"STEP: Defining input blob: {input_blob_name}")
        input_blob = input_bucket.blob(input_blob_name)
        video_file.seek(0) # Ensure stream is at the beginning

        logging.info(f"STEP: Starting GCS upload to gs://{INPUT_BUCKET_NAME}/{input_blob_name}...")
        input_blob.upload_from_file(video_file, content_type=video_file.content_type)
        logging.info(f"STEP: Successfully uploaded input to gs://{INPUT_BUCKET_NAME}/{input_blob_name}")
    except Exception as e:
        logging.error(f"ERROR: Failed during GCS input upload: {e}", exc_info=True)
        abort(500, description=f"Failed to upload input file to storage: {e}")

    # --- Download Input from GCS to Local Temp Path for Processing ---
    try:
        logging.info(f"STEP: Starting GCS download of input file from gs://{INPUT_BUCKET_NAME}/{input_blob_name} to {local_input_path}")
        # Ensure directory exists
        os.makedirs(os.path.dirname(local_input_path), exist_ok=True)
        input_blob.download_to_filename(local_input_path) # Use the same blob object
        logging.info(f"STEP: Finished GCS download of input file for processing.")
    except NotFound:
        logging.error(f"ERROR: Input blob gs://{INPUT_BUCKET_NAME}/{input_blob_name} not found after upload!", exc_info=True)
        abort(500, description="Internal server error: Uploaded file disappeared.")
    except Exception as e:
        logging.error(f"ERROR: Failed to download input from GCS for processing: {e}", exc_info=True)
        # Clean up local input file if it exists before aborting
        if os.path.exists(local_input_path):
            try:
                os.remove(local_input_path)
            except OSError as rm_err:
                logging.warning(f"Could not remove partial input file {local_input_path}: {rm_err}")
        abort(500, description=f"Failed to retrieve file from storage for processing: {e}")

    # --- Perform Conversion using ffmpeg ---
    try:
        logging.info(f"STEP: Starting ffmpeg conversion: {local_input_path} -> {local_output_path}")
        (
            ffmpeg
            .input(local_input_path)
            .output(local_output_path)
            .run(capture_stdout=True, capture_stderr=True, overwrite_output=True) # Overwrite if exists locally
        )
        logging.info("STEP: Finished ffmpeg conversion.")
    except ffmpeg.Error as e:
        error_message = e.stderr.decode(errors='ignore') if e.stderr else "Unknown ffmpeg error"
        logging.error(f"ERROR: ffmpeg conversion failed: {error_message}")
        # Clean up local input file before aborting (output might not exist or be partial)
        if os.path.exists(local_input_path):
            try:
                os.remove(local_input_path)
            except OSError as rm_err:
                logging.warning(f"Could not remove input file {local_input_path} after ffmpeg error: {rm_err}")
        abort(500, description=f"Error during conversion: {error_message}")
    except Exception as e: # Catch other potential errors during ffmpeg execution
        logging.error(f"ERROR: Unexpected error during ffmpeg processing: {e}", exc_info=True)
        if os.path.exists(local_input_path):
            try:
                os.remove(local_input_path)
            except OSError as rm_err:
                logging.warning(f"Could not remove input file {local_input_path} after unexpected error: {rm_err}")
        abort(500, description=f"Unexpected error during conversion processing: {e}")

    # --- Upload Converted File to Output GCS Bucket ---
    try:
        logging.info(f"STEP: Uploading converted file {local_output_path} to gs://{OUTPUT_BUCKET_NAME}/{output_blob_name}...")
        output_blob = output_bucket.blob(output_blob_name)

        if target_format.lower() == 'mov':
            output_content_type = 'video/quicktime'
        elif target_format.lower() == 'avi':
            output_content_type = 'video/x-msvideo'

        output_blob.upload_from_filename(local_output_path)

        logging.info("STEP: Output upload to GCS complete.")
    except Exception as e:
        logging.error(f"ERROR: Failed to upload result to GCS output bucket: {e}", exc_info=True)
        # Conversion worked, but upload failed. Decide how to handle this.
        # For now, we'll still try to clean up and report an error.
        abort(500, description=f"Conversion successful, but failed to upload output to storage: {e}")
    finally:
        # --- Clean Up Local Temporary Processing Files ---
        # This block executes whether the GCS upload succeeded or failed (but after conversion was attempted)
        logging.info("STEP: Cleaning up local temporary processing files...")
        removed_input, removed_output = False, False
        if os.path.exists(local_input_path):
            try:
                os.remove(local_input_path)
                removed_input = True
                logging.info(f"Successfully removed local processing input file: {local_input_path}")
            except OSError as e:
                logging.warning(f"Could not remove temp processing input file {local_input_path}: {e}")
        if os.path.exists(local_output_path):
            try:
                os.remove(local_output_path)
                removed_output = True
                logging.info(f"Successfully removed local processing output file: {local_output_path}")
            except OSError as e:
                logging.warning(f"Could not remove temp processing output file {local_output_path}: {e}")
        logging.info(f"Local processing file cleanup complete. Removed input: {removed_input}, Removed output: {removed_output}")

    # --- Redirect to Download Options Endpoint ---
    logging.info(f"Successfully processed '{original_filename}'. Redirecting to download options for: {gcs_output_filename}")
    # Pass the GCS output filename (without prefix) as the identifier
    return redirect(url_for('download_options', identifier=gcs_output_filename))


# --- Download Page ---
@app.route('/download_options/<path:identifier>')
def download_options(identifier):
    """
    Displays download links for the converted video and its transcription,
    checking GCS for their existence.
    """
    if not storage_client:
        logging.error("GCS client not initialized. Aborting options request.")
        abort(500, description="Server configuration error: Storage client unavailable.")

    # Construct the expected video blob name
    video_blob_name = f"converted/{identifier}"
    video_available = False
    video_download_url = None

    # Construct the expected transcription blob name
    base_name, _ = os.path.splitext(identifier)
    transcription_filename = f"{base_name}.json"
    transcription_blob_name = f"transcriptions/converted/{transcription_filename}"
    transcription_available = False
    transcription_download_url = None

    try:
        # Check for video file
        video_blob = output_bucket.blob(video_blob_name)
        if video_blob.exists():
            video_available = True
            video_download_url = url_for('download_video_file', filename=identifier)
            logging.info(f"Video file found: gs://{OUTPUT_BUCKET_NAME}/{video_blob_name}")
        else:
            logging.info(f"Video file NOT yet found: gs://{OUTPUT_BUCKET_NAME}/{video_blob_name}")

        # Check for transcription file
        transcription_blob = output_bucket.blob(transcription_blob_name)
        if transcription_blob.exists():
            transcription_available = True
            transcription_download_url = url_for('download_transcription_file', filename=transcription_filename)
            logging.info(f"Transcription file found: gs://{OUTPUT_BUCKET_NAME}/{transcription_blob_name}")
        else:
            logging.info(f"Transcription file NOT yet found: gs://{OUTPUT_BUCKET_NAME}/{transcription_blob_name}")

    except Exception as e:
        logging.error(f"Error checking GCS for files (video: {video_blob_name}, transcript: {transcription_blob_name}): {e}", exc_info=True)
        abort(500, description="Error checking for downloadable files in storage.")

    return render_template('download_options.html',
                            video_filename=identifier,
                            video_available=video_available,
                            video_download_url=video_download_url,
                            transcription_filename=transcription_filename,
                            transcription_available=transcription_available,
                            transcription_download_url=transcription_download_url)


# --- Video Download Endpoint ---
@app.route('/download/video/<path:filename>')
def download_video_file(filename):
    """Downloads the converted video file from GCS."""
    blob_name = f"converted/{filename}"
    logging.info(f"Initiating video download for GCS blob: {blob_name}")
    # Pass the output_bucket object
    return download_blob_and_serve(output_bucket, blob_name, filename)

# --- Transcription Download Endpoint ---
@app.route('/download/transcription/<path:filename>')
def download_transcription_file(filename):
    """Downloads the transcription file (.json) from GCS."""
    blob_name = f"transcriptions/converted/{filename}"
    logging.info(f"Initiating transcription download for GCS blob: {blob_name}")
    # Pass the output_bucket object
    return download_blob_and_serve(output_bucket, blob_name, filename)


# --- Main Execution ---
if __name__ == '__main__':
    # Make sure the host is 0.0.0.0 to be accessible externally (e.g., on GCE/Cloud Run)
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False) # Use PORT env var, default 8080; Turn debug off for production
