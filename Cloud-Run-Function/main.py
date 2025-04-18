import functions_framework
import google.cloud.videointelligence as videointelligence
import os
import re

# --- Configuration ---
OUTPUT_FOLDER = 'transcriptions/'
# -------------------

@functions_framework.cloud_event
def process_video_transcription(cloud_event):
    """
    Cloud Function triggered by Cloud Storage events to transcribe videos.

    Args:
        cloud_event: The CloudEvent data associated with the trigger.
                     Contains information about the uploaded file.
    """
    data = cloud_event.data

    bucket_name = data.get("bucket")
    file_name = data.get("name")

    if not bucket_name or not file_name:
        print("Error: Bucket name or file name missing in event data.")
        return

    # --- Input Validation ---
    # 1. Prevent infinite loops: Check if the file is already in the output folder.
    if file_name.startswith(OUTPUT_FOLDER):
        print(f"File '{file_name}' is in the output folder. Skipping.")
        return

    # 2. Basic video format check
    supported_extensions = ('.mp4', '.mov', '.avi', '.mpg', '.mpeg', '.mkv', '.webm')
    if not file_name.lower().endswith(supported_extensions):
        print(f"File '{file_name}' is not a supported video type. Skipping.")
        return
    # -----------------------

    print(f"Processing file: gs://{bucket_name}/{file_name}")

    # Construct GCS URIs
    gcs_input_uri = f"gs://{bucket_name}/{file_name}"

    # Create a base name for the output by removing extension
    base_name = os.path.splitext(file_name)[0]
    # Define the output path within the specified folder
    gcs_output_uri = f"gs://{bucket_name}/{OUTPUT_FOLDER}{base_name}.json"

    # Initialize the Video Intelligence client
    video_client = videointelligence.VideoIntelligenceServiceClient()

    # Configure the transcription request
    config = videointelligence.SpeechTranscriptionConfig(
        language_code="en-US",
        enable_automatic_punctuation=True,
        enable_speaker_diarization=True, # Identify different speakers
    )

    # Set up the features and context for the annotation request
    video_context = videointelligence.VideoContext(speech_transcription_config=config)

    try:
        # Start the asynchronous video annotation request
        print(f"Starting transcription job for {gcs_input_uri}")
        print(f"Output will be saved to {gcs_output_uri}")

        operation = video_client.annotate_video(
            request={
                "features": [videointelligence.Feature.SPEECH_TRANSCRIPTION],
                "input_uri": gcs_input_uri,
                "output_uri": gcs_output_uri,
                "video_context": video_context,
            }
        )

        print(f"Transcription operation started: {operation.metadata.name}")
        # The function finishes here. The actual transcription happens asynchronously.
        # The results will be written to gcs_output_uri by the Video Intelligence service.

    except Exception as e:
        print(f"Error starting transcription for {gcs_input_uri}: {e}")