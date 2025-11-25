"""
AWS Batch Worker Script for PDF Rotation and Queueing

This script runs in an AWS Batch container and processes PDF rotation
for multiple PDFs in parallel, then queues them to SQS for invoice processing.

Architecture:
1. Lambda receives email → submits Batch job with PDF metadata
2. Batch job processes all PDFs in parallel (with high memory allocation)
3. After each PDF rotation completes → immediately queue to SQS
4. Update DynamoDB with attachment status

Memory considerations:
- 50 PDFs × 15 pages = 750 pages total
- Each page at 300 DPI ≈ 5-10MB in memory
- Total: ~3.75-7.5GB for all images
- Batch job can allocate 30GB+ to handle this safely
"""

import json
import logging
import os
import sys
import boto3
import uuid
import tempfile
import time
import threading
import io
import math
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pdf2image import convert_from_path
import cv2
import numpy as np
import pytesseract
from pytesseract import Output
from PIL import Image
from utils import (
    create_attachment_entry,
    update_attachment_status
)

# Setup logging - CRITICAL: Configure handler to write to stdout/stderr for CloudWatch
# Without explicit handler, logs may be buffered or not appear
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s',
    handlers=[
        logging.StreamHandler()  # Explicitly write to stderr (stdout for Batch)
    ]
)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS Textract is used for orientation detection - no model preloading needed

# Textract pricing constants
# detect_document_text API: $1.50 per 1,000 pages = $0.0015 per page (first 1M pages/month)
# After 1M pages/month: $0.60 per 1,000 pages = $0.0006 per page
TEXTRACT_COST_PER_PAGE = 0.0015  # Default: $0.0015 per page (can be adjusted based on usage tier)

# Force immediate flush for real-time logs in CloudWatch
# Note: sys is already imported above
# IMPORTANT: CloudWatch has log size limits and may truncate very long logs.
# We use aggressive flushing and limit detailed output to prevent truncation.
try:
    # Python 3.7+: Enable line buffering for real-time logs
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
except (AttributeError, ValueError):
    # Fallback for older Python versions or if reconfigure fails
    # The logging StreamHandler will still work, just may be slightly buffered
    pass

# Configure PATH and library paths for Batch environment
# This allows pdf2image and pytesseract to find Poppler and Tesseract binaries
# Priority: System paths (/usr/local, /usr) > Docker container paths
if os.path.exists('/usr/local/bin'):
    os.environ['PATH'] = f"/usr/local/bin:{os.environ.get('PATH', '')}"
if os.path.exists('/usr/local/lib'):
    os.environ['LD_LIBRARY_PATH'] = f"/usr/local/lib:{os.environ.get('LD_LIBRARY_PATH', '')}"
if os.path.exists('/usr/bin'):
    os.environ['PATH'] = f"/usr/bin:{os.environ.get('PATH', '')}"

# Configure Tesseract path
if os.path.exists('/usr/local/bin/tesseract'):
    pytesseract.pytesseract.tesseract_cmd = '/usr/local/bin/tesseract'
elif os.path.exists('/usr/bin/tesseract'):
    pytesseract.pytesseract.tesseract_cmd = '/usr/bin/tesseract'

# Configure TESSDATA_PREFIX for Tesseract language data
if os.path.exists('/usr/local/share/tessdata'):
    os.environ['TESSDATA_PREFIX'] = '/usr/local/share/tessdata'
elif os.path.exists('/usr/share/tessdata'):
    os.environ['TESSDATA_PREFIX'] = '/usr/share/tessdata'

# AWS clients
s3_client = boto3.client('s3')
dynamodb_client = boto3.client('dynamodb')
sqs_client = boto3.client('sqs')
# Textract client for orientation detection
# Get region from environment or default to us-east-1
textract_region = os.environ.get('AWS_REGION', 'us-east-1')
textract_client = boto3.client('textract', region_name=textract_region)

# Environment variables (passed from Lambda via Batch job definition)
# CRITICAL: Check environment variables early and log if missing
# This prevents silent failures if env vars are not set
DESTINATION_BUCKET = os.environ.get('DESTINATION_BUCKET')
DYNAMODB_TABLE = os.environ.get('DYNAMODB_TABLE')
INVOICE_PROCESSING_QUEUE_URL = os.environ.get('INVOICE_PROCESSING_QUEUE_URL')

# Validate required environment variables
missing_vars = []
if not DESTINATION_BUCKET:
    missing_vars.append('DESTINATION_BUCKET')
if not DYNAMODB_TABLE:
    missing_vars.append('DYNAMODB_TABLE')
if not INVOICE_PROCESSING_QUEUE_URL:
    missing_vars.append('INVOICE_PROCESSING_QUEUE_URL')

if missing_vars:
    error_msg = f"CRITICAL: Missing required environment variables: {', '.join(missing_vars)}"
    logger.error(error_msg)
    print(error_msg, flush=True)  # Force print to stdout
    sys.exit(1)


# =========================================================================
# ========== PDF ROTATION & SKEW CORRECTION FUNCTIONS ====================
# =========================================================================

def detect_orientation_with_textract(pil_image):
    """
    Detects orientation by analyzing the Geometry of text lines from Textract.
    
    This bypasses the top-level 'OrientationCorrection' field which fails on mixed documents
    (e.g., pages with upright headers/stamps but sideways main content).
    
    Instead, we manually calculate angles from each text line's geometry and use voting
    to determine the dominant orientation, filtering out outliers like stamps or headers.
    
    FIX: Resizes images that exceed Textract limits:
    - Max dimension: 10,000px (we resize to 2048px for efficiency)
    - Max file size: 10 MB (we reduce quality if needed)
    
    Returns: (rotation_needed_cw, confidence_score, cost)
        - rotation_needed_cw: The clockwise rotation detected (0, 90, 180, 270)
        - confidence_score: Always 1.0 for Textract (highly reliable)
        - cost: Cost in USD for this Textract API call (per page)
    """
    try:
        # --- FIX: Resize Image if too large ---
        # Textract limit is 10,000px on longest side, but we don't need that much for orientation.
        # Downscale to max 2048px on longest side to save bandwidth and fix errors.
        # Orientation detection works fine at lower resolution.
        max_dimension = 2048
        original_size = pil_image.size
        
        if max(pil_image.size) > max_dimension:
            logger.info(f"   Resizing image from {original_size} to fit Textract limits (max {max_dimension}px)")
            # Create a copy to avoid modifying the original
            resized_image = pil_image.copy()
            resized_image.thumbnail((max_dimension, max_dimension), Image.Resampling.LANCZOS)
            pil_image = resized_image
            logger.info(f"   Resized to: {pil_image.size}")
        
        # 1. Convert PIL Image to Bytes for AWS Textract
        img_byte_arr = io.BytesIO()
        
        # AWS expects JPEG or PNG. Ensure we send a supported format.
        if pil_image.mode != 'RGB':
            pil_image = pil_image.convert('RGB')
        pil_image.save(img_byte_arr, format='JPEG', quality=95)
        img_bytes = img_byte_arr.getvalue()
        
        # --- FIX: Check Byte Size ---
        # Textract synchronous API has a 10 MB limit
        # If still > 10MB after resize (rare), reduce quality
        max_size_bytes = 10 * 1024 * 1024  # 10 MB
        if len(img_bytes) > max_size_bytes:
            logger.warning(f"   Image size ({len(img_bytes) / 1024 / 1024:.2f} MB) exceeds 10MB limit, reducing quality...")
            img_byte_arr = io.BytesIO()
            # Reduce quality progressively until under limit
            for quality in [85, 75, 65, 55]:
                pil_image.save(img_byte_arr, format='JPEG', quality=quality)
                img_bytes = img_byte_arr.getvalue()
                if len(img_bytes) <= max_size_bytes:
                    logger.info(f"   Reduced to {len(img_bytes) / 1024 / 1024:.2f} MB at quality {quality}")
                    break
                img_byte_arr.seek(0)
                img_byte_arr.truncate(0)
            
            # Final check - if still too large, resize more aggressively
            if len(img_bytes) > max_size_bytes:
                logger.warning(f"   Still too large after quality reduction, resizing more aggressively...")
                aggressive_max = 1500
                if max(pil_image.size) > aggressive_max:
                    resized_image = pil_image.copy()
                    resized_image.thumbnail((aggressive_max, aggressive_max), Image.Resampling.LANCZOS)
                    pil_image = resized_image
                    img_byte_arr = io.BytesIO()
                    pil_image.save(img_byte_arr, format='JPEG', quality=75)
                    img_bytes = img_byte_arr.getvalue()
                    logger.info(f"   Aggressively resized to: {pil_image.size}, size: {len(img_bytes) / 1024 / 1024:.2f} MB")
        
        # 2. Call AWS Textract API
        # We use 'detect_document_text' as it's the cheapest/fastest Sync API
        response = textract_client.detect_document_text(
            Document={'Bytes': img_bytes}
        )
        
        # --- NEW LOGIC: Manual Angle Calculation from Text Line Geometry ---
        # Instead of relying on OrientationCorrection (which fails on mixed documents),
        # we calculate the angle of each text line and vote for the dominant orientation.
        # This handles cases where headers/stamps are upright but main content is rotated.
        
        blocks = response.get('Blocks', [])
        lines = [b for b in blocks if b.get('BlockType') == 'LINE']
        
        if not lines:
            logger.warning("   No text lines found in Textract response, assuming upright")
            # Still charge for the API call even if no lines found
            cost = TEXTRACT_COST_PER_PAGE
            logger.info(f"   💰 Textract cost for this page: ${cost:.4f}")
            return (0, 1.0, cost)
        
        angles = []
        for line in lines:
            try:
                # Get coordinates of the start and end of the text line
                # Polygon points: 0=TopLeft, 1=TopRight, 2=BottomRight, 3=BottomLeft
                geometry = line.get('Geometry', {})
                poly = geometry.get('Polygon', [])
                
                if len(poly) < 2:
                    continue
                
                p0 = poly[0]  # TopLeft
                p1 = poly[1]  # TopRight
                
                # Calculate angle relative to horizontal (0 degrees)
                # Delta Y / Delta X
                dy = p1.get('Y', 0) - p0.get('Y', 0)
                dx = p1.get('X', 0) - p0.get('X', 0)
                
                # Skip if line is too short (likely noise)
                if abs(dx) < 0.001 and abs(dy) < 0.001:
                    continue
                
                # Calculate degrees (-180 to 180)
                angle_rad = math.atan2(dy, dx)
                angle_deg = math.degrees(angle_rad)
                
                # Normalize to 0, 90, 180, 270
                # A horizontal line (upright) has angle ~0
                # A line going down (rotated 90 CW) has angle ~90
                if -45 <= angle_deg <= 45:
                    snapped = 0
                elif 45 < angle_deg <= 135:
                    snapped = 90
                elif -135 <= angle_deg < -45:
                    snapped = 270  # (Same as -90)
                else:
                    snapped = 180
                
                angles.append(snapped)
            except (KeyError, IndexError, TypeError) as e:
                # Skip lines with invalid geometry
                continue
        
        if not angles:
            logger.warning("   No valid text line angles calculated, assuming upright")
            cost = TEXTRACT_COST_PER_PAGE
            logger.info(f"   💰 Textract cost for this page: ${cost:.4f}")
            return (0, 1.0, cost)
        
        # 3. Vote for the dominant angle
        # We count which angle appears most frequently
        counts = Counter(angles)
        dominant_angle, count = counts.most_common(1)[0]
        total_lines = len(angles)
        dominance_ratio = count / total_lines
        
        logger.info(f"   Textract Line Analysis: {dict(counts)} (Total lines: {total_lines}, Dominant: {dominant_angle}° with {dominance_ratio:.2%} confidence)")
        
        # Only rotate if a significant portion of the page is rotated (e.g. >40%)
        # This filters out small stamps or headers that might be upright on a rotated page
        cost = TEXTRACT_COST_PER_PAGE
        logger.info(f"   💰 Textract cost for this page: ${cost:.4f}")
        
        if dominance_ratio > 0.4:
            if dominant_angle != 0:
                logger.info(f"   ✅ Detected rotation via line geometry: {dominant_angle}° ({dominance_ratio:.2%} of lines)")
                print(f"   ✅ AWS Textract detected rotation: {dominant_angle}° (via line geometry)", flush=True)
                return (dominant_angle, 1.0, cost)
        
        logger.info(f"   Page determined to be Upright (0°) based on line geometry (dominant angle: {dominant_angle}° but only {dominance_ratio:.2%} of lines)")
        print(f"   AWS Textract detected no rotation (0°)", flush=True)
        return (0, 1.0, cost)
            
    except Exception as e:
        logger.warning(f"   ❌ AWS Textract Error: {str(e)}")
        print(f"   ❌ AWS Textract Error: {str(e)}", flush=True)
        # Fallback to 0 if API fails - no cost if API call failed
        return (0, 0.0, 0.0)

def detect_and_correct_skew_from_image(pil_image):
    """
    Detects and corrects small-angle skew.
    Safety mechanisms added to prevent aligning to invoice borders.
    """
    try:
        # Convert to grayscale
        img_np = np.array(pil_image)
        if len(img_np.shape) == 3:
            gray = cv2.cvtColor(img_np, cv2.COLOR_RGB2GRAY)
        else:
            gray = img_np

        # 1. Inverse thresholding to get white text on black background
        # using simple binary thresholding to avoid noise
        thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]

        # 2. DILATION: Connect text characters into lines. 
        # This is CRITICAL for invoices. We want to detect *text lines*, not just individual pixels.
        # A horizontal kernel emphasizes horizontal text flow.
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (20, 1))
        dilated = cv2.dilate(thresh, kernel, iterations=1)

        # 3. Find contours of the text lines
        contours, _ = cv2.findContours(dilated, cv2.RETR_LIST, cv2.CHAIN_APPROX_SIMPLE)
        
        angles = []
        for cnt in contours:
            # Only look at contours that are wide enough to be text lines
            if cv2.contourArea(cnt) < 1000: 
                continue
                
            # MinAreaRect returns ((center_x, center_y), (width, height), angle)
            rect = cv2.minAreaRect(cnt)
            width = rect[1][0]
            height = rect[1][1]
            angle = rect[2]

            # Handle OpenCV angle quirks (varies by version, usually -90 to 0 or 0 to 90)
            if width < height:
                angle = 90 + angle
            
            # We only care about essentially horizontal lines.
            # If an angle is steep (e.g., > 5 degrees), it's likely a vertical line 
            # or graphic, so we ignore it.
            if abs(angle) < 5.0:
                angles.append(angle)

        if not angles:
            # No reliable text lines found
            return pil_image

        # Get the median angle of all text lines
        median_angle = np.median(angles)
        
        # Strict Safety Check: Only correct if skew is very small but noticeable.
        # If skew is > 2 degrees, it might be a graphic or misinterpretation.
        # If skew is < 0.1, it's straight enough.
        if 0.1 < abs(median_angle) < 2.0:
            logger.info(f"Correcting skew: {median_angle:.2f} degrees")
            corrected_pil = pil_image.rotate(
                -median_angle, 
                resample=Image.BICUBIC, 
                expand=True, 
                fillcolor='white'
            )
            return corrected_pil
        else:
            return pil_image
        
    except Exception as e:
        logger.error(f"Skew detection failed: {str(e)}")
        return pil_image

def rotate_pdf_optimized(pdf_file_path, output_pdf_path):
    """
    Optimized PDF rotation logic with memory-efficient processing.
    Uses disk-based image storage to avoid loading all pages into RAM at once.
    This prevents OOM (Out of Memory) errors for large PDFs (100+ pages).
    
    INCLUDES FIX: Resizes and compresses images to reduce output PDF size.
    - Resizes images to max 2500px before saving (prevents 150MP monster files)
    - Applies JPEG compression (quality=75) to reduce file size
    """
    corrected_images = []
    page_rotation_summary = []  # Track rotation info for each page
    total_textract_cost = 0.0  # Track total Textract cost for this PDF
    try:
        logger.info(f"Starting optimized rotation for: {pdf_file_path}")
        
        # Create temporary directory for page images to avoid RAM spikes
        # This is critical for large PDFs (100+ pages) to prevent OOM
        with tempfile.TemporaryDirectory() as temp_dir:
            try:
                # Convert PDF to images, saving to disk instead of loading all into RAM
                # fmt='jpeg' reduces disk usage compared to default ppm
                # paths_only=True returns file paths instead of Image objects (memory efficient)
                logger.info(f"Converting PDF to images in temporary directory...")
                image_paths = convert_from_path(
                    pdf_file_path, 
                    dpi=300,
                    output_folder=temp_dir,
                    fmt='jpeg',
                    paths_only=True  # Returns paths instead of Image objects (memory efficient)
                )
                logger.info(f"Converted {len(image_paths)} pages to disk")
            except Exception as e:
                logger.error(f"Failed to convert PDF to images: {str(e)}")
                return False
            
            # Process each page one at a time (load from disk, process, keep in memory)
            # This prevents loading all pages into RAM simultaneously
            for i, image_path in enumerate(image_paths):
                page_num = i + 1
                
                # Load image from disk only when needed
                try:
                    with Image.open(image_path) as pil_image:
                        # Ensure RGB mode for Textract (requires RGB format)
                        if pil_image.mode != 'RGB':
                            pil_image = pil_image.convert('RGB')
                        
                        # 1. Detect Orientation using AWS Textract
                        logger.info(f"Page {page_num}: Analyzing orientation with AWS Textract...")
                        detected_rotation, confidence, page_cost = detect_orientation_with_textract(pil_image)
                        total_textract_cost += page_cost
                        
                        logger.info(f"Page {page_num}: Detected rotation needed: {detected_rotation}° (Confidence: {confidence:.2f})")
                        logger.info(f"Page {page_num}: Textract cost: ${page_cost:.4f} | Running total for PDF: ${total_textract_cost:.4f}")
                        print(f"Page {page_num}: Detected rotation needed: {detected_rotation}° (Confidence: {confidence:.2f})", flush=True)
                        print(f"Page {page_num}: Textract cost: ${page_cost:.4f} | Running total: ${total_textract_cost:.4f}", flush=True)
                        # Flush every 5 pages to prevent log buffer issues
                        if page_num % 5 == 0:
                            sys.stdout.flush()
                            sys.stderr.flush()

                        # 2. Apply Rotation only if we detected a rotation and confidence is reasonable
                        # Textract confidence is always 1.0 (highly reliable), so we use > 0.3 as threshold
                        # This is more conservative to avoid false positives
                        final_image = pil_image.copy()  # Create a copy since we're using 'with' context
                        if detected_rotation != 0 and confidence > 0.3:
                            # ROTATION LOGIC EXPLANATION:
                            # Textract's OrientationCorrection reports the CURRENT clockwise rotation of the page.
                            # Example: ROTATE_90 means the page is currently rotated 90° clockwise (top is at RIGHT).
                            # To correct it, we need to rotate 90° counter-clockwise.
                            # PIL's rotate() with positive values rotates counter-clockwise.
                            # Therefore: rotate(detected_rotation) correctly fixes the rotation.
                            # 
                            # Verification:
                            # - Textract says ROTATE_90 (page is 90° CW) → detected_rotation = 90
                            # - We apply rotate(90) which is 90° CCW
                            # - Result: Page becomes upright (0°) ✅
                            rotation_to_apply = detected_rotation
                            logger.info(f"Page {page_num}: Applying {rotation_to_apply}° CCW rotation (to correct {detected_rotation}° CW rotation detected by Textract, confidence: {confidence:.2f}).")
                            print(f"Page {page_num}: Applying {rotation_to_apply}° CCW rotation (to correct {detected_rotation}° CW rotation)", flush=True)
                            final_image = pil_image.rotate(rotation_to_apply, expand=True)
                        elif detected_rotation != 0:
                            logger.warning(f"Page {page_num}: Detected {detected_rotation}° but confidence {confidence:.2f} is too low. Skipping rotation to avoid false positives.")
                            print(f"Page {page_num}: WARNING - Detected {detected_rotation}° but confidence {confidence:.2f} is too low. Skipping rotation.", flush=True)
                        else:
                            logger.info(f"Page {page_num}: No rotation needed (detected angle: 0°)")
                            print(f"Page {page_num}: No rotation needed (detected angle: 0°)", flush=True)
                        
                        # Track rotation info for summary
                        page_rotation_summary.append({
                            'page': page_num,
                            'detected_angle': detected_rotation,
                            'applied_rotation': detected_rotation if detected_rotation != 0 and confidence > 0.3 else 0,
                            'confidence': confidence
                        })

                        # 3. Skew Correction - DISABLED
                        # Skew correction has been disabled to focus on rotation only.
                        # Small-angle skew (< 2°) is less common and can cause false positives
                        # by aligning to invoice borders or graphics instead of text.
                        # Uncomment the line below if you need skew correction:
                        # final_image = detect_and_correct_skew_from_image(final_image)
                        
                        # 4. --- SIZE FIX: Resize image before saving to PDF ---
                        # 150MP is overkill. 2500px is plenty for invoice processing (approx 200-300 DPI for A4).
                        # This prevents output PDF from being 50+ MB due to massive rasterized images.
                        max_save_dimension = 2500
                        original_save_size = final_image.size
                        if max(final_image.size) > max_save_dimension:
                            logger.info(f"Page {page_num}: Resizing from {original_save_size} to max {max_save_dimension}px before saving")
                            # Use LANCZOS for sharp text downscaling
                            final_image.thumbnail((max_save_dimension, max_save_dimension), Image.Resampling.LANCZOS)
                            logger.info(f"Page {page_num}: Resized to {final_image.size} for PDF output")
                        
                        # Ensure RGB mode (removes alpha channel if present, reduces size)
                        if final_image.mode != 'RGB':
                            final_image = final_image.convert('RGB')
                        
                        # Store processed image (we need all pages in memory to save final PDF)
                        # For very large PDFs (>50 pages), consider incremental PDF building
                        corrected_images.append(final_image)
                        
                except Exception as e:
                    logger.error(f"Failed to process page {page_num}: {str(e)}")
                    # Continue with other pages even if one fails
                    # You might want to add the original page or skip it
                    continue

        if not corrected_images:
            return False
        
        # Log rotation summary
        logger.info(f"📊 Rotation Summary for {os.path.basename(pdf_file_path)}:")
        print(f"📊 Rotation Summary for {os.path.basename(pdf_file_path)}:", flush=True)
        for page_info in page_rotation_summary:
            if page_info['applied_rotation'] != 0:
                logger.info(f"   Page {page_info['page']}: Detected {page_info['detected_angle']}° → Applied {page_info['applied_rotation']}° rotation")
                print(f"   Page {page_info['page']}: Detected {page_info['detected_angle']}° → Applied {page_info['applied_rotation']}° rotation", flush=True)
            else:
                logger.info(f"   Page {page_info['page']}: No rotation (detected: {page_info['detected_angle']}°)")
                print(f"   Page {page_info['page']}: No rotation (detected: {page_info['detected_angle']}°)", flush=True)
        
        # Log Textract cost summary for this PDF
        total_pages = len(page_rotation_summary)
        logger.info(f"💰 Textract Cost Summary for {os.path.basename(pdf_file_path)}:")
        logger.info(f"   • Total pages processed: {total_pages}")
        logger.info(f"   • Cost per page: ${TEXTRACT_COST_PER_PAGE:.4f}")
        logger.info(f"   • Total Textract cost: ${total_textract_cost:.4f}")
        print(f"💰 Textract Cost Summary for {os.path.basename(pdf_file_path)}:", flush=True)
        print(f"   • Total pages processed: {total_pages}", flush=True)
        print(f"   • Cost per page: ${TEXTRACT_COST_PER_PAGE:.4f}", flush=True)
        print(f"   • Total Textract cost: ${total_textract_cost:.4f}", flush=True)
            
        logger.info(f"Saving corrected PDF to: {output_pdf_path}")
        
        # --- COMPRESSION FIX: Save with JPEG compression to drastically reduce file size ---
        # resolution=150.0: Set metadata resolution (lower than 300 DPI but still good quality)
        # quality=75: JPEG Quality (75 is good balance - standard is often higher, but 75 reduces size significantly)
        # optimize=True: Enable PIL optimization for smaller file size
        corrected_images[0].save(
            output_pdf_path,
            "PDF",
            save_all=True,
            append_images=corrected_images[1:],
            resolution=150.0,  # Set metadata resolution (lower than 300 DPI but still good quality)
            quality=75,        # JPEG Quality (75 is good balance - reduces size significantly)
            optimize=True      # Enable PIL optimization for smaller file size
        )
        
        # Log file size for verification
        try:
            file_size = os.path.getsize(output_pdf_path)
            logger.info(f"✅ Saved corrected PDF: {file_size:,} bytes ({file_size / 1024 / 1024:.2f} MB)")
        except Exception as e:
            logger.warning(f"Could not get file size: {str(e)}")
        return True, total_textract_cost
        
    except Exception as e:
        logger.error(f"Processing failed: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False, 0.0

def preprocess_pdf_rotation(s3_client, bucket, key, email_id):
    """
    Download PDF from S3, detect/correct rotation, and re-upload to S3.
    Returns the new S3 key if correction was applied, otherwise returns original key.
    
    Note: Skew correction has been disabled - only rotation (0°, 90°, 180°, 270°) is corrected.
    """
    try:
        logger.info(f"Starting optimized PDF rotation preprocessing for {key}")

        with tempfile.TemporaryDirectory() as tmpdir:
            local_pdf_path = os.path.join(tmpdir, 'original.pdf')
            s3_client.download_file(bucket, key, local_pdf_path)
            logger.info(f"Downloaded PDF from s3://{bucket}/{key}")
            
            # Path for the new, corrected PDF
            corrected_pdf_path = os.path.join(tmpdir, 'corrected.pdf')
            
            # Run the optimized rotation logic (skew correction disabled)
            success, pdf_textract_cost = rotate_pdf_optimized(
                local_pdf_path,
                corrected_pdf_path
            )
            
            if not success:
                logger.error("Failed to process PDF with optimized logic, using original.")
                return key, 0.0
            
            # Log PDF-level Textract cost
            logger.info(f"💰 Total Textract cost for PDF '{os.path.basename(key)}': ${pdf_textract_cost:.4f}")
            print(f"💰 Total Textract cost for PDF '{os.path.basename(key)}': ${pdf_textract_cost:.4f}", flush=True)

            # Check if the new file is different from the old (e.g., if no rotation was needed)
            # This is a simple check; a more robust one would compare content/hash
            # For now, we'll assume if the function succeeded, we upload the new file
            # as it's now a "clean" image-based PDF at 300 DPI, which is better for Textract.
            logger.info("Rotation correction applied. Uploading new PDF.")

            # Generate new S3 key for corrected file
            key_parts = key.rsplit('/', 1)
            directory = key_parts[0] if len(key_parts) > 1 else ''
            filename = key_parts[-1]
            
            filename_parts = filename.rsplit('.', 1)
            base_name = filename_parts[0]
            extension = f".{filename_parts[1]}" if len(filename_parts) > 1 else ""

            # Add "corrected" suffix
            new_filename = f"{base_name}_corrected{extension}"
            new_key = f"{directory}/{new_filename}" if directory else new_filename

            # Upload rotated PDF to S3
            logger.info(f"Uploading corrected PDF to s3://{bucket}/{new_key}...")
            s3_client.upload_file(corrected_pdf_path, bucket, new_key)
            
            # Verify the file was uploaded successfully
            try:
                s3_client.head_object(Bucket=bucket, Key=new_key)
                file_size = s3_client.head_object(Bucket=bucket, Key=new_key)['ContentLength']
                logger.info(f"✅ Verified: Corrected PDF uploaded successfully to s3://{bucket}/{new_key}")
                logger.info(f"   File size: {file_size:,} bytes ({file_size / 1024 / 1024:.2f} MB)")
            except Exception as verify_error:
                logger.error(f"⚠️ WARNING: Could not verify uploaded file: {str(verify_error)}")
            
            return new_key, pdf_textract_cost

    except Exception as e:
        logger.error(f"PDF rotation preprocessing failed: {str(e)}")
        return key, 0.0  # Return original key if preprocessing fails


# =========================================================================
# ========== PDF PROCESSING & QUEUEING FUNCTIONS ==========================
# =========================================================================

def process_and_queue_single_pdf(pdf_data):
    """
    Process a single PDF: rotate it, then queue to SQS.
    
    Args:
        pdf_data: dict with keys:
            - 's3_path': Full S3 path to PDF
            - 'filename': Original filename
            - 'email_id': Email job ID
            - 'email_details': Email metadata for SQS message
            - 'status_email_*': Status email details
    
    Returns:
        tuple: (attachment_id, error_message or None, processing_time_seconds, worker_id)
    """
    attachment_id = None
    worker_id = threading.current_thread().name
    start_time = time.time()
    try:
        filename = pdf_data['filename']
        email_id = pdf_data['email_id']
        s3_path = pdf_data['s3_path']
        
        logger.info(f"🔄 Processing PDF rotation for: {filename}")
        
        # Extract S3 key from s3_path
        pdf_key = s3_path.replace(f"s3://{DESTINATION_BUCKET}/", "")
        
        # Step 1: Create attachment entry in DynamoDB
        attachment_id = str(uuid.uuid4())
        create_attachment_entry(
            dynamodb_client,
            DYNAMODB_TABLE,
            email_id,
            attachment_id,
            filename,
            'PDF',
            s3_path,  # Original path, will update after rotation
            'Invoice'
        )
        logger.info(f"✅ Created attachment entry: {attachment_id}")
        
        # Step 2: Preprocess PDF rotation (download, rotate, upload corrected version)
        logger.info(f"🔄 Starting PDF rotation for: {filename}")
        corrected_key, pdf_textract_cost = preprocess_pdf_rotation(
            s3_client,
            DESTINATION_BUCKET,
            pdf_key,
            email_id
        )
        
        # Step 3: Update S3 path if rotation was applied
        if corrected_key != pdf_key:
            corrected_s3_path = f"s3://{DESTINATION_BUCKET}/{corrected_key}"
            logger.info(f"✅ PDF rotated. New S3 path: {corrected_s3_path}")
            rotation_applied = True
        else:
            corrected_s3_path = s3_path
            logger.info(f"ℹ️ No rotation needed for: {filename}")
            logger.info(f"   Using original S3 path: {corrected_s3_path}")
            rotation_applied = False
        
        # Step 4: Prepare SQS message for invoice processor
        input_key = corrected_s3_path.replace(f"s3://{DESTINATION_BUCKET}/", "")
        invoice_processor_input = {
            'input_bucket': DESTINATION_BUCKET,
            'input_key': input_key,
            'output_bucket': DESTINATION_BUCKET,
            'output_prefix': 'invoice/output/',
            'email_details': pdf_data['email_details'],
            'job_id': email_id,
            'attachment_id': attachment_id
        }
        
        # Step 5: Update attachment status to QUEUED
        update_attachment_status(
            dynamodb_client,
            DYNAMODB_TABLE,
            email_id,
            attachment_id,
            'QUEUED'
        )
        
        # Step 6: Send message to SQS queue
        sqs_client.send_message(
            QueueUrl=INVOICE_PROCESSING_QUEUE_URL,
            MessageBody=json.dumps(invoice_processor_input)
        )
        
        processing_time = time.time() - start_time
        logger.info(f"✅ Successfully processed and queued: {filename}")
        logger.info(f"   📍 Final S3 path: {corrected_s3_path}")
        logger.info(f"   🆔 Attachment ID: {attachment_id}")
        logger.info(f"   ⏱️  Processing time: {processing_time:.2f}s")
        logger.info(f"   👷 Worker: {worker_id}")
        logger.info(f"   🔄 Rotation applied: {'Yes' if rotation_applied else 'No'}")
        logger.info(f"   💰 Textract cost: ${pdf_textract_cost:.4f}")
        # Also print to stdout to ensure visibility in CloudWatch
        print(f"✅ Successfully processed and queued: {filename}", flush=True)
        print(f"   📍 Final S3 path: {corrected_s3_path}", flush=True)
        print(f"   🆔 Attachment ID: {attachment_id}", flush=True)
        print(f"   ⏱️  Processing time: {processing_time:.2f}s", flush=True)
        print(f"   👷 Worker: {worker_id}", flush=True)
        print(f"   🔄 Rotation applied: {'Yes' if rotation_applied else 'No'}", flush=True)
        print(f"   💰 Textract cost: ${pdf_textract_cost:.4f}", flush=True)
        return attachment_id, None, processing_time, worker_id, corrected_s3_path, pdf_textract_cost
        
    except Exception as e:
        processing_time = time.time() - start_time
        logger.error(f"❌ Failed to process PDF {pdf_data.get('filename', 'unknown')}: {str(e)}")
        logger.error(f"   ⏱️  Processing time: {processing_time:.2f}s")
        logger.error(f"   👷 Worker: {worker_id}")
        import traceback
        logger.error(traceback.format_exc())
        
        # Update attachment status to FAILED if attachment_id was created
        if attachment_id:
            try:
                update_attachment_status(
                    dynamodb_client,
                    DYNAMODB_TABLE,
                    pdf_data['email_id'],
                    attachment_id,
                    'FAILED',
                    error={
                        'message': str(e),
                        'error_code': 'BATCH_PDF_PROCESSING_FAILED'
                    }
                )
            except Exception as update_error:
                logger.error(f"Failed to update attachment status: {str(update_error)}")
        
        return attachment_id, str(e), processing_time, worker_id, None, 0.0


def main():
    """
    Main entry point for Batch job.
    Reads PDF metadata from environment variables and processes all PDFs in parallel.
    """
    # CRITICAL: Log startup immediately to verify script is running
    job_start_time = time.time()
    logger.info("=" * 80)
    logger.info("BATCH JOB STARTED - batch_pdf_processor.py")
    logger.info("=" * 80)
    print("BATCH JOB STARTED", flush=True)  # Force print to stdout
    
    try:
        # Read EMAIL_ID from environment variable (set by Lambda when submitting Batch job)
        email_id = os.environ.get('EMAIL_ID')
        if not email_id:
            error_msg = "CRITICAL: EMAIL_ID environment variable not set"
            logger.error(error_msg)
            print(error_msg, flush=True)
            raise ValueError(error_msg)
        
        logger.info(f"📧 Processing PDFs for email_id: {email_id}")
        
        # Fetch PDF list from DynamoDB
        pdf_list_key = f"EMAIL#{email_id}#PDF_LIST"
        try:
            response = dynamodb_client.get_item(
                TableName=DYNAMODB_TABLE,
                Key={
                    'pk': {'S': pdf_list_key},
                    'sk': {'S': 'PDF_LIST'}
                }
            )
            if 'Item' not in response or 'pdf_list' not in response['Item']:
                error_msg = f"PDF list not found in DynamoDB for email_id: {email_id}"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            pdf_list = json.loads(response['Item']['pdf_list']['S'])
            logger.info(f"✅ Retrieved PDF list from DynamoDB: {len(pdf_list)} PDF(s)")
        except Exception as e:
            error_msg = f"Failed to fetch PDF list from DynamoDB: {str(e)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Fetch email details from DynamoDB
        email_details_key = f"EMAIL#{email_id}#EMAIL_DETAILS"
        email_details = None
        try:
            response = dynamodb_client.get_item(
                TableName=DYNAMODB_TABLE,
                Key={
                    'pk': {'S': email_details_key},
                    'sk': {'S': 'EMAIL_DETAILS'}
                }
            )
            if 'Item' in response and 'email_details' in response['Item']:
                email_details = json.loads(response['Item']['email_details']['S'])
                logger.info(f"✅ Retrieved email details from DynamoDB")
            else:
                logger.warning(f"⚠️ Email details not found in DynamoDB, using minimal data")
                # Fallback: Create minimal email_details
                email_details = {
                    'to': '',
                    'subject': '',
                    'message_id': '',
                    'original_body': '',
                    'status_message_id': None,
                    'status_email_body': None,
                    'status_email_subject': None,
                    'status_email_sender': None,
                    'status_email_date': None
                }
        except Exception as e:
            logger.warning(f"Failed to fetch email details from DynamoDB: {str(e)}, using minimal data")
            email_details = {
                'to': '',
                'subject': '',
                'message_id': '',
                'original_body': '',
                'status_message_id': None,
                'status_email_body': None,
                'status_email_subject': None,
                'status_email_sender': None,
                'status_email_date': None
            }
        
        # Build pdf_attachments array with email_details
        # Note: Each PDF needs its own filename in email_details for the invoice processor
        pdf_attachments = []
        for pdf_item in pdf_list:
            # Create a copy of email_details and add the filename for this specific PDF
            pdf_email_details = email_details.copy()
            pdf_email_details['filename'] = pdf_item['filename']
            pdf_attachments.append({
                's3_path': pdf_item['s3_path'],
                'filename': pdf_item['filename'],
                'email_id': email_id,
                'email_details': pdf_email_details  # Add email_details with filename to each PDF
            })
        
        logger.info(f"📦 Prepared {len(pdf_attachments)} PDF(s) for processing")
        
        if not pdf_attachments:
            warning_msg = "WARNING: No PDFs to process, exiting"
            logger.warning(warning_msg)
            print(warning_msg, flush=True)
            logger.info("=" * 80)
            logger.info("BATCH JOB COMPLETED - No PDFs to process")
            logger.info("=" * 80)
            return
        
        # AWS Textract is used for orientation detection - no model preloading needed
        logger.info("")
        logger.info("🔧 Using AWS Textract for orientation detection (no model preloading required)")
        
        # Process all PDFs in parallel
        # Use ThreadPoolExecutor to process multiple PDFs simultaneously
        # With Fargate (4 vCPU, 30GB memory), we can safely process 4-8 PDFs in parallel
        # Adjust based on your Fargate configuration (4 vCPU = max 8 workers recommended)
        max_workers = min(8, len(pdf_attachments))  # Process up to 8 PDFs in parallel (Fargate optimized)
        
        processing_start_time = time.time()
        logger.info(f"🚀 Starting parallel processing with {max_workers} workers")
        logger.info(f"⏱️  Processing start time: {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(processing_start_time))}")
        
        # Track worker statistics
        worker_stats = {}  # {worker_id: {'count': int, 'pdfs': list, 'total_time': float, 'avg_time': float}}
        results = []
        
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="Worker") as executor:
            # Submit all PDF processing tasks
            future_to_pdf = {
                executor.submit(process_and_queue_single_pdf, pdf_data): pdf_data
                for pdf_data in pdf_attachments
            }
            
            # Collect results as they complete
            success_count = 0
            failed_count = 0
            total_textract_cost_all_pdfs = 0.0  # Track total cost across all PDFs
            
            for future in as_completed(future_to_pdf):
                pdf_data = future_to_pdf[future]
                result_tuple = future.result()
                # Handle result tuple formats: (4 items) or (5 items with S3 path) or (6 items with cost)
                if len(result_tuple) == 6:
                    attachment_id, error, processing_time, worker_id, corrected_s3_path, pdf_textract_cost = result_tuple
                elif len(result_tuple) == 5:
                    attachment_id, error, processing_time, worker_id, corrected_s3_path = result_tuple
                    pdf_textract_cost = 0.0
                else:
                    attachment_id, error, processing_time, worker_id = result_tuple
                    corrected_s3_path = None
                    pdf_textract_cost = 0.0
                
                # Accumulate total cost
                total_textract_cost_all_pdfs += pdf_textract_cost
                
                # Update worker statistics
                if worker_id not in worker_stats:
                    worker_stats[worker_id] = {
                        'count': 0,
                        'pdfs': [],
                        'total_time': 0.0,
                        'success_count': 0,
                        'failed_count': 0
                    }
                
                worker_stats[worker_id]['count'] += 1
                worker_stats[worker_id]['total_time'] += processing_time
                worker_stats[worker_id]['pdfs'].append({
                    'filename': pdf_data['filename'],
                    'time': processing_time,
                    'status': 'success' if not error else 'failed',
                    's3_path': corrected_s3_path,
                    'textract_cost': pdf_textract_cost
                })
                
                if error:
                    failed_count += 1
                    worker_stats[worker_id]['failed_count'] += 1
                    logger.warning(f"❌ Failed: {pdf_data['filename']} (worker: {worker_id}, time: {processing_time:.2f}s)")
                else:
                    success_count += 1
                    worker_stats[worker_id]['success_count'] += 1
                    logger.info(f"✅ Completed: {pdf_data['filename']} ({success_count}/{len(pdf_attachments)}, worker: {worker_id}, time: {processing_time:.2f}s)")
                
                results.append({
                    'filename': pdf_data['filename'],
                    'attachment_id': attachment_id,
                    'error': error,
                    'processing_time': processing_time,
                    'worker_id': worker_id,
                    'corrected_s3_path': corrected_s3_path,
                    'textract_cost': pdf_textract_cost
                })
        
        processing_end_time = time.time()
        total_processing_time = processing_end_time - processing_start_time
        total_job_time = processing_end_time - job_start_time
        
        # Calculate worker averages
        for worker_id, stats in worker_stats.items():
            if stats['count'] > 0:
                stats['avg_time'] = stats['total_time'] / stats['count']
        
        # Force flush all pending logs before summary (CRITICAL for CloudWatch)
        sys.stdout.flush()
        sys.stderr.flush()
        # Additional flush to ensure logs are sent (os is already imported at top)
        try:
            if hasattr(sys.stdout, 'fileno'):
                os.fsync(sys.stdout.fileno())
            if hasattr(sys.stderr, 'fileno'):
                os.fsync(sys.stderr.fileno())
        except (OSError, AttributeError):
            pass  # Ignore if fsync fails or not available
        
        # ========== DETAILED SUMMARY LOG ==========
        # CRITICAL: Force flush to ensure summary appears in CloudWatch
        # CloudWatch has limits on log size, so we limit detailed output
        sys.stdout.flush()
        sys.stderr.flush()
        print("=" * 80, flush=True)
        print("📊 BATCH JOB PROCESSING SUMMARY", flush=True)
        print("=" * 80, flush=True)
        logger.info("=" * 80)
        logger.info("📊 BATCH JOB PROCESSING SUMMARY")
        logger.info("=" * 80)
        sys.stdout.flush()  # Extra flush after header
        
        # Overall statistics
        logger.info(f"📦 Total PDFs processed: {len(pdf_attachments)}")
        logger.info(f"✅ Successful: {success_count}")
        logger.info(f"❌ Failed: {failed_count}")
        logger.info(f"")
        print(f"📦 Total PDFs processed: {len(pdf_attachments)}", flush=True)
        print(f"✅ Successful: {success_count}", flush=True)
        print(f"❌ Failed: {failed_count}", flush=True)
        print("", flush=True)
        
        # Timing information
        logger.info(f"⏱️  TIMING INFORMATION:")
        logger.info(f"   • Total job time: {total_job_time:.2f} seconds ({total_job_time/60:.2f} minutes)")
        logger.info(f"   • PDF processing time: {total_processing_time:.2f} seconds ({total_processing_time/60:.2f} minutes)")
        logger.info(f"   • Average time per PDF: {total_processing_time/len(pdf_attachments):.2f} seconds")
        if success_count > 0:
            avg_success_time = sum(r['processing_time'] for r in results if not r['error']) / success_count
            logger.info(f"   • Average time per successful PDF: {avg_success_time:.2f} seconds")
        logger.info(f"   • Processing start: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(processing_start_time))}")
        logger.info(f"   • Processing end: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(processing_end_time))}")
        logger.info(f"")
        print(f"⏱️  TIMING INFORMATION:", flush=True)
        print(f"   • Total job time: {total_job_time:.2f} seconds ({total_job_time/60:.2f} minutes)", flush=True)
        print(f"   • PDF processing time: {total_processing_time:.2f} seconds ({total_processing_time/60:.2f} minutes)", flush=True)
        print(f"   • Average time per PDF: {total_processing_time/len(pdf_attachments):.2f} seconds", flush=True)
        if success_count > 0:
            avg_success_time = sum(r['processing_time'] for r in results if not r['error']) / success_count
            print(f"   • Average time per successful PDF: {avg_success_time:.2f} seconds", flush=True)
        print(f"   • Processing start: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(processing_start_time))}", flush=True)
        print(f"   • Processing end: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(processing_end_time))}", flush=True)
        print("", flush=True)
        
        # Worker details
        logger.info(f"👷 WORKER DETAILS:")
        logger.info(f"   • Total workers used: {len(worker_stats)}")
        logger.info(f"   • Max workers configured: {max_workers}")
        logger.info(f"")
        
        # Per-worker statistics (limit per-worker PDF list to avoid truncation)
        for worker_id in sorted(worker_stats.keys()):
            stats = worker_stats[worker_id]
            logger.info(f"   🔧 {worker_id}:")
            logger.info(f"      • PDFs handled: {stats['count']}")
            logger.info(f"      • Successful: {stats['success_count']}")
            logger.info(f"      • Failed: {stats['failed_count']}")
            logger.info(f"      • Total processing time: {stats['total_time']:.2f} seconds")
            logger.info(f"      • Average time per PDF: {stats['avg_time']:.2f} seconds")
            worker_total_cost = sum(pdf_info.get('textract_cost', 0.0) for pdf_info in stats['pdfs'])
            logger.info(f"      • Total Textract cost: ${worker_total_cost:.4f}")
            logger.info(f"      • PDFs processed:")
            # Limit to first 10 PDFs per worker to avoid log truncation
            pdfs_to_show = stats['pdfs'][:10] if len(stats['pdfs']) > 10 else stats['pdfs']
            for pdf_info in pdfs_to_show:
                status_icon = "✅" if pdf_info['status'] == 'success' else "❌"
                s3_path_info = f" | S3: {pdf_info.get('s3_path', 'N/A')}" if pdf_info.get('s3_path') else ""
                cost_info = f" | Cost: ${pdf_info.get('textract_cost', 0.0):.4f}" if pdf_info.get('textract_cost', 0.0) > 0 else ""
                logger.info(f"         {status_icon} {pdf_info['filename']} ({pdf_info['time']:.2f}s{s3_path_info}{cost_info})")
            if len(stats['pdfs']) > 10:
                logger.info(f"         ... ({len(stats['pdfs']) - 10} more PDFs for this worker)")
            logger.info(f"")
            sys.stdout.flush()  # Flush after each worker
        
        # Detailed PDF-by-PDF summary with S3 paths (limit to avoid truncation)
        logger.info(f"📄 DETAILED PDF PROCESSING RESULTS:")
        logger.info(f"")
        print(f"📄 DETAILED PDF PROCESSING RESULTS:", flush=True)
        print("", flush=True)
        # Limit detailed results to first 20 PDFs to prevent log truncation
        results_to_detail = results[:20] if len(results) > 20 else results
        for result in results_to_detail:
            status_icon = "✅" if not result['error'] else "❌"
            logger.info(f"   {status_icon} {result['filename']}:")
            logger.info(f"      • Attachment ID: {result['attachment_id']}")
            logger.info(f"      • Worker: {result['worker_id']}")
            logger.info(f"      • Processing time: {result['processing_time']:.2f}s")
            if result.get('textract_cost', 0.0) > 0:
                logger.info(f"      • Textract cost: ${result['textract_cost']:.4f}")
            if result.get('corrected_s3_path'):
                logger.info(f"      • Corrected S3 path: {result['corrected_s3_path']}")
                print(f"   {status_icon} {result['filename']}:", flush=True)
                print(f"      • Corrected S3 path: {result['corrected_s3_path']}", flush=True)
                if result.get('textract_cost', 0.0) > 0:
                    print(f"      • Textract cost: ${result['textract_cost']:.4f}", flush=True)
            if result['error']:
                logger.info(f"      • Error: {result['error']}")
            logger.info(f"")
        if len(results) > 20:
            logger.info(f"   ... ({len(results) - 20} more PDFs - see worker details above for full list)")
            print(f"   ... ({len(results) - 20} more PDFs - see worker details above for full list)", flush=True)
        sys.stdout.flush()  # Flush after detailed results
        
        # Performance metrics
        logger.info(f"📈 PERFORMANCE METRICS:")
        if total_processing_time > 0:
            throughput = len(pdf_attachments) / total_processing_time
            logger.info(f"   • Throughput: {throughput:.2f} PDFs/second")
        if success_count > 0:
            success_rate = (success_count / len(pdf_attachments)) * 100
            logger.info(f"   • Success rate: {success_rate:.2f}%")
        
        # Find fastest and slowest PDFs
        if results:
            fastest = min(results, key=lambda x: x['processing_time'])
            slowest = max(results, key=lambda x: x['processing_time'])
            logger.info(f"   • Fastest PDF: {fastest['filename']} ({fastest['processing_time']:.2f}s, worker: {fastest['worker_id']})")
            logger.info(f"   • Slowest PDF: {slowest['filename']} ({slowest['processing_time']:.2f}s, worker: {slowest['worker_id']})")
        
        # S3 Paths Summary
        logger.info(f"")
        logger.info(f"📍 S3 PATHS SUMMARY:")
        logger.info(f"")
        print("", flush=True)
        print("📍 S3 PATHS SUMMARY:", flush=True)
        print("", flush=True)
        successful_pdfs = [r for r in results if not r['error'] and r.get('corrected_s3_path')]
        if successful_pdfs:
            logger.info(f"   ✅ Corrected PDFs ({len(successful_pdfs)}):")
            print(f"   ✅ Corrected PDFs ({len(successful_pdfs)}):", flush=True)
            for result in successful_pdfs:
                logger.info(f"      • {result['filename']}")
                logger.info(f"        → {result['corrected_s3_path']}")
                print(f"      • {result['filename']}", flush=True)
                print(f"        → {result['corrected_s3_path']}", flush=True)
            logger.info(f"")
            print("", flush=True)
        else:
            logger.info(f"   ℹ️  No corrected PDFs (all used original paths or failed)")
            logger.info(f"")
            print(f"   ℹ️  No corrected PDFs (all used original paths or failed)", flush=True)
            print("", flush=True)
        
        # Force flush before cost summary (important section)
        sys.stdout.flush()
        sys.stderr.flush()
        
        # Textract Cost Summary (All PDFs)
        logger.info("=" * 80)
        logger.info(f"💰 TEXTRACT COST SUMMARY (ALL PDFs)")
        logger.info("=" * 80)
        print("=" * 80, flush=True)
        print(f"💰 TEXTRACT COST SUMMARY (ALL PDFs)", flush=True)
        print("=" * 80, flush=True)
        sys.stdout.flush()  # Extra flush after headers
        
        # Estimate total pages processed based on total cost
        # Each page costs TEXTRACT_COST_PER_PAGE, so pages = total_cost / cost_per_page
        estimated_total_pages = int(round(total_textract_cost_all_pdfs / TEXTRACT_COST_PER_PAGE)) if TEXTRACT_COST_PER_PAGE > 0 else 0
        
        logger.info(f"   • Total PDFs processed: {len(pdf_attachments)}")
        logger.info(f"   • Successful PDFs: {success_count}")
        logger.info(f"   • Failed PDFs: {failed_count}")
        logger.info(f"   • Cost per page: ${TEXTRACT_COST_PER_PAGE:.4f}")
        logger.info(f"   • Estimated total pages: {estimated_total_pages}")
        logger.info(f"   • Total Textract cost (all PDFs): ${total_textract_cost_all_pdfs:.4f}")
        logger.info(f"   • Average cost per PDF: ${total_textract_cost_all_pdfs / len(pdf_attachments):.4f}" if len(pdf_attachments) > 0 else "   • Average cost per PDF: $0.0000")
        print(f"   • Total PDFs processed: {len(pdf_attachments)}", flush=True)
        print(f"   • Successful PDFs: {success_count}", flush=True)
        print(f"   • Failed PDFs: {failed_count}", flush=True)
        print(f"   • Cost per page: ${TEXTRACT_COST_PER_PAGE:.4f}", flush=True)
        print(f"   • Estimated total pages: {estimated_total_pages}", flush=True)
        print(f"   • Total Textract cost (all PDFs): ${total_textract_cost_all_pdfs:.4f}", flush=True)
        print(f"   • Average cost per PDF: ${total_textract_cost_all_pdfs / len(pdf_attachments):.4f}" if len(pdf_attachments) > 0 else "   • Average cost per PDF: $0.0000", flush=True)
        
        # Per-PDF cost breakdown (limit to avoid truncation)
        logger.info(f"")
        logger.info(f"   📊 Per-PDF Cost Breakdown:")
        print("", flush=True)
        print(f"   📊 Per-PDF Cost Breakdown:", flush=True)
        # Limit to first 50 PDFs to avoid log truncation
        results_to_show = results[:50] if len(results) > 50 else results
        for result in results_to_show:
            status_icon = "✅" if not result['error'] else "❌"
            cost = result.get('textract_cost', 0.0)
            logger.info(f"      {status_icon} {result['filename']}: ${cost:.4f}")
            print(f"      {status_icon} {result['filename']}: ${cost:.4f}", flush=True)
        if len(results) > 50:
            logger.info(f"      ... ({len(results) - 50} more PDFs, see individual PDF logs above)")
            print(f"      ... ({len(results) - 50} more PDFs, see individual PDF logs above)", flush=True)
        sys.stdout.flush()  # Flush after cost breakdown
        
        logger.info("=" * 80)
        print("=" * 80, flush=True)
        
        # CRITICAL: Force flush before final status (most important section)
        sys.stdout.flush()
        sys.stderr.flush()
        # Additional flush to ensure logs are sent (os is already imported at top)
        try:
            if hasattr(sys.stdout, 'fileno'):
                os.fsync(sys.stdout.fileno())
            if hasattr(sys.stderr, 'fileno'):
                os.fsync(sys.stderr.fileno())
        except (OSError, AttributeError):
            pass  # Ignore if fsync fails or not available
        
        # Final status
        if failed_count > 0:
            error_msg = f"⚠️ {failed_count} PDF(s) failed processing"
            logger.error(error_msg)
            print(error_msg, flush=True)
            logger.info("=" * 80)
            logger.info("BATCH JOB COMPLETED WITH FAILURES")
            logger.info("=" * 80)
            # Exit with error code so Batch marks job as FAILED
            sys.exit(1)
        else:
            success_msg = "✅ All PDFs processed and queued successfully"
            logger.info(success_msg)
            print(success_msg, flush=True)
            logger.info("=" * 80)
            logger.info("BATCH JOB COMPLETED SUCCESSFULLY")
            logger.info("=" * 80)
            # Final flush before exit
            sys.stdout.flush()
            sys.stderr.flush()
            try:
                if hasattr(sys.stdout, 'fileno'):
                    os.fsync(sys.stdout.fileno())
                if hasattr(sys.stderr, 'fileno'):
                    os.fsync(sys.stderr.fileno())
            except:
                pass
            sys.exit(0)
            
    except Exception as e:
        error_msg = f"❌ Batch job failed: {str(e)}"
        logger.error(error_msg)
        print(error_msg, flush=True)
        import traceback
        traceback_str = traceback.format_exc()
        logger.error(traceback_str)
        print(traceback_str, flush=True)
        logger.info("=" * 80)
        logger.info("BATCH JOB FAILED")
        logger.info("=" * 80)
        sys.exit(1)


if __name__ == '__main__':
    main()

