# PDF Rotation Detection & Correction Process

## 📋 Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [How Textract Detects Orientation](#how-textract-detects-orientation)
4. [Rotation Application Logic](#rotation-application-logic)
5. [Complete Processing Flow](#complete-processing-flow)
6. [Image Size Optimization](#image-size-optimization)
7. [Technical Details](#technical-details)
8. [Example Scenarios](#example-scenarios)

---

## 🎯 Overview

This document explains how the PDF rotation detection and correction system works in `batch_pdf_processor.py`. The system uses **AWS Textract** to detect page orientation by analyzing text line geometry, then applies rotation corrections to ensure all pages are upright (0°).

### Key Features
- ✅ **Robust Detection**: Handles mixed-orientation documents (upright headers + rotated content)
- ✅ **Voting Mechanism**: Uses majority voting to filter out outliers (stamps, headers)
- ✅ **Size Optimization**: Resizes and compresses images to prevent 50+ MB PDFs
- ✅ **API Limit Handling**: Automatically resizes images to meet Textract's 10MB limit

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  AWS Batch Job (batch_pdf_processor.py)                    │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  1. Download PDF from S3                                   │
│     ↓                                                       │
│  2. Convert PDF → Images (300 DPI)                         │
│     ↓                                                       │
│  3. For Each Page:                                          │
│     ├─ Resize for Textract (max 2048px)                    │
│     ├─ Call AWS Textract API                                │
│     ├─ Calculate angles from text line geometry             │
│     ├─ Vote for dominant angle                              │
│     ├─ Apply rotation to original full-size image          │
│     ├─ Resize for saving (max 2500px)                       │
│     └─ Store in memory                                      │
│     ↓                                                       │
│  4. Save all corrected pages as PDF (compressed)           │
│     ↓                                                       │
│  5. Upload corrected PDF to S3                             │
│     ↓                                                       │
│  6. Queue to SQS for invoice processing                    │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## 🔍 How Textract Detects Orientation

### The Problem with Standard Textract

AWS Textract's `OrientationCorrection` field works well for simple documents, but **fails on mixed-orientation pages**:

**Example Failure Case:**
- Main form content: Rotated 270° (sideways)
- Header/Stamp: Upright (0°)
- Textract sees conflicting signals → Returns `ROTATE_0` ❌

### Our Solution: Manual Angle Calculation

Instead of relying on `OrientationCorrection`, we **manually calculate the angle of each text line** and use **voting** to find the dominant orientation.

#### Step-by-Step Detection Process

**Step 1: Image Preparation**
```python
# Resize if too large (Textract limit: 10,000px)
if max(image.size) > 2048:
    image.thumbnail((2048, 2048), Image.Resampling.LANCZOS)

# Convert to JPEG bytes
image.save(byte_array, format='JPEG', quality=95)

# Check file size (Textract limit: 10 MB)
if size > 10MB:
    # Reduce quality progressively: 85 → 75 → 65 → 55
    # Or resize more aggressively to 1500px
```

**Step 2: Call Textract API**
```python
response = textract_client.detect_document_text(
    Document={'Bytes': img_bytes}
)
```

**Step 3: Extract Text Lines**
```python
blocks = response.get('Blocks', [])
lines = [b for b in blocks if b.get('BlockType') == 'LINE']
```

**Step 4: Calculate Angle for Each Line**

For each text line, we calculate its angle from the polygon coordinates:

```
Polygon Structure:
  p0 (TopLeft) ────────── p1 (TopRight)
     │                        │
     │      Text Line          │
     │                        │
  p3 (BottomLeft) ──────── p2 (BottomRight)
```

**Angle Calculation:**
```python
# Use TopLeft (p0) and TopRight (p1) to get line direction
dy = p1['Y'] - p0['Y']  # Vertical change
dx = p1['X'] - p0['X']  # Horizontal change

# Calculate angle in degrees
angle_rad = math.atan2(dy, dx)
angle_deg = math.degrees(angle_rad)

# Snap to nearest: 0°, 90°, 180°, 270°
if -45 <= angle_deg <= 45:
    snapped = 0°    # Horizontal (upright)
elif 45 < angle_deg <= 135:
    snapped = 90°   # Vertical going down (rotated 90° CW)
elif -135 <= angle_deg < -45:
    snapped = 270°  # Vertical going up (rotated 270° CW)
else:
    snapped = 180°  # Upside down
```

**Visual Example:**

```
Upright Text (0°):
  ──────────────────  (dy ≈ 0, dx > 0) → angle ≈ 0°

Rotated 90° CW:
  │
  │
  │                  (dy > 0, dx ≈ 0) → angle ≈ 90°

Rotated 270° CW:
  │
  │
  │                  (dy < 0, dx ≈ 0) → angle ≈ -90° → snapped to 270°
```

**Step 5: Voting Mechanism**

```python
# Count how many lines have each angle
angles = [0, 0, 0, 270, 270, 270, 270, 0, 270, ...]
counts = Counter(angles)
# Result: {270: 85, 0: 15}

# Find dominant angle
dominant_angle, count = counts.most_common(1)[0]
# Result: (270, 85)

# Calculate dominance ratio
dominance_ratio = count / total_lines
# Result: 85 / 100 = 0.85 (85%)
```

**Step 6: Decision Logic**

```python
# Only rotate if >40% of lines agree (filters out small stamps/headers)
if dominance_ratio > 0.4:
    if dominant_angle != 0:
        return (dominant_angle, 1.0)  # Rotate!
else:
    return (0, 1.0)  # Stay upright
```

### Why This Works

**Example: Mixed-Orientation Document**

```
Page Content:
├─ Header (upright): 15 lines at 0°
├─ Main Form (rotated): 85 lines at 270°
└─ Footer (upright): 5 lines at 0°

Textract Analysis:
├─ Counts: {270: 85, 0: 20}
├─ Dominant: 270° (85 lines)
├─ Dominance: 85 / 105 = 81% (> 40% threshold)
└─ Decision: Rotate 270° ✅

Result: Header/stamps are ignored, main content rotation is detected!
```

---

## 🔄 Rotation Application Logic

### Understanding Clockwise vs Counter-Clockwise

**Textract Reports:**
- `ROTATE_90` = Page is currently rotated **90° clockwise** (top is at RIGHT)
- `ROTATE_270` = Page is currently rotated **270° clockwise** (top is at LEFT)

**PIL/Pillow Rotates:**
- `rotate(90)` = Rotates **90° counter-clockwise**
- `rotate(-90)` = Rotates **90° clockwise**

### The Correction Logic

**Key Insight:** To fix a clockwise rotation, we need to rotate counter-clockwise by the same amount.

```python
# Textract says: "Page is 90° clockwise"
detected_rotation = 90

# To fix it, rotate 90° counter-clockwise
rotation_to_apply = detected_rotation  # = 90

# PIL's rotate(90) rotates counter-clockwise
final_image = image.rotate(90, expand=True)
# Result: Page becomes upright (0°) ✅
```

### Rotation Matrix

| Textract Detects | Current State | PIL rotate() Action | Result |
|------------------|---------------|---------------------|--------|
| 0° | Upright | No rotation | Upright ✅ |
| 90° | Top at RIGHT | `rotate(90)` = 90° CCW | Upright ✅ |
| 180° | Upside down | `rotate(180)` = 180° CCW | Upright ✅ |
| 270° | Top at LEFT | `rotate(270)` = 270° CCW | Upright ✅ |

### Code Implementation

```python
# Detect rotation
detected_rotation, confidence = detect_orientation_with_textract(pil_image)

# Apply rotation if detected
if detected_rotation != 0 and confidence > 0.3:
    # Positive rotation = counter-clockwise (corrects clockwise skew)
    rotation_to_apply = detected_rotation
    final_image = pil_image.rotate(rotation_to_apply, expand=True)
```

**Why `expand=True`?**
- When rotating 90° or 270°, the image dimensions change
- `expand=True` ensures the entire rotated image is visible (no cropping)

---

## 📊 Complete Processing Flow

### High-Level Flow

```
┌─────────────────────────────────────────────────────────────┐
│  PDF File (from S3)                                          │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  Step 1: Convert PDF to Images                              │
│  - Uses pdf2image library                                   │
│  - Converts at 300 DPI (high quality for detection)         │
│  - Saves to disk (memory efficient)                         │
│  - Format: JPEG                                             │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  Step 2: Process Each Page (Loop)                          │
│                                                             │
│  For each page:                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ 2.1: Load Image from Disk                           │   │
│  │      - Opens PIL Image object                       │   │
│  │      - Converts to RGB if needed                    │   │
│  └─────────────────────────────────────────────────────┘   │
│                          ↓                                   │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ 2.2: Detect Orientation (Textract)                  │   │
│  │      - Resize to max 2048px (for Textract API)      │   │
│  │      - Check file size < 10MB                       │   │
│  │      - Call Textract API                            │   │
│  │      - Extract LINE blocks                          │   │
│  │      - Calculate angle for each line                │   │
│  │      - Vote for dominant angle                      │   │
│  │      - Return: (angle, confidence)                  │   │
│  └─────────────────────────────────────────────────────┘   │
│                          ↓                                   │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ 2.3: Apply Rotation                                  │   │
│  │      - If angle != 0 and confidence > 0.3:          │   │
│  │        → Rotate image using PIL                     │   │
│  │      - Uses original full-size image (not resized)   │   │
│  └─────────────────────────────────────────────────────┘   │
│                          ↓                                   │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ 2.4: Resize for Saving                               │   │
│  │      - Resize to max 2500px (reduces file size)     │   │
│  │      - Ensures RGB mode                              │   │
│  │      - Store in corrected_images list               │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  Step 3: Save Corrected PDF                                 │
│  - Combines all corrected pages                            │
│  - Applies JPEG compression (quality=75)                   │
│  - Sets resolution metadata (150 DPI)                      │
│  - Enables optimization                                     │
│  - Logs final file size                                     │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  Step 4: Upload to S3                                       │
│  - Uploads corrected PDF                                    │
│  - New filename: {original}_corrected.pdf                  │
│  - Verifies upload success                                  │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  Step 5: Queue to SQS                                       │
│  - Creates SQS message with corrected S3 path             │
│  - Queues for invoice processor                            │
└─────────────────────────────────────────────────────────────┘
```

### Detailed Page Processing Flow

```
Page N Processing:
├─ Load Image (full resolution, e.g., 12000x12000px)
│
├─ Detect Orientation:
│  ├─ Create resized copy (max 2048px) for Textract
│  ├─ Convert to JPEG bytes
│  ├─ Check size < 10MB (reduce quality if needed)
│  ├─ Call Textract API
│  ├─ Extract LINE blocks
│  ├─ For each line:
│  │  ├─ Get polygon coordinates (TopLeft, TopRight)
│  │  ├─ Calculate: angle = atan2(dy, dx)
│  │  └─ Snap to: 0°, 90°, 180°, 270°
│  ├─ Count angles: Counter(angles)
│  ├─ Find dominant angle
│  └─ Return: (dominant_angle, confidence)
│
├─ Apply Rotation:
│  ├─ If detected_rotation != 0:
│  │  └─ Rotate original image: rotate(detected_rotation)
│  └─ Else: Keep original
│
├─ Resize for Saving:
│  ├─ If max(size) > 2500px:
│  │  └─ Resize to max 2500px
│  └─ Ensure RGB mode
│
└─ Store in corrected_images list
```

---

## 🖼️ Image Size Optimization

### The Problem

**Before Optimization:**
- PDF pages converted at 300 DPI
- Large documents (A3, A1) → 12000x12000px images
- Each page: ~150 megapixels = ~430 MB in memory
- Output PDF: 50+ MB

### The Solution

**Two-Stage Resizing:**

1. **For Textract Detection** (lines 146-198)
   - Resize to max 2048px
   - Reduces API call size
   - Orientation detection works fine at lower resolution

2. **For PDF Saving** (lines 463-476)
   - Resize to max 2500px
   - Reduces final PDF size
   - Still high quality (≈200-300 DPI for A4)

**Compression** (lines 504-516):
- JPEG quality: 75 (good balance)
- Resolution metadata: 150 DPI
- Optimization: Enabled

### Size Reduction Example

| Stage | Original | After Resize | After Compression |
|-------|----------|--------------|-------------------|
| Dimensions | 12000×12000px | 2500×2500px | 2500×2500px |
| Pixels | 144 MP | 6.25 MP | 6.25 MP |
| File Size | ~50 MB | ~15 MB | ~2-5 MB |

**Result:** 10x size reduction while maintaining quality!

---

## 🔧 Technical Details

### Textract API Limits

| Limit | Value | Our Handling |
|-------|-------|--------------|
| Max dimension | 10,000px | Resize to 2048px |
| Max file size | 10 MB | Progressive quality reduction |
| API | `detect_document_text` | Synchronous (fastest) |

### Angle Calculation Details

**Coordinate System:**
- Textract uses normalized coordinates (0.0 to 1.0)
- TopLeft (p0): `{'X': 0.1, 'Y': 0.2}`
- TopRight (p1): `{'X': 0.9, 'Y': 0.25}`

**Angle Formula:**
```python
dy = p1['Y'] - p0['Y']  # Vertical change
dx = p1['X'] - p0['X']  # Horizontal change
angle = atan2(dy, dx)   # Returns radians (-π to π)
angle_deg = degrees(angle)  # Convert to degrees (-180° to 180°)
```

**Angle Snapping:**
```
-45° to +45°   → 0°   (horizontal, upright)
+45° to +135°  → 90°  (vertical down, rotated 90° CW)
-135° to -45°  → 270° (vertical up, rotated 270° CW)
Otherwise      → 180° (upside down)
```

### Voting Threshold

**Why 40%?**
- Filters out small stamps/headers (typically < 10% of page)
- Requires majority agreement (> 40% of lines)
- Prevents false positives from noise

**Example:**
```
100 lines total:
- 85 lines at 270° (main content)
- 15 lines at 0° (header/stamp)

Dominance: 85 / 100 = 85% (> 40%) ✅
Decision: Rotate 270°
```

---

## 📝 Example Scenarios

### Scenario 1: Simple Rotated Page

**Input:**
- Page rotated 90° clockwise (top is at RIGHT)
- All text lines are at 90°

**Processing:**
```
1. Textract finds 50 lines
2. All 50 lines calculate to 90°
3. Counter: {90: 50}
4. Dominance: 50/50 = 100% (> 40%) ✅
5. Decision: Rotate 90° CCW
6. Result: Page becomes upright ✅
```

### Scenario 2: Mixed Orientation (The Challenge)

**Input:**
- Main form: Rotated 270° (85 lines)
- Header: Upright (10 lines)
- Stamp: Upright (5 lines)

**Processing:**
```
1. Textract finds 100 lines
2. Angles calculated:
   - 85 lines → 270°
   - 15 lines → 0°
3. Counter: {270: 85, 0: 15}
4. Dominant: 270° (85 lines)
5. Dominance: 85/100 = 85% (> 40%) ✅
6. Decision: Rotate 270° CCW
7. Result: Main form becomes upright, header/stamp ignored 
```

### Scenario 3: Already Upright

**Input:**
- Page is already upright (0°)
- All text lines are at 0°

**Processing:**
```
1. Textract finds 50 lines
2. All 50 lines calculate to 0°
3. Counter: {0: 50}
4. Dominant: 0°
5. Decision: No rotation needed
6. Result: Page stays upright ✅
```

### Scenario 4: Insufficient Agreement

**Input:**
- 30 lines at 90°
- 25 lines at 0°
- 20 lines at 180°
- 25 lines at 270°

**Processing:**
```
1. Textract finds 100 lines
2. Counter: {90: 30, 0: 25, 180: 20, 270: 25}
3. Dominant: 90° (30 lines)
4. Dominance: 30/100 = 30% (< 40%) ❌
5. Decision: No rotation (insufficient agreement)
6. Result: Page stays as-is (conservative approach)
```

---

## 🎯 Key Design Decisions

### Why Manual Angle Calculation?

**Problem:** Textract's `OrientationCorrection` fails on mixed documents.

**Solution:** Calculate angles from each text line's geometry and vote.

**Benefit:** Handles complex documents with headers, stamps, and rotated content.

### Why 40% Threshold?

**Problem:** Small stamps or headers might be upright on a rotated page.

**Solution:** Require >40% of lines to agree on rotation.

**Benefit:** Filters out outliers while still detecting rotation.

### Why Two-Stage Resizing?

**Problem:** 
- Textract needs smaller images (API limits)
- But we want high-quality output PDFs

**Solution:**
- Resize to 2048px for Textract (detection)
- Resize to 2500px for saving (output quality)

**Benefit:** Fast API calls + High-quality output.

### Why Positive Rotation?

**Problem:** Textract reports clockwise, PIL rotates counter-clockwise.

**Solution:** Use positive rotation values (no negation).

**Logic:**
- Textract: "Page is 90° CW"
- PIL: `rotate(90)` = 90° CCW
- Result: Corrected to 0° ✅

---

## 📈 Performance Characteristics

### Processing Time

| Stage | Time per Page | Notes |
|-------|---------------|-------|
| PDF → Images | ~2-5s | Depends on page count |
| Textract API | ~1-3s | Network + processing |
| Angle Calculation | ~0.1s | In-memory processing |
| Rotation | ~0.5s | PIL image manipulation |
| Resize | ~0.2s | Thumbnail operation |
| PDF Save | ~1-2s | Depends on image size |

**Total per page:** ~5-10 seconds

### Memory Usage

| Stage | Memory | Notes |
|-------|--------|-------|
| Image loading | ~50-100 MB | Per page at 300 DPI |
| Textract resize | ~10-20 MB | Temporary copy |
| Final resize | ~15-30 MB | Per page at 2500px |
| PDF in memory | ~30-50 MB | All pages combined |

**Peak memory:** ~100-200 MB per page (processed sequentially)

### File Size Reduction

| Input | Output | Reduction |
|-------|--------|-----------|
| 50 MB PDF | 2-5 MB PDF | 10x smaller |
| 12000×12000px | 2500×2500px | 23x fewer pixels |

---

## 🐛 Troubleshooting

### Issue: Textract Returns No Lines

**Symptom:** `No text lines found in Textract response`

**Causes:**
- Blank page
- Image too small after resize
- Textract API error

**Solution:** Returns (0, 1.0) - assumes upright (safe default)

### Issue: InvalidParameterException

**Symptom:** `Request has invalid parameters`

**Causes:**
- Image > 10,000px (dimension limit)
- File size > 10 MB (size limit)

**Solution:** Automatic resizing and quality reduction (lines 146-198)

### Issue: PDF File Size Still Large

**Symptom:** Output PDF > 10 MB

**Causes:**
- Resize not applied (image already < 2500px)
- Compression not working

**Solution:** Check logs for resize operations, verify compression parameters

### Issue: Wrong Rotation Applied

**Symptom:** Page rotated incorrectly

**Causes:**
- Mixed orientation with insufficient voting
- Dominance ratio < 40%

**Solution:** Check logs for angle distribution, may need to adjust threshold

---

## 📚 Code References

### Key Functions

| Function | Purpose | Location |
|----------|---------|----------|
| `detect_orientation_with_textract()` | Detects rotation using Textract | Lines 127-291 |
| `rotate_pdf_optimized()` | Main PDF processing function | Lines 367-530 |
| `preprocess_pdf_rotation()` | S3 download/upload wrapper | Lines 532-600 |

### Key Variables

| Variable | Purpose |
|----------|---------|
| `detected_rotation` | Clockwise rotation detected (0, 90, 180, 270) |
| `rotation_to_apply` | Counter-clockwise rotation to apply (same value) |
| `dominance_ratio` | Percentage of lines agreeing on angle |
| `corrected_images` | List of processed PIL images |

---

## ✅ Summary

The PDF rotation system:

1. **Detects** orientation by analyzing text line geometry from Textract
2. **Votes** for dominant angle, filtering out outliers
3. **Rotates** pages using PIL's counter-clockwise rotation
4. **Optimizes** file size through resizing and compression
5. **Outputs** corrected PDFs ready for invoice processing

The system is designed to handle:
- Simple rotated pages
- Mixed-orientation documents (headers + rotated content)
- Large documents (automatic resizing)
- API limits (automatic quality reduction)

All while maintaining high quality and reasonable file sizes!

