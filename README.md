# Shimmer Data Sync API

RESTful API for managing and processing Shimmer wearable sensor data in the cloud. Handles file uploads to S3, decodes binary sensor streams with inertial calibration, stores metadata in DynamoDB, and provides endpoints for patient management and data retrieval.

## Quick Links

- [Key Features](#key-features)
- [Flexible Time-Based Grouping](#flexible-time-based-grouping-for-shimmer-data)
- [Calibration and Decoding Script](#calibration-and-decoding-script-improvements)
- [API Endpoints](#key-endpoints)
- [Setup](#setup)
- [UI deployment (AWS Amplify)](#ui-deployment-aws-amplify)
- [Architecture Overview](#architecture-overview)
- [DynamoDB Size Limit Solution](#dynamodb-size-limit-solution)
- [Project Structure](#project-structure)
- [Contributing](#contributing)
- [License](#license)


## Key Features

### Data Management
- Upload Shimmer sensor files (.txt) to S3
- Automatic filename parsing (device, timestamp, experiment, shimmer device)
- Patient-device mapping with DynamoDB
- Batch file downloads (by day, by user/date)
- Generate presigned upload/download URLs

### Sensor Data Processing
- Binary sensor data decoding (Shimmer3 format)
  - 256-byte header: device info, sample rate, enabled sensors, calibration parameters
  - Variable-length data packets (3-byte timestamp + sensor channels)
- Multi-channel support with raw and calibrated data:
  - Accel_LN (Low-Noise Accelerometer): X, Y, Z axes
  - Accel_WR (Wide-Range Accelerometer): X, Y, Z axes
  - Gyro (Gyroscope): X, Y, Z axes
  - Mag (Magnetometer): X, Y, Z axes
  - Each channel provides both raw and calibrated (_cal) values
- Inertial sensor calibration
  - Offset correction, gain scaling, alignment matrix (applied to all inertial sensors)
- Time synchronization with phone RTC and rollover correction
  - Initial RTC sync from phone timestamp (Unix epoch)
  - Output: Unix timestamps in `timestampCal`, ISO 8601 in `timestampReadable`
- Computed metrics:
  - `Accel_WR_Absolute`: Magnitude (√(x² + y² + z²)) for each sample
  - `Accel_WR_VAR`: Range (max - min) of absolute acceleration
    - UWB distance (`uwbDis`): Ultra-wideband distance readings (float, meters or device units)
    - `uwbDis`: List of UWB distance readings per sample (if available)

### Smart Storage
- Full decoded data → S3 as JSON (handles 60k+ samples)
- Summary metrics only → DynamoDB (stays under 400KB limit)
- Scalable architecture for large sensor datasets

### Daily Data Aggregation
- Scheduled Lambda service to combine daily Shimmer sensor data
- Combines all hourly files for a day based on `recordedTimestamp` (not filename sync date)
- Downsampling: Averages every 50 consecutive `Accel_WR_Absolute` points into one
- UWB counting: Counts total non-zero `uwbDis` values across all hourly files
- Stores combined data in `combinedbyDay/` folder: `device_date_shimmername_combined.json`
- Tracks last processed date to avoid reprocessing
- Groups by unique shimmer device per day (separate files for shimmer1 and shimmer2)

### Flexible Time-Based Grouping for Shimmer Data
- Decoded records are grouped by device and patient, then split into groups where all records are within a tunable time window (default: 15 seconds) of each other, regardless of date boundaries.
- Each group is assigned a unique `group_id` (e.g., `group1`, `group2`, ...) based on the earliest timestamp in the group.
- Shimmer assignment: Within each group, decoded data is assigned to `shimmer1_decoded` or `shimmer2_decoded` based on the device mapping from DynamoDB, not just by order or presence.
- Single-shimmer groups: If only one shimmer is present in a group, it is still assigned to the correct field based on the mapping, and the other field is left empty.
- This grouping approach ensures all records within a group are temporally close (within 15 seconds), supporting flexible analysis and robust downstream processing. Grouping is based on time proximity, not by calendar date.
- When combining files, shimmer assignment is always determined by looking up the device's mapping in DynamoDB (device-patient map). The system does not assign by order or filename, but uses the mapping to ensure each decoded record is placed in the correct field (`shimmer1_decoded` or `shimmer2_decoded`).

### Calibration and Decoding Script Improvements
- The calibration and decoding script (`shimmerCalibrate.py`) is a direct Python port of the MATLAB function, with robust handling for:
  - Binary decoding of Shimmer3 files, including dynamic channel parsing and custom 24-bit signed integer support.
  - Inertial sensor calibration (offset, gain, alignment) for all axes and sensor types.
  - Time calibration with rollover correction and smoothing, outputting both Unix and ISO 8601 timestamps.
  - Output file naming now preserves the original base name for both `.mat` and `.json` files.
  - All array math is implemented using standard Python (no NumPy required).
  - Optional plotting and MATLAB file export if dependencies are available.

### Smart Storage
- **Full decoded data** → S3 as JSON (handles 60k+ samples)
- **Summary metrics only** → DynamoDB (stays under 400KB limit)
- Scalable architecture for large sensor datasets

### API Endpoints
- File operations (upload, download, list, deconstructed filename parsing)
- Device-patient mapping (CRUD operations)
- File metadata and combined-meta with time-based grouping
- Decode and store sensor data (`recordedTimestamp` + `endRecordedTimestamp`)
- Decode backfill (single file or batched pending files)
- Retrieve decoded fields / full JSON from S3
- Daily aggregation and backfill for missing combined dates

## Tech Stack
- **Backend**: FastAPI with Mangum (AWS Lambda compatible)
- **Storage**: AWS S3 (raw files + decoded JSON)
- **Database**: AWS DynamoDB (metadata + summaries)
- **Deployment**: AWS Lambda with API Gateway

## Architecture Overview

<p align="center"> 
 <img src="./architecture.png" width="900" height="300"> <br/>
  <b>Figure: Shimmer Data Sync API Architecture</b>
</p>

**User → API → AWS Pipeline**

- Researchers upload Shimmer sensor `.txt` files via REST API.

**FastAPI + Mangum (Lambda)**
- Entry point that handles routing, upload, decode requests, and DynamoDB/S3 interactions.

**Shimmer Decoder (`shimmerCalibrate.py`)**
- Parses binary stream → applies calibration → generates calibrated data.

**AWS S3**
- Stores raw uploaded files.
- Stores full decoded JSON (large datasets).

**AWS DynamoDB**
- Stores file metadata and summary metrics.
- Maintains device–patient mappings.

**Retrieval Layer**
- Allows fetching summarized data quickly or full decoded datasets from S3 when needed.

## Setup

### Local Development
1. Install dependencies:
   ```sh
   pip install fastapi uvicorn boto3 python-dotenv mangum pydantic
   ```

2. Configure environment variables (`.env`):
   ```env
   S3_BUCKET=your-bucket-name
   DDB_TABLE=your-device-patient-db
   DDB_FILE_TABLE=your-file-db
   AWS_REGION=your-region
   ```

3. Run locally:
   ```sh
   uvicorn main:app --reload
   ```

### AWS Lambda Deployment
1. Package dependencies:
   ```sh
   pip install -t lambda_package/ -r requirements.txt
   cp main.py shimmer_decode.py shimmerCalibrate.py lambda_package/
   cd lambda_package && zip -r ../lambda_package.zip .
   ```

2. Deploy to Lambda and configure API Gateway

## Key Endpoints

Base URL example: `https://odb777ddnc.execute-api.us-east-2.amazonaws.com`

### File Management
- `POST /upload/` - Upload Shimmer sensor file (header metadata to DynamoDB; does not set `recordedTimestamp` from `timestampCal`)
- `GET /files/` - List all S3 object keys
- `GET /files/by-day/` - List files grouped by date (`YYYY-MM-DD`)
- `GET /files/metadata/` - Files grouped by device/date/patient; uses `recordedTimestamp` from DynamoDB when available
- `GET /files/deconstructed/` - List raw S3 uploads with filename parsed into fields (`device`, `date`, `time`, `shimmer_device`, etc.); date/time from filename only, not `timestampCal`
- `GET /file/parse-name/` - Parse one filename; query param `filename`
- `GET /files/combined-meta/` - Decoded metadata from DynamoDB + patient mapping, time-grouped (`GROUP_WINDOW_SECONDS`, default 15s). Each item in `shimmer1_decoded` / `shimmer2_decoded` includes `recordedTimestamp`, `endRecordedTimestamp`, and `decode_s3_key`
- `GET /download/{filename}` - Download file
- `POST /download-zip-by-day/` - ZIP all S3 files for a calendar date (filename date)
- `POST /download-zip-by-user-date/` - ZIP files from a metadata list (body: array of `{fullname, ...}`)
- `GET /download-zip-by-date/{date}` - ZIP raw files whose DynamoDB `recordedTimestamp` date matches `YYYY-MM-DD`; optional query `user` (patient filter)
- `GET /generate-upload-url/` - Presigned S3 upload URL
- `GET /generate-download-url/` - Presigned S3 download URL
- `GET /download-all-url/` - Presigned URL for bulk download
- `POST /missing-files/` - Compare expected vs present files

### Sensor Data Processing
- `GET /file/decode/` - Decode sensor file in memory (returns full data)
- `POST /decode-and-store/` - Full decode: large arrays → S3 `decode/{base}_decoded.json`, summary + timestamps → DynamoDB
  - Body: `{"full_file_name": "device__YYYYMMDD_HHMMSS__...__000.txt"}`
  - Sets `recordedTimestamp` from `timestampCal[0]` and `endRecordedTimestamp` from `timestampCal[-1]` (UTC ISO)
- `POST /decode-and-store/backfill/` - Re-decode one or many raw uploads to refresh timestamps in DynamoDB
  - Single file: `{"full_file_name": "device__...__000.txt"}`
  - Batch (API Gateway safe): `{"limit": 3}` — processes next pending files; repeat until `processed` is 0
  - Options: `skip_existing` (default `true`, skip rows that already have `endRecordedTimestamp`), `force` (`true` to re-decode anyway)
  - Response includes `pending_remaining` when running in batch mode
- `GET /get-decoded-field-direct/` - Read one field from S3 decoded JSON; query `full_file_name`, `field_name`
- `GET /get-decoded-file-url/` - Presigned URL for full decoded JSON in S3

### Device/Patient Mapping
- `GET /ddb/device-patient-map` - List all mappings
- `GET /ddb/device-patient-map/{device}` - Get mapping for device
- `PUT /ddb/device-patient-map/{device}` - Create/update mapping
- `DELETE /ddb/device-patient-map/{device}` - Delete mapping
- `GET /devices/unregistered` - Find devices without patient mapping

### Daily Data Aggregation
- `POST /daily-aggregator/` - Trigger daily aggregation for a specific date or automatically process next date
  - Uses `recordedTimestamp` from DynamoDB to determine actual recording date
  - Combines all hourly files for a day, downsamples `Accel_WR_Absolute` (averages every 50 points), and counts non-zero `uwbDis` values
  - Saves combined data to `combinedbyDay/` folder with format: `device_date_shimmername_combined.json`
  - Request body (optional): `{"date": "2025-12-11"}` - if omitted, processes next unprocessed date
  - Can be scheduled via EventBridge (e.g., daily at 11:45 PM IST)
- `POST /daily-aggregator/backfill/` - Process dates that have DynamoDB files but no `combinedbyDay/` output yet
  - Body (optional): `{"limit": 10}` — max dates per invocation (API Gateway may timeout on large runs)

### Combined Data Files
- `GET /combined-data-files/` - List all combined data files from `combinedbyDay/` folder
  - Groups by date and device
  - Shows patient mapping and shimmer assignments (shimmer1/shimmer2 based on device mapping)
  - Returns: `[{date, patient, shimmer1, shimmer2, shimmer1_file, shimmer2_file}, ...]`
- `GET /get-combined-data-file/` - Get full content of a combined data file
  - Query params: `filename` (e.g., `device_2025-12-11_Shimmer_DCFF_combined.json`)
  - For files >5MB, returns presigned URL; otherwise returns full JSON content
- `GET /get-combined-data-field/` - Get a specific field from a combined data file
  - Query params: `filename`, `field_name` (e.g., `accel_wr_absolute_downsampled` or `uwb_dis_non_zero_count`)
  - Returns the field value (useful for charting/visualization)

## Architecture Notes

### DynamoDB Size Limit Solution
Shimmer files can contain 60,000+ samples, making arrays too large for DynamoDB's 400KB item limit. Our solution:

1. **Full decoded data** → Stored in S3 at `decode/{base_name}_decoded.json`
2. **Summary metrics** → Stored in DynamoDB (e.g. `sampleRate`, `Accel_WR_VAR`, etc.)
3. **Reference link** → DynamoDB item includes `decode_s3_key` for full data retrieval
4. **Recording window (metadata only)** — set by `POST /decode-and-store/` or backfill:
   - `recordedTimestamp` — from `timestampCal[0]`, converted to UTC ISO 8601 (e.g. `2025-12-11T16:32:13.191559+00:00`)
   - `endRecordedTimestamp` — from `timestampCal[-1]`, same conversion (recording end)
   - Full per-sample `timestampCal` remains in the S3 decoded JSON only
5. **Filename fields** — `timestamp`, `date`, `time` parsed from the S3 key (`device__YYYYMMDD_HHMMSS__...`); used as fallback when `recordedTimestamp` is missing

`GET /files/combined-meta/` and `GET /files/metadata/` prefer **`recordedTimestamp`** (and expose **`endRecordedTimestamp`**) over filename date/time for grouping and UI start/end columns.

This keeps DynamoDB items small (~2–5 KB) while preserving full data access via S3 and avoiding frontend fetches of `timestampCal` for start/end display.

### Backfill existing files
After deploying timestamp changes, refresh DynamoDB for old uploads:

```bash
# One file
curl -X POST "$API/decode-and-store/backfill/" \
  -H "Content-Type: application/json" \
  -d '{"full_file_name": "device__20251211_223308__...__000.txt"}'

# Batch (repeat until processed=0; ~3 files per call avoids API Gateway timeout)
curl -X POST "$API/decode-and-store/backfill/" \
  -H "Content-Type: application/json" \
  -d '{"limit": 3}'
```

For hundreds of files, use `run_backfill_all.py` locally (one HTTP request per file).

## Project Structure
```
.
├── main.py                    # FastAPI application & endpoints
├── shimmerCalibrate.py        # Calibrated decoder with inertial cal
├── daily_aggregator_handler.py
├── run_backfill_all.py        # Optional: backfill all raw files via API (one file per request)
├── test/                      # scripts to test the decoder code
└── README.md
```

- **Flexible Time-Based Grouping for Shimmer Data:**
  - Decoded records are grouped by device and patient, and then split into groups where all records are within a tunable time window (default: 15 seconds) of each other, regardless of date boundaries.
  - Each group is assigned a unique `group_id` (e.g., `group1`, `group2`, ...) based on the earliest timestamp in the group.
  - This allows for robust handling of recordings that are close in time but not exactly synchronized, and supports both single- and dual-shimmer scenarios. Grouping is not strictly by date, but by temporal proximity.


## Contributing
This project is part of the Shimmer UMass research platform. For access or collaboration, contact the Shimmer research team.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
