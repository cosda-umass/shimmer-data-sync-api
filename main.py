import os
from fastapi import FastAPI, UploadFile, File, HTTPException, Query, Body, Path
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
import io
import zipfile
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from typing import List, Optional, Dict, Any
from collections import defaultdict
import json
import traceback




# Tunable grouping window for /files/combined-meta/ (seconds)
GROUP_WINDOW_SECONDS = 15
from dotenv import load_dotenv
from mangum import Mangum
from pydantic import BaseModel
from datetime import datetime, timezone
from decimal import Decimal
from fastapi import Request
import struct
from shimmerCaliberate import read_shimmer_dat

# Load environment variables from .env if present
load_dotenv()

# For AWS Lambda, credentials and region are automatically provided by the environment.
# Only S3_BUCKET should be loaded from environment variables.
S3_BUCKET = os.getenv("S3_BUCKET")

# Use default boto3 session (credentials and region are handled by Lambda)
s3_client = boto3.client("s3")

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

class FileItem(BaseModel):
    name: str
    device: str
    date: str  # YYYY-MM-DD
    time: str  # HH:MM:SS
    part: Optional[str] = None
    ext: str
    patient: Optional[str] = None

class DayFiles(BaseModel):
    date: str
    files: List[str]
# ...existing code...

# New endpoint: group files by day
@app.get("/files/by-day/", response_model=List[DayFiles])
def list_files_by_day():
    """
    Returns files grouped by date, each with a list of filenames for that day.
    """
    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET)
        contents = response.get("Contents", [])
        files_by_day = defaultdict(list)
        for obj in contents:
            key = obj["Key"]
            fi = parse_file_name(key)
            if fi.date:
                files_by_day[fi.date].append(fi.name)
        result = [DayFiles(date=day, files=sorted(files)) for day, files in sorted(files_by_day.items())]
        return result
    except (BotoCoreError, ClientError) as e:
        raise HTTPException(status_code=500, detail=str(e))

# New endpoint: download ZIP of all files for a given day
@app.post("/download-zip-by-day/")
def download_zip_by_day(date: str = Body(..., embed=True)):
    """
    Create a ZIP of all S3 files for a given date and return a presigned download URL.
    Body: { "date": "YYYY-MM-DD" }
    """
    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET)
        contents = response.get("Contents", [])
        selected_keys = []
        for obj in contents:
            key = obj["Key"]
            fi = parse_file_name(key)
            if fi.date == date:
                selected_keys.append(key)
        if not selected_keys:
            raise HTTPException(status_code=404, detail="No files found for this date.")
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zipf:
            for key in selected_keys:
                s3_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
                file_bytes = s3_obj["Body"].read()
                zipf.writestr(key, file_bytes)
        zip_buffer.seek(0)
        zip_key = f"{date}_files.zip"
        s3_client.upload_fileobj(zip_buffer, S3_BUCKET, zip_key)
        url = s3_client.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": S3_BUCKET, "Key": zip_key},
            ExpiresIn=3600
        )
        return {"download_url": url}
    except (BotoCoreError, ClientError) as e:
        raise HTTPException(status_code=500, detail=str(e))

class DevicePatientRecord(BaseModel):
    device: str
    patient: Optional[str] = None
    shimmer1: Optional[List[str]] = None  # List of permissible shimmer1 devices
    shimmer2: Optional[List[str]] = None  # List of permissible shimmer2 devices
    updatedAt: Optional[str] = None

def parse_file_name(key: str) -> FileItem:
    name = os.path.basename(key)

    # extension from last dot
    last_dot = name.rfind(".")
    ext = name[last_dot + 1:] if last_dot > -1 else ""

    # split into at most 4 segments: device, yyyymmdd, hhmmss, remainder (part+ext)
    parts = name.split("_", 3)
    device = parts[0] if len(parts) > 0 else ""
    ymd = parts[1] if len(parts) > 1 else ""
    hms = parts[2] if len(parts) > 2 else ""
    remainder = parts[3] if len(parts) > 3 else ""

    # date
    yyyy = ymd[0:4] if len(ymd) >= 4 else ""
    mm = ymd[4:6] if len(ymd) >= 6 else ""
    dd = ymd[6:8] if len(ymd) >= 8 else ""
    date = f"{yyyy}-{mm}-{dd}" if (yyyy and mm and dd) else ""

    # time
    hh = hms[0:2] if len(hms) >= 2 else ""
    mi = hms[2:4] if len(hms) >= 4 else ""
    ss = hms[4:6] if len(hms) >= 6 else ""
    time = f"{hh}:{mi}:{ss}" if (hh and mi and ss) else ""

    # part = text before first dot in the remainder (if any)
    part = None
    if remainder:
        dot_idx = remainder.find(".")
        part = remainder[:dot_idx] if dot_idx > -1 else remainder or None

    return FileItem(name=name, device=device, date=date, time=time, part=part, ext=ext)

@app.post("/upload/")
async def upload_file(file: UploadFile = File(...)):
    try:
        if not file.filename.endswith('.txt'):
            raise HTTPException(status_code=400, detail="Only .txt files are allowed.")
        # Read file bytes for decoding
        file_bytes = await file.read()
        # Upload to S3
        file.file.seek(0)
        s3_client.upload_fileobj(io.BytesIO(file_bytes), S3_BUCKET, file.filename)

        # Decode header (reuse decode_shimmer_header from combined-meta)
        def decode_shimmer_header(file_bytes):
            HEADER_LENGTH = 256
            if len(file_bytes) < HEADER_LENGTH:
                return {}
            header = file_bytes[:HEADER_LENGTH]
            def get_byte(offset):
                return header[offset]
            def get_bytes(offset, length):
                return header[offset:offset+length]
            SDH_MAC_ADDR_C_OFFSET = 24
            MAC_ADDRESS_LENGTH = 6
            mac_bytes = get_bytes(SDH_MAC_ADDR_C_OFFSET, MAC_ADDRESS_LENGTH)
            mac_address = ':'.join(f'{b:02X}' for b in mac_bytes)
            SDH_SAMPLE_RATE_0 = 0
            sample_rate_ticks = struct.unpack('<H', get_bytes(SDH_SAMPLE_RATE_0, 2))[0]
            sample_rate = 32768 / sample_rate_ticks if sample_rate_ticks else None
            SDH_SENSORS0 = 3
            SDH_SENSORS1 = 4
            SDH_SENSORS2 = 5
            sensors0 = get_byte(SDH_SENSORS0)
            sensors1 = get_byte(SDH_SENSORS1)
            sensors2 = get_byte(SDH_SENSORS2)
            SDH_CONFIG_SETUP_BYTE3 = 11
            configByte3 = get_byte(SDH_CONFIG_SETUP_BYTE3)
            SDH_TRIAL_CONFIG0 = 16
            SDH_TRIAL_CONFIG1 = 17
            trialConfig0 = get_byte(SDH_TRIAL_CONFIG0)
            trialConfig1 = get_byte(SDH_TRIAL_CONFIG1)
            SDH_SHIMMERVERSION_BYTE_0 = 30
            shimmer_version = struct.unpack('>H', get_bytes(SDH_SHIMMERVERSION_BYTE_0, 2))[0]
            SDH_MYTRIAL_ID = 32
            experiment_id = get_byte(SDH_MYTRIAL_ID)
            SDH_NSHIMMER = 33
            n_shimmer = get_byte(SDH_NSHIMMER)
            SDH_FW_VERSION_TYPE_0 = 34
            SDH_FW_VERSION_MAJOR_0 = 36
            SDH_FW_VERSION_MINOR = 38
            SDH_FW_VERSION_INTERNAL = 39
            fw_type = struct.unpack('>H', get_bytes(SDH_FW_VERSION_TYPE_0, 2))[0]
            fw_major = struct.unpack('>H', get_bytes(SDH_FW_VERSION_MAJOR_0, 2))[0]
            fw_minor = get_byte(SDH_FW_VERSION_MINOR)
            fw_internal = get_byte(SDH_FW_VERSION_INTERNAL)
            return {
                "mac_address": mac_address,
                "sample_rate": sample_rate,
                "sensors0": sensors0,
                "sensors1": sensors1,
                "sensors2": sensors2,
                "configByte3": configByte3,
                "trialConfig0": trialConfig0,
                "trialConfig1": trialConfig1,
                "shimmer_version": shimmer_version,
                "experiment_id": experiment_id,
                "n_shimmer": n_shimmer,
                "fw_type": fw_type,
                "fw_major": fw_major,
                "fw_minor": fw_minor,
                "fw_internal": fw_internal
            }

        # Parse filename for metadata
        def parse_custom_filename(fname):
            parts = fname.split("__")
            device = parts[0] if len(parts) > 0 else "none"
            timestamp = parts[1] if len(parts) > 1 else "none"
            experiment_name = parts[2] if len(parts) > 2 else "none"
            shimmer_field = parts[3] if len(parts) > 3 else "none"
            filename = parts[5] if len(parts) > 5 else "none"
            shimmer_device = shimmer_field
            shimmer_day = "none"
            if shimmer_field != "none" and "-" in shimmer_field:
                shimmer_device, shimmer_day = shimmer_field.rsplit("-", 1)
            ext = ""
            part = None
            if filename and "." in filename:
                ext = filename.split(".")[-1]
                part = filename.split(".")[0]
            elif filename:
                part = filename
            date = "none"
            time = "none"
            if timestamp and "_" in timestamp:
                ymd, hms = timestamp.split("_", 1)
                if len(ymd) == 8 and len(hms) == 6:
                    date = f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:8]}"
                    time = f"{hms[:2]}:{hms[2:4]}:{hms[4:6]}"
            return {
                "full_file_name": fname,
                "device": device,
                "timestamp": timestamp,
                "date": date,
                "time": time,
                "experiment_name": experiment_name,
                "shimmer_device": shimmer_device,
                "shimmer_day": shimmer_day,
                "filename": filename,
                "ext": ext,
                "part": part
            }

        meta = parse_custom_filename(file.filename)
        decoded = decode_shimmer_header(file_bytes)

        # Combine metadata and decoded info
        item = {**meta, **decoded, "updatedAt": datetime.now(timezone.utc).isoformat()}

        # Store in DynamoDB (use separate table for file metadata)
        file_table_name = os.getenv("DDB_FILE_TABLE")
        if not file_table_name:
            raise HTTPException(status_code=500, detail="DDB_FILE_TABLE env not set")
        ddb = boto3.resource("dynamodb")
        file_table = ddb.Table(file_table_name)
        file_table.put_item(Item=item)

        return {"filename": file.filename, "message": "Upload and decode successful", "ddb_item": item}
    except (BotoCoreError, ClientError) as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/files/", response_model=List[str])
def list_files():
    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET)
        contents = response.get("Contents", [])
        return [obj["Key"] for obj in contents]
    except (BotoCoreError, ClientError) as e:
        raise HTTPException(status_code=500, detail=str(e))

from typing import Any
@app.get("/files/metadata/")
def get_files_metadata() -> Dict[str, Any]:
    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET)
        contents = response.get("Contents", [])
        # Filter out combined files, decode folder, daily-aggregated folder, and zip files
        keys = [
            obj["Key"] for obj in contents 
            if not (obj["Key"].startswith("combinedbyDay/") or 
                   obj["Key"].startswith("decode/") or 
                   obj["Key"].startswith("daily-aggregated/") or
                   obj["Key"].endswith(".zip"))
        ]
        
        # Continue pagination if needed
        while response.get("IsTruncated"):
            response = s3_client.list_objects_v2(
                Bucket=S3_BUCKET,
                ContinuationToken=response.get("NextContinuationToken")
            )
            for obj in response.get("Contents", []):
                key = obj.get("Key")
                # Filter out combined files, decode folder, daily-aggregated folder, and zip files
                if not (key.startswith("combinedbyDay/") or 
                       key.startswith("decode/") or 
                       key.startswith("daily-aggregated/") or
                       key.endswith(".zip")):
                    keys.append(key)

        # Load device→patient mapping and shimmer lists from DynamoDB
        mapping: Dict[str, Optional[str]] = {}
        shimmer_mapping: Dict[str, Dict[str, Optional[List[str]]]] = {}
        table = _get_ddb_table()
        scan_kwargs: Dict = {"ProjectionExpression": "device, patient, shimmer1, shimmer2"}
        while True:
            dresp = table.scan(**scan_kwargs)
            for it in dresp.get("Items", []):
                dev = it.get("device")
                pat = it.get("patient")
                if dev:
                    mapping[dev] = pat if (pat is not None and pat != "") else None
                    # Normalize shimmer values to lists (handles backward compatibility)
                    shimmer1 = _normalize_shimmer_to_list(it.get("shimmer1"))
                    shimmer2 = _normalize_shimmer_to_list(it.get("shimmer2"))
                    shimmer_mapping[dev] = {
                        "shimmer1": shimmer1,
                        "shimmer2": shimmer2
                    }
            if "LastEvaluatedKey" in dresp:
                scan_kwargs["ExclusiveStartKey"] = dresp["LastEvaluatedKey"]
            else:
                break

        # Load recordedTimestamp from DynamoDB file table (for decoded files)
        file_metadata: Dict[str, Dict[str, Any]] = {}
        file_table_name = os.getenv("DDB_FILE_TABLE")
        if file_table_name:
            try:
                ddb = boto3.resource("dynamodb")
                file_table = ddb.Table(file_table_name)
                scan_kwargs = {"ProjectionExpression": "full_file_name, recordedTimestamp"}
                while True:
                    fresp = file_table.scan(**scan_kwargs)
                    for it in fresp.get("Items", []):
                        fname = it.get("full_file_name")
                        recorded_ts = it.get("recordedTimestamp")
                        if fname:
                            file_metadata[fname] = {"recordedTimestamp": recorded_ts}
                    if "LastEvaluatedKey" in fresp:
                        scan_kwargs["ExclusiveStartKey"] = fresp["LastEvaluatedKey"]
                    else:
                        break
            except Exception as e:
                # If file table doesn't exist or error, continue without recordedTimestamp
                pass

        from collections import defaultdict
        # Group by (device, date)
        def parse_custom_filename(fname):
            parts = fname.split("__")
            device = parts[0] if len(parts) > 0 else "none"
            timestamp = parts[1] if len(parts) > 1 else "none"
            experiment_name = parts[2] if len(parts) > 2 else "none"
            shimmer_field = parts[3] if len(parts) > 3 else "none"
            filename = parts[5] if len(parts) > 5 else "none"
            # Split shimmer_field into shimmer_device and shimmer_day
            shimmer_device = shimmer_field
            shimmer_day = "none"
            if shimmer_field != "none" and "-" in shimmer_field:
                shimmer_device, shimmer_day = shimmer_field.rsplit("-", 1)
            # ext and part from filename
            ext = ""
            part = None
            if filename and "." in filename:
                ext = filename.split(".")[-1]
                part = filename.split(".")[0]
            elif filename:
                part = filename
            # Parse date and time from timestamp (format: YYYYMMDD_HHMMSS)
            date = "none"
            time = "none"
            if timestamp and "_" in timestamp:
                ymd, hms = timestamp.split("_", 1)
                if len(ymd) == 8 and len(hms) == 6:
                    date = f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:8]}"
                    time = f"{hms[:2]}:{hms[2:4]}:{hms[4:6]}"
            return {
                "device": device,
                "timestamp": timestamp,
                "time": time,
                "experiment_name": experiment_name,
                "shimmer_device": shimmer_device,
                "shimmer_day": shimmer_day,
                "date": date,
                "filename": filename,
                "ext": ext,
                "part": part
            }

        # Group by (device, date, patient) - pair up all files from same date
        # Track shimmer devices by type within each date group
        grouped = defaultdict(lambda: {"files": [], "patient": None, "shimmer1_devices": set(), "shimmer2_devices": set()})
        for k in keys:
            meta = parse_custom_filename(os.path.basename(k))
            device = meta["device"]
            date = meta["date"]  # Fallback date from filename
            time = meta["time"]  # Fallback time from filename
            experiment_name = meta["experiment_name"]
            shimmer_device = meta["shimmer_device"]
            timestamp = meta["timestamp"]
            pat = mapping.get(device)
            
            # Get shimmer mapping for this device
            device_shimmer_map = shimmer_mapping.get(device, {})
            s1_list = device_shimmer_map.get("shimmer1")
            s2_list = device_shimmer_map.get("shimmer2")
            
            # Identify which shimmer type this device belongs to based on permissible lists
            # This ensures new shimmers added to the lists are automatically mapped correctly
            shimmer_type = None
            if shimmer_device != "none":
                # Check if shimmer device is in shimmer1 permissible list
                if s1_list and isinstance(s1_list, list) and shimmer_device in s1_list:
                    shimmer_type = "shimmer1"
                elif s1_list and not isinstance(s1_list, list) and shimmer_device == s1_list:
                    # Backward compatibility: handle old string format
                    shimmer_type = "shimmer1"
                # Check if shimmer device is in shimmer2 permissible list
                elif s2_list and isinstance(s2_list, list) and shimmer_device in s2_list:
                    shimmer_type = "shimmer2"
                elif s2_list and not isinstance(s2_list, list) and shimmer_device == s2_list:
                    # Backward compatibility: handle old string format
                    shimmer_type = "shimmer2"
            
            # Get recordedTimestamp from DynamoDB if available
            file_meta = file_metadata.get(k, {})
            recorded_ts = file_meta.get("recordedTimestamp")
            
            # ALWAYS use recordedTimestamp date for grouping (paired by actual recording date)
            # Parse date and time from recordedTimestamp - this is the source of truth
            if recorded_ts:
                try:
                    # Parse ISO format timestamp (e.g., "2024-09-24T22:38:36+00:00")
                    dt = datetime.fromisoformat(recorded_ts.replace("Z", "+00:00"))
                    date = dt.strftime("%Y-%m-%d")  # Use date from recordedTimestamp for grouping
                    time = dt.strftime("%H:%M:%S")  # Use time from recordedTimestamp
                except (ValueError, AttributeError):
                    # If parsing fails, keep filename-based date/time as fallback
                    pass
            # If no recordedTimestamp, use filename date (but these should ideally have recordedTimestamp)
            
            file_record = {
                "fullname": k,
                "timestamp": timestamp,
                "time": time,  # Use time from recordedTimestamp if available
                "filename": meta["filename"],
                "shimmer_device": meta["shimmer_device"],
                "shimmer_day": meta["shimmer_day"],
                "ext": meta["ext"],
                "part": meta["part"],
                "experiment_name": experiment_name
            }
            if recorded_ts:
                file_record["recordedTimestamp"] = recorded_ts
            
            # Group by (device, date, patient) - all files from same date are paired together
            group_key = (device, date, pat)
            
            grouped[group_key]["files"].append(file_record)
            grouped[group_key]["patient"] = pat if (pat is not None and pat != "") else "none"
            grouped[group_key]["experiment_name"] = experiment_name
            
            # Track shimmer devices by type - collect all shimmers used on this date
            if shimmer_device != "none" and shimmer_type:
                if shimmer_type == "shimmer1":
                    grouped[group_key]["shimmer1_devices"].add(shimmer_device)
                elif shimmer_type == "shimmer2":
                    grouped[group_key]["shimmer2_devices"].add(shimmer_device)
        
        # Convert to desired output format
        result = []
        for (device, date, patient), value in grouped.items():
            # Get first shimmer device from each type (if any)
            # If multiple shimmers of same type exist (e.g., backup), show the first one
            shimmer1_list = sorted(list(value["shimmer1_devices"]))
            shimmer2_list = sorted(list(value["shimmer2_devices"]))
            shimmer1 = shimmer1_list[0] if len(shimmer1_list) > 0 else "none"
            shimmer2 = shimmer2_list[0] if len(shimmer2_list) > 0 else "none"
            
            result.append({
                "device": device,
                "date": date,
                "experiment_name": value["experiment_name"],
                "shimmer1": shimmer1,
                "shimmer2": shimmer2,
                "files": value["files"],
                "patient": patient
            })
        return {"data": result, "error": None}
    except (BotoCoreError, ClientError, Exception) as e:
        return {"data": [], "error": str(e)}

@app.get("/download/{filename}")
def download_file(filename: str):
    try:
        fileobj = s3_client.get_object(Bucket=S3_BUCKET, Key=filename)["Body"]
        return StreamingResponse(
            fileobj,
            media_type="text/plain",
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Type": "text/plain"
            }
        )
    except (BotoCoreError, ClientError) as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.get("/generate-upload-url/")
async def generate_upload_url(filename: str = Query(...), request: Request = None):
    """
    Optionally accepts 'tags' as a query parameter (tags as key1=value1&key2=value2).
    """
    try:
        tags = request.query_params.get("tags") if request else None
        params = {"Bucket": S3_BUCKET, "Key": filename}
        if tags:
            params["Tagging"] = tags
        url = s3_client.generate_presigned_url(
            ClientMethod="put_object",
            Params=params,
            ExpiresIn=3600
        )
        return {"upload_url": url}
    except (BotoCoreError, ClientError) as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/generate-download-url/")
def generate_download_url(filename: str = Query(...)):
    try:
        url = s3_client.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": S3_BUCKET, "Key": filename},
            ExpiresIn=3600  # URL valid for 1 hour
        )
        return {"download_url": url}
    except (BotoCoreError, ClientError) as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/missing-files/")
def missing_files(filenames: List[str] = Body(...)):
    """
    Given a list of filenames, return the ones not present in S3.
    """
    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET)
        s3_files = set(obj["Key"] for obj in response.get("Contents", []))
        missing = [f for f in filenames if f not in s3_files]
        return {"missing_files": missing}
    except (BotoCoreError, ClientError) as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/download-all-url/")
def download_all_url():
    """
    Create a ZIP of all S3 files, upload to S3, and return a presigned download URL.
    """
    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET)
        contents = response.get("Contents", [])
        if not contents:
            raise HTTPException(status_code=404, detail="No files found in S3 bucket.")
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zipf:
            for obj in contents:
                key = obj["Key"]
                s3_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
                file_bytes = s3_obj["Body"].read()
                zipf.writestr(key, file_bytes)
        zip_buffer.seek(0)
        zip_key = "all_files.zip"
        # Upload ZIP to S3
        s3_client.upload_fileobj(zip_buffer, S3_BUCKET, zip_key)
        # Generate presigned URL
        url = s3_client.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": S3_BUCKET, "Key": zip_key},
            ExpiresIn=3600  # 1 hour
        )
        return {"download_url": url}
    except (BotoCoreError, ClientError) as e:
        raise HTTPException(status_code=500, detail=str(e))

# ---------------------- DynamoDB helpers ----------------------

def _normalize_shimmer_to_list(shimmer_value):
    """
    Normalize shimmer value to list format.
    Converts string to single-item list, keeps list as-is, returns None for None/empty.
    This ensures all shimmer values are in list format for consistent processing.
    """
    if shimmer_value is None:
        return None
    if isinstance(shimmer_value, list):
        return shimmer_value if shimmer_value else None
    # Convert string to list
    if isinstance(shimmer_value, str) and shimmer_value.strip():
        return [shimmer_value]
    return None

def _get_ddb_table():
    table_name = os.getenv("DDB_TABLE")
    if not table_name:
        raise HTTPException(status_code=500, detail="DDB_TABLE env not set")
    ddb = boto3.resource("dynamodb")
    return ddb.Table(table_name)

# ---------------------- DynamoDB mapping endpoints ----------------------
@app.get("/ddb/device-patient-map", response_model=List[DevicePatientRecord])
def ddb_get_device_patient_map():
    """Return full list of records with device, patient, updatedAt from DynamoDB."""
    try:
        table = _get_ddb_table()
        records: List[DevicePatientRecord] = []
        scan_kwargs: Dict = {"ProjectionExpression": "device, patient, shimmer1, shimmer2, updatedAt"}
        while True:
            resp = table.scan(**scan_kwargs)
            for it in resp.get("Items", []):
                # Normalize shimmer values to lists (handles backward compatibility)
                shimmer1 = _normalize_shimmer_to_list(it.get("shimmer1"))
                shimmer2 = _normalize_shimmer_to_list(it.get("shimmer2"))
                
                records.append(DevicePatientRecord(
                    device=it.get("device", ""),
                    patient=it.get("patient"),
                    shimmer1=shimmer1,
                    shimmer2=shimmer2,
                    updatedAt=it.get("updatedAt")
                ))
            if "LastEvaluatedKey" in resp:
                scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
            else:
                break
        return records
    except (BotoCoreError, ClientError) as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/ddb/device-patient-map/details", response_model=List[DevicePatientRecord])
def ddb_get_device_patient_map_details():
    """Return full records with device, patient, updatedAt from DynamoDB."""
    try:
        table = _get_ddb_table()
        records: List[DevicePatientRecord] = []
        scan_kwargs: Dict = {"ProjectionExpression": "device, patient, shimmer1, shimmer2, updatedAt"}
        while True:
            resp = table.scan(**scan_kwargs)
            for it in resp.get("Items", []):
                # Handle backward compatibility: convert string to list if needed
                shimmer1 = it.get("shimmer1")
                shimmer2 = it.get("shimmer2")
                if shimmer1 is not None and not isinstance(shimmer1, list):
                    shimmer1 = [shimmer1] if shimmer1 else None
                if shimmer2 is not None and not isinstance(shimmer2, list):
                    shimmer2 = [shimmer2] if shimmer2 else None
                
                records.append(DevicePatientRecord(
                    device=it.get("device", ""),
                    patient=it.get("patient"),
                    shimmer1=shimmer1,
                    shimmer2=shimmer2,
                    updatedAt=it.get("updatedAt")
                ))
            if "LastEvaluatedKey" in resp:
                scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
            else:
                break
        return records
    except (BotoCoreError, ClientError) as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/ddb/device-patient-map/{device}")
def ddb_get_device_mapping(device: str):
    try:
        table = _get_ddb_table()
        resp = table.get_item(Key={"device": device})
        item = resp.get("Item")
        if not item:
            raise HTTPException(status_code=404, detail="Device not found")
        
        # Handle backward compatibility: convert string to list if needed
        shimmer1 = item.get("shimmer1")
        shimmer2 = item.get("shimmer2")
        if shimmer1 is not None and not isinstance(shimmer1, list):
            shimmer1 = [shimmer1] if shimmer1 else None
        if shimmer2 is not None and not isinstance(shimmer2, list):
            shimmer2 = [shimmer2] if shimmer2 else None
        
        return {
            "device": device,
            "patient": item.get("patient"),
            "shimmer1": shimmer1,
            "shimmer2": shimmer2,
            "updatedAt": item.get("updatedAt")
        }
    except (BotoCoreError, ClientError) as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/ddb/device-patient-map", response_model=List[DevicePatientRecord])
def ddb_put_device_patient_map(mapping: Dict[str, Any] = Body(...)):
    """Replace the map by writing items and return full records (device, patient, updatedAt)."""
    try:
        table = _get_ddb_table()
        written: List[DevicePatientRecord] = []
        devices = list(mapping.keys())
        for i in range(0, len(devices), 25):
            chunk = devices[i:i+25]
            with table.batch_writer() as batch:
                for d in chunk:
                    ts = datetime.now(timezone.utc).isoformat()
                    patient = mapping[d].get("patient") if isinstance(mapping[d], dict) else mapping[d]
                    shimmer1 = mapping[d].get("shimmer1") if isinstance(mapping[d], dict) else None
                    shimmer2 = mapping[d].get("shimmer2") if isinstance(mapping[d], dict) else None
                    
                    # Convert string to list for backward compatibility
                    if shimmer1 is not None and not isinstance(shimmer1, list):
                        shimmer1 = [shimmer1] if shimmer1 else None
                    if shimmer2 is not None and not isinstance(shimmer2, list):
                        shimmer2 = [shimmer2] if shimmer2 else None
                    
                    batch.put_item(Item={
                        "device": d,
                        "patient": patient,
                        "shimmer1": shimmer1,
                        "shimmer2": shimmer2,
                        "updatedAt": ts,
                    })
                    written.append(DevicePatientRecord(device=d, patient=patient, shimmer1=shimmer1, shimmer2=shimmer2, updatedAt=ts))
        return written
    except (BotoCoreError, ClientError) as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/ddb/device-patient-map/{device}")
def ddb_put_device_mapping(device: str, payload: Dict[str, Any] = Body(...)):
    patient = payload.get("patient")
    shimmer1 = payload.get("shimmer1")
    shimmer2 = payload.get("shimmer2")
    if not patient:
        raise HTTPException(status_code=400, detail="'patient' is required")
    
    # Convert string to list for backward compatibility
    if shimmer1 is not None and not isinstance(shimmer1, list):
        shimmer1 = [shimmer1] if shimmer1 else None
    if shimmer2 is not None and not isinstance(shimmer2, list):
        shimmer2 = [shimmer2] if shimmer2 else None
    
    try:
        table = _get_ddb_table()
        ts = datetime.now(timezone.utc).isoformat()
        table.put_item(Item={
            "device": device,
            "patient": patient,
            "shimmer1": shimmer1,
            "shimmer2": shimmer2,
            "updatedAt": ts,
        })
        return {
            "device": device,
            "patient": patient,
            "shimmer1": shimmer1,
            "shimmer2": shimmer2,
            "updatedAt": ts
        }
    except (BotoCoreError, ClientError) as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/ddb/device-patient-map/{device}")
def ddb_delete_device_mapping(device: str):
    try:
        table = _get_ddb_table()
        resp = table.delete_item(
            Key={"device": device},
            ConditionExpression="attribute_exists(device)",
            ReturnValues="ALL_OLD",
        )
        attrs = resp.get("Attributes", {}) or {}
        return {
            "device": attrs.get("device", device),
            "patient": attrs.get("patient"),
            "updatedAt": attrs.get("updatedAt"),
            "deleted": True,
        }
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            raise HTTPException(status_code=404, detail="Device not found")
        raise HTTPException(status_code=500, detail=str(e))
    except BotoCoreError as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/devices/unregistered", response_model=List[str])
def get_unregistered_devices():
    """Return devices present in S3 filenames but missing in DynamoDB mapping."""
    try:
        # Collect unique devices from S3 object keys
        devices_in_s3 = set()
        resp = s3_client.list_objects_v2(Bucket=S3_BUCKET)
        contents = resp.get("Contents", [])
        for obj in contents:
            key = obj.get("Key")
            if not key:
                continue
            # Skip combined files, decode folder, daily-aggregated folder, and zip files
            if key.startswith("combinedbyDay/") or key.startswith("decode/") or key.startswith("daily-aggregated/") or key.endswith(".zip"):
                continue
            dev = parse_file_name(key).device
            if dev:
                devices_in_s3.add(dev)
        while resp.get("IsTruncated"):
            resp = s3_client.list_objects_v2(
                Bucket=S3_BUCKET,
                ContinuationToken=resp.get("NextContinuationToken")
            )
            for obj in resp.get("Contents", []):
                key = obj.get("Key")
                if not key:
                    continue
                # Skip combined files, decode folder, daily-aggregated folder, and zip files
                if key.startswith("combinedbyDay/") or key.startswith("decode/") or key.startswith("daily-aggregated/") or key.endswith(".zip"):
                    continue
                dev = parse_file_name(key).device
                if dev:
                    devices_in_s3.add(dev)

        # Collect registered devices from DynamoDB
        table = _get_ddb_table()
        registered = set()
        scan_kwargs: Dict = {"ProjectionExpression": "device"}
        while True:
            dresp = table.scan(**scan_kwargs)
            for it in dresp.get("Items", []):
                dev = it.get("device")
                if dev:
                    registered.add(dev)
            if "LastEvaluatedKey" in dresp:
                scan_kwargs["ExclusiveStartKey"] = dresp["LastEvaluatedKey"]
            else:
                break

        missing = sorted(list(devices_in_s3 - registered))
        return missing
    except (BotoCoreError, ClientError) as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/patients", response_model=List[str])
def list_unique_patients():
    """Return a sorted unique list of patient names from DynamoDB (exclude empty/null)."""
    try:
        table = _get_ddb_table()
        patients = set()
        scan_kwargs: Dict = {"ProjectionExpression": "patient"}
        while True:
            resp = table.scan(**scan_kwargs)
            for it in resp.get("Items", []):
                p = it.get("patient")
                if p is not None and str(p).strip() != "":
                    patients.add(str(p))
            if "LastEvaluatedKey" in resp:
                scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
            else:
                break
        return sorted(patients)
    except (BotoCoreError, ClientError) as e:
        raise HTTPException(status_code=500, detail=str(e))

handler = Mangum(app)

# Endpoint: download ZIP of files for a user and date (accepts metadata file list)
@app.post("/download-zip-by-user-date/")
def download_zip_by_user_date(files: List[Dict] = Body(...)):
    """
    Accepts the 'files' array from metadata (list of dicts), extracts 'fullname' from each, zips those files, uploads the ZIP to S3, and returns a presigned download URL.
    Body: [ {"fullname": "file1.txt", ...}, ... ]
    """
    try:
        if not files:
            raise HTTPException(status_code=400, detail="No files provided.")
        filenames = [f.get("fullname") for f in files if f.get("fullname")]
        if not filenames:
            raise HTTPException(status_code=400, detail="No valid 'fullname' fields found.")
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zipf:
            for key in filenames:
                try:
                    s3_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
                    file_bytes = s3_obj["Body"].read()
                    zipf.writestr(key, file_bytes)
                except (BotoCoreError, ClientError) as e:
                    raise HTTPException(status_code=404, detail=f"File not found: {key}")
        zip_buffer.seek(0)
        # Use first file's device and date for ZIP name if available
        zip_key = "user_date_files.zip"
        if files and files[0].get("fullname"):
            first = files[0]["fullname"]
            parts = first.split("_")
            if len(parts) >= 3:
                device = parts[0]
                ymd = parts[1]
                zip_key = f"{device}_{ymd}_files.zip"
        s3_client.upload_fileobj(zip_buffer, S3_BUCKET, zip_key)
        url = s3_client.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": S3_BUCKET, "Key": zip_key},
            ExpiresIn=3600
        )
        return {"download_url": url}
    except (BotoCoreError, ClientError) as e:
        raise HTTPException(status_code=500, detail=str(e))



@app.get("/file/parse-name/")
def parse_filename(filename: str = Query(...)):
    """
    Parses a filename and returns its components as JSON.
    Handles the custom format: device__timestamp__experiment__shimmer_field__filename
    """
    try:
        def parse_custom_filename(fname):
            parts = fname.split("__")
            device = parts[0] if len(parts) > 0 else "none"
            timestamp = parts[1] if len(parts) > 1 else "none"
            experiment_name = parts[2] if len(parts) > 2 else "none"
            shimmer_field = parts[3] if len(parts) > 3 else "none"
            filename = parts[5] if len(parts) > 5 else "none"
            
            # Split shimmer_field into shimmer_device and shimmer_day
            shimmer_device = shimmer_field
            shimmer_day = "none"
            if shimmer_field != "none" and "-" in shimmer_field:
                shimmer_device, shimmer_day = shimmer_field.rsplit("-", 1)
            
            # ext and part from filename
            ext = ""
            part = None
            if filename and "." in filename:
                ext = filename.split(".")[-1]
                part = filename.split(".")[0]
            elif filename:
                part = filename
            
            # Parse date and time from timestamp (format: YYYYMMDD_HHMMSS)
            date = "none"
            time = "none"
            if timestamp and "_" in timestamp:
                ymd, hms = timestamp.split("_", 1)
                if len(ymd) == 8 and len(hms) == 6:
                    date = f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:8]}"
                    time = f"{hms[:2]}:{hms[2:4]}:{hms[4:6]}"
            
            return {
                "original_filename": fname,
                "device": device,
                "timestamp": timestamp,
                "date": date,
                "time": time,
                "experiment_name": experiment_name,
                "shimmer_device": shimmer_device,
                "shimmer_day": shimmer_day,
                "filename": filename,
                "ext": ext,
                "part": part
            }
        
        parsed = parse_custom_filename(filename)
        return parsed
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/files/deconstructed/")
def get_deconstructed_files():
    """
    Returns a list of all files in S3 with their parsed components as individual JSON records.
    Each file is returned as a separate record with all its parsed fields.
    Skips .zip files and files in the decode folder.
    """
    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET)
        contents = response.get("Contents", [])
        
        # Continue pagination if needed
        while response.get("IsTruncated"):
            response = s3_client.list_objects_v2(
                Bucket=S3_BUCKET,
                ContinuationToken=response.get("NextContinuationToken")
            )
            contents.extend(response.get("Contents", []))
        
        def parse_custom_filename(fname):
            parts = fname.split("__")
            device = parts[0] if len(parts) > 0 else "none"
            timestamp = parts[1] if len(parts) > 1 else "none"
            experiment_name = parts[2] if len(parts) > 2 else "none"
            shimmer_field = parts[3] if len(parts) > 3 else "none"
            filename = parts[5] if len(parts) > 5 else "none"
            
            # Split shimmer_field into shimmer_device and shimmer_day
            shimmer_device = shimmer_field
            shimmer_day = "none"
            if shimmer_field != "none" and "-" in shimmer_field:
                shimmer_device, shimmer_day = shimmer_field.rsplit("-", 1)
            
            # ext and part from filename
            ext = ""
            part = None
            if filename and "." in filename:
                ext = filename.split(".")[-1]
                part = filename.split(".")[0]
            elif filename:
                part = filename
            
            # Parse date and time from timestamp (format: YYYYMMDD_HHMMSS)
            date = "none"
            time = "none"
            if timestamp and "_" in timestamp:
                ymd, hms = timestamp.split("_", 1)
                if len(ymd) == 8 and len(hms) == 6:
                    date = f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:8]}"
                    time = f"{hms[:2]}:{hms[2:4]}:{hms[4:6]}"
            
            return {
                "fullname": fname,
                "device": device,
                "timestamp": timestamp,
                "date": date,
                "time": time,
                "experiment_name": experiment_name,
                "shimmer_device": shimmer_device,
                "shimmer_day": shimmer_day,
                "filename": filename,
                "ext": ext,
                "part": part
            }
        
        result = []
        for obj in contents:
            key = obj["Key"]
            # Skip .zip files, decode folder, combinedbyDay folder, and daily-aggregated folder
            if (key.lower().endswith('.zip') or 
                key.startswith("decode/") or 
                key.startswith("combinedbyDay/") or 
                key.startswith("daily-aggregated/")):
                continue
            parsed = parse_custom_filename(key)
            result.append(parsed)
        
        return {"data": result, "error": None}
    
    except (BotoCoreError, ClientError, Exception) as e:
        return {"data": [], "error": str(e)}


# @app.get("/files/combined-meta/")
# def get_combined_meta():
#     """
#     Combines decoded file metadata from DynamoDB with patient mapping.
#     Each record includes S3 pointer ('decode_s3_key') to full decoded arrays.
#     Excludes heavy fields like headerBytes and raw sensor channels.
#     STRICTLY enforces one record per shimmer per group (max 2 records per group).
#     """
#     try:
#         # ----------- Load decoded metadata -----------
#         file_table_name = os.getenv("DDB_FILE_TABLE")
#         if not file_table_name:
#             return {"data": [], "error": "DDB_FILE_TABLE env not set"}

#         ddb = boto3.resource("dynamodb")
#         file_table = ddb.Table(file_table_name)
#         items = []
#         scan_kwargs = {}
#         while True:
#             resp = file_table.scan(**scan_kwargs)
#             items.extend(resp.get("Items", []))
#             if "LastEvaluatedKey" in resp:
#                 scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
#             else:
#                 break

#         # ----------- Load patient mapping -----------
#         mapping_table_name = os.getenv("DDB_TABLE")
#         mapping_table = ddb.Table(mapping_table_name) if mapping_table_name else None

#         from collections import defaultdict
        
#         # Group by (patient, device, date, within 15 sec)
#         # 1. Collect all records by (patient, device, date)
#         records_by_key = defaultdict(list)
#         for item in items:
#             device = item.get("device", "none")
#             date = item.get("date", "none")
#             shimmer_name = item.get("shimmer_device", "none")
#             decode_s3_key = item.get("decode_s3_key", None)
#             # Get patient
#             patient = "none"
#             if mapping_table and device != "none":
#                 try:
#                     resp = mapping_table.get_item(Key={"device": device})
#                     patient = resp.get("Item", {}).get("patient", "none")
#                 except Exception:
#                     pass
#             # Remove unneeded heavy keys (just in case)
#             EXCLUDE_KEYS = {"headerBytes", "Accel_LN_X", "Accel_LN_Y", "Accel_LN_Z",
#                             "Gyro_X", "Gyro_Y", "Gyro_Z", "Mag_X", "Mag_Y", "Mag_Z"}
#             record = {k: v for k, v in item.items() if k not in EXCLUDE_KEYS}
#             record["decode_s3_key"] = decode_s3_key
#             record["shimmer_name"] = shimmer_name
#             record["patient"] = patient
#             # Parse timestamp as unix
#             recorded_ts = item.get("recordedTimestamp")
#             try:
#                 ts_unix = None
#                 if recorded_ts and isinstance(recorded_ts, str):
#                     ts_unix = datetime.fromisoformat(recorded_ts.replace("Z", "+00:00")).timestamp()
#             except Exception:
#                 ts_unix = None
#             record["_ts_unix"] = ts_unix
#             records_by_key[(patient, device, date)].append(record)

#         # Build device->shimmer1/shimmer2 mapping for assignment
#         shimmer_map = {}
#         if mapping_table_name:
#             scan_kwargs = {"ProjectionExpression": "device, shimmer1, shimmer2"}
#             while True:
#                 mresp = mapping_table.scan(**scan_kwargs)
#                 for it in mresp.get("Items", []):
#                     dev = it.get("device")
#                     s1 = it.get("shimmer1")
#                     s2 = it.get("shimmer2")
#                     if dev:
#                         shimmer_map[dev] = {"shimmer1": s1, "shimmer2": s2}
#                 if "LastEvaluatedKey" in mresp:
#                     scan_kwargs["ExclusiveStartKey"] = mresp["LastEvaluatedKey"]
#                 else:
#                     break

#         grouped = defaultdict(lambda: {"shimmer1_decoded": [], "shimmer2_decoded": []})
        
#         for key, recs in records_by_key.items():
#             # Sort by timestamp
#             recs = sorted([r for r in recs if r["_ts_unix"] is not None], key=lambda r: r["_ts_unix"])
            
#             group_id = 0
#             prev_ts = None
#             group_shimmer_count = {}  # Track how many of each shimmer type in current group
            
#             for rec in recs:
#                 shimmer_name = rec["shimmer_name"]
#                 device = key[1]
#                 mapping = shimmer_map.get(device, {})
#                 s1 = mapping.get("shimmer1")
#                 s2 = mapping.get("shimmer2")
                
#                 # Determine shimmer type (1 or 2)
#                 shimmer_type = None
#                 if s1 and shimmer_name == s1:
#                     shimmer_type = "shimmer1"
#                 elif s2 and shimmer_name == s2:
#                     shimmer_type = "shimmer2"
#                 else:
#                     # Fallback logic
#                     shimmer_type = "shimmer1" if shimmer_name == s1 else "shimmer2"
                
#                 # Check if we need a new group
#                 # Start new group if: no prev_ts, gap > 15s, OR current shimmer type already exists in group
#                 if (prev_ts is None or 
#                     rec["_ts_unix"] - prev_ts > GROUP_WINDOW_SECONDS or
#                     group_shimmer_count.get(shimmer_type, 0) > 0):
#                     group_id += 1
#                     group_shimmer_count = {}  # Reset counter for new group
                
#                 group_key = (*key, f"group{group_id}")
#                 group = grouped[group_key]
#                 group["device"] = key[1]
#                 group["date"] = key[2]
#                 group["patient"] = key[0]
#                 group["group_id"] = f"group{group_id}"
                
#                 # Add to appropriate shimmer list
#                 if shimmer_type == "shimmer1":
#                     group["shimmer1"] = shimmer_name
#                     group["shimmer1_decoded"].append(rec)
#                 else:
#                     group["shimmer2"] = shimmer_name
#                     group["shimmer2_decoded"].append(rec)
                
#                 # Update counters
#                 group_shimmer_count[shimmer_type] = group_shimmer_count.get(shimmer_type, 0) + 1
#                 prev_ts = rec["_ts_unix"]

#         result = list(grouped.values())
#         return {"data": result, "error": None}

#     except Exception as e:
#         return {"data": [], "error": str(e)}

@app.get("/files/combined-meta/")
def get_combined_meta():
    """
    Combines decoded file metadata from DynamoDB with patient mapping.
    Each record includes S3 pointer ('decode_s3_key') to full decoded arrays.
    STRICTLY enforces one record per shimmer per group (max 2 records per group).
    Uses recordedTimestamp for grouping (NOT filename date).  # modified
    Cross-day grouping supported (23:59 -> 00:00).             # modified
    """
    try:
        # ----------- Load decoded metadata -----------
        file_table_name = os.getenv("DDB_FILE_TABLE")
        if not file_table_name:
            return {"data": [], "error": "DDB_FILE_TABLE env not set"}

        ddb = boto3.resource("dynamodb")
        file_table = ddb.Table(file_table_name)

        items = []
        scan_kwargs = {}
        while True:
            resp = file_table.scan(**scan_kwargs)
            items.extend(resp.get("Items", []))
            if "LastEvaluatedKey" in resp:
                scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
            else:
                break

        # ----------- Load patient mapping -----------
        mapping_table_name = os.getenv("DDB_TABLE")
        mapping_table = ddb.Table(mapping_table_name) if mapping_table_name else None

        from collections import defaultdict

        GROUP_WINDOW_SECONDS = 15

        # ====== CHANGED: group by ONLY (patient, device) NOT date ====== # modified
        records_by_key = defaultdict(list)

        for item in items:
            device = item.get("device", "none")
            shimmer_name = item.get("shimmer_device", "none")
            decode_s3_key = item.get("decode_s3_key", None)
            date = item.get("date", "none")
            
            # Parse date from filename if date is missing or invalid
            full_file_name = item.get("full_file_name", "")
            if (date == "none" or not date or date == "28-10" or len(date) < 10) and full_file_name:
                # Parse date from filename: device__YYYYMMDD_HHMMSS__...
                try:
                    parts = full_file_name.split("__")
                    if len(parts) > 1:
                        timestamp = parts[1]
                        if "_" in timestamp:
                            ymd, hms = timestamp.split("_", 1)
                            if len(ymd) == 8:
                                date = f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:8]}"
                except Exception:
                    pass

            # Get patient
            patient = "none"
            if mapping_table and device != "none":
                try:
                    resp = mapping_table.get_item(Key={"device": device})
                    patient = resp.get("Item", {}).get("patient", "none")
                except Exception:
                    pass

            # Remove heavy fields
            EXCLUDE_KEYS = {
                "headerBytes", "Accel_LN_X", "Accel_LN_Y", "Accel_LN_Z",
                "Gyro_X", "Gyro_Y", "Gyro_Z", "Mag_X", "Mag_Y", "Mag_Z"
            }
            record = {k: v for k, v in item.items() if k not in EXCLUDE_KEYS}

            record["decode_s3_key"] = decode_s3_key
            record["shimmer_name"] = shimmer_name
            record["patient"] = patient
            record["date"] = date

            # Parse timestamp as UNIX
            recorded_ts = item.get("recordedTimestamp")
            try:
                ts_unix = None
                if recorded_ts and isinstance(recorded_ts, str):
                    ts_unix = datetime.fromisoformat(
                        recorded_ts.replace("Z", "+00:00")
                    ).timestamp()
            except Exception:
                ts_unix = None

            record["_ts_unix"] = ts_unix

            # ====== CHANGED: group ONLY by (patient, device) ====== # modified
            records_by_key[(patient, device)].append(record)

        # ----------- Build shimmer assignment map -----------
        shimmer_map = {}
        if mapping_table_name:
            scan_kwargs = {"ProjectionExpression": "device, shimmer1, shimmer2"}
            while True:
                mresp = mapping_table.scan(**scan_kwargs)
                for it in mresp.get("Items", []):
                    dev = it.get("device")
                    if dev:
                        # Normalize shimmer values to lists (handles backward compatibility)
                        shimmer1 = _normalize_shimmer_to_list(it.get("shimmer1"))
                        shimmer2 = _normalize_shimmer_to_list(it.get("shimmer2"))
                        
                        shimmer_map[dev] = {
                            "shimmer1": shimmer1,
                            "shimmer2": shimmer2,
                        }
                if "LastEvaluatedKey" in mresp:
                    scan_kwargs["ExclusiveStartKey"] = mresp["LastEvaluatedKey"]
                else:
                    break

        # ----------- Final grouping -----------

        grouped = []

        for (patient, device), recs in records_by_key.items():

            # Sort by timestamp
            recs = sorted(
                [r for r in recs if r["_ts_unix"] is not None],
                key=lambda r: r["_ts_unix"]
            )

            mapping = shimmer_map.get(device, {})
            s1 = mapping.get("shimmer1")
            s2 = mapping.get("shimmer2")

            curr_group = None
            group_id = 0

            for rec in recs:
                shimmer_name = rec["shimmer_name"]

                # Identify shimmer type - check if shimmer_name is in the list
                # This ensures new shimmers added to the lists are automatically mapped correctly
                if s1 and isinstance(s1, list) and shimmer_name in s1:
                    shimmer_type = "shimmer1"
                elif s1 and not isinstance(s1, list) and shimmer_name == s1:
                    # Backward compatibility: handle old string format
                    shimmer_type = "shimmer1"
                elif s2 and isinstance(s2, list) and shimmer_name in s2:
                    shimmer_type = "shimmer2"
                elif s2 and not isinstance(s2, list) and shimmer_name == s2:
                    # Backward compatibility: handle old string format
                    shimmer_type = "shimmer2"
                else:
                    # fallback
                    shimmer_type = "shimmer1"

                # Decide new group or same group
                # Use date from current record, not outer scope variable
                rec_date = rec.get("date", "none")
                
                if curr_group is None:
                    group_id += 1
                    curr_group = {
                        "patient": patient,
                        "date": rec_date,  # Use date from current record
                        "device": device,
                        "group_id": f"group{group_id}",
                        "shimmer1": None,
                        "shimmer2": None,
                        "shimmer1_decoded": [],
                        "shimmer2_decoded": [],
                    }
                    curr_group["_last_ts"] = rec["_ts_unix"]  # modified

                else:
                    # Time difference check (supports cross-day) # modified
                    time_ok = abs(rec["_ts_unix"] - curr_group["_last_ts"]) <= GROUP_WINDOW_SECONDS

                    # Check if shimmer slot already taken
                    shimmer_slot_free = (
                        shimmer_type == "shimmer1" and not curr_group["shimmer1"]
                    ) or (
                        shimmer_type == "shimmer2" and not curr_group["shimmer2"]
                    )

                    # Conditions requiring a new group
                    if not time_ok or not shimmer_slot_free:
                        grouped.append(curr_group)
                        group_id += 1
                        curr_group = {
                            "patient": patient,
                            "device": device,
                            "date": rec_date,  # Use date from current record
                            "group_id": f"group{group_id}",
                            "shimmer1": None,
                            "shimmer2": None,
                            "shimmer1_decoded": [],
                            "shimmer2_decoded": [],
                        }

                # Add to group
                if shimmer_type == "shimmer1":
                    curr_group["shimmer1"] = shimmer_name
                    curr_group["shimmer1_decoded"].append(rec)
                else:
                    curr_group["shimmer2"] = shimmer_name
                    curr_group["shimmer2_decoded"].append(rec)

                curr_group["_last_ts"] = rec["_ts_unix"]

            # Append last group
            if curr_group:
                grouped.append(curr_group)

        # Remove helper keys
        for g in grouped:
            g.pop("_last_ts", None)

        return {"data": grouped, "error": None}

    except Exception as e:
        return {"data": [], "error": str(e)}


# Place this endpoint after app = FastAPI() initialization

@app.post("/decode-and-store/")
def decode_and_store(full_file_name: str = Body(..., embed=True)):
    """
    Given a full S3 filename, download, decode header, and store metadata in DynamoDB file table.
    Large decoded arrays and unnecessary raw fields (like headerBytes) are stored in S3 under 'decode/'.
    DynamoDB stores only lightweight metadata + pointer to decode_s3_key.
    """
    print(f"[decode-and-store] Called with full_file_name: {full_file_name}")
    try:
        # ----------- Download file from S3 -----------
        print(f"[decode-and-store] Downloading file from S3: {full_file_name}")
        s3_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=full_file_name)
        file_bytes = s3_obj["Body"].read()
        print(f"[decode-and-store] Downloaded {len(file_bytes)} bytes from S3.")

        # ----------- Parse filename -----------
        def parse_custom_filename(fname):
            parts = fname.split("__")
            device = parts[0] if len(parts) > 0 else "none"
            timestamp = parts[1] if len(parts) > 1 else "none"
            experiment_name = parts[2] if len(parts) > 2 else "none"
            shimmer_field = parts[3] if len(parts) > 3 else "none"
            filename = parts[5] if len(parts) > 5 else "none"

            shimmer_device = shimmer_field
            shimmer_day = "none"
            if shimmer_field != "none" and "-" in shimmer_field:
                shimmer_device, shimmer_day = shimmer_field.rsplit("-", 1)

            ext, part = "", None
            if filename and "." in filename:
                ext = filename.split(".")[-1]
                part = filename.split(".")[0]
            elif filename:
                part = filename

            date, time = "none", "none"
            if timestamp and "_" in timestamp:
                ymd, hms = timestamp.split("_", 1)
                if len(ymd) == 8 and len(hms) == 6:
                    date = f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:8]}"
                    time = f"{hms[:2]}:{hms[2:4]}:{hms[4:6]}"

            return {
                "full_file_name": fname,
                "device": device,
                "timestamp": timestamp,
                "date": date,
                "time": time,
                "experiment_name": experiment_name,
                "shimmer_device": shimmer_device,
                "shimmer_day": shimmer_day,
                "filename": filename,
                "ext": ext,
                "part": part
            }

        meta = parse_custom_filename(full_file_name)
        print(f"[decode-and-store] Parsed filename meta: {meta}")

        # ----------- Patient mapping -----------
        patient = None
        try:
            mapping_table_name = os.getenv("DDB_TABLE")
            if mapping_table_name and meta.get("device"):
                ddb = boto3.resource("dynamodb")
                mapping_table = ddb.Table(mapping_table_name)
                resp = mapping_table.get_item(Key={"device": meta["device"]})
                patient = resp.get("Item", {}).get("patient")
                print(f"[decode-and-store] Patient mapping found: {patient}")
        except Exception as e:
            print(f"[decode-and-store] Error in patient mapping: {e}")
            patient = None
        if patient:
            meta["patient"] = patient

        # ----------- Decode shimmer data -----------
        print(f"[decode-and-store] Decoding shimmer data...")
        decoded = read_shimmer_dat(file_bytes)
        print(f"[decode-and-store] Decoded keys: {list(decoded.keys())}")

        # ----------- Remove unneeded heavy keys (just in case)
        EXCLUDE_KEYS = {
            "timestamp", "headerInfo", "headerBytes", "channelNames", "packetLengthBytes",
            "Accel_LN_X", "Accel_LN_Y", "Accel_LN_Z", "VSenseBatt", 
            "Gyro_X", "Gyro_Y", "Gyro_Z",
            "Accel_WR_X", "Accel_WR_Y", "Accel_WR_Z",
            "Mag_X", "Mag_Y", "Mag_Z",
            "Accel_WR_y"  # in case of typo variant
        }

        # ----------- Separate large vs small data -----------
        large_data, small_data = {}, {}
        for k, v in decoded.items():
            if k in EXCLUDE_KEYS:
                continue
            if isinstance(v, (list, dict)) and len(str(v)) > 2000:
                large_data[k] = v
            else:
                small_data[k] = v

        # Always include sampleRate in both small_data and large_data
        if "sampleRate" in decoded:
            try:
                sr = round(float(decoded["sampleRate"]), 2)
                small_data["sampleRate"] = sr
                large_data["sampleRate"] = sr
            except Exception:
                pass

        print(f"[decode-and-store] Large data keys: {list(large_data.keys())}")
        print(f"[decode-and-store] Small data keys: {list(small_data.keys())}")

        # ----------- Store large data to S3 using presigned URL -----------
        import json
        decode_key = f"decode/{os.path.splitext(full_file_name)[0]}_decoded.json"
        presigned_url = s3_client.generate_presigned_url(
            ClientMethod="put_object",
            Params={"Bucket": S3_BUCKET, "Key": decode_key, "ContentType": "application/json"},
            ExpiresIn=600
        )
        print(f"[decode-and-store] Presigned URL for decoded upload: {presigned_url}")
        try:
            import requests
            resp = requests.put(
                presigned_url,
                data=json.dumps(large_data),
                headers={"Content-Type": "application/json"}
            )
            print(f"[decode-and-store] S3 upload response status: {resp.status_code}")
            if resp.status_code not in [200, 201]:
                print(f"[decode-and-store] S3 upload error: {resp.text}")
                return {"error": f"Failed to upload decoded file to S3: {resp.text}"}
        except ImportError:
            print("[decode-and-store] requests library is not installed.")
            return {"error": "requests library is required for presigned URL upload. Please install it."}

        # ----------- Prepare DynamoDB record -----------
        def convert_floats(obj):
            from decimal import Decimal
            if isinstance(obj, float):
                return Decimal(str(obj))
            if isinstance(obj, dict):
                return {k: convert_floats(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [convert_floats(v) for v in obj]
            return obj

        # Extract recordedTimestamp from first timestampCal value
        recorded_timestamp = None
        if "timestampCal" in decoded and isinstance(decoded["timestampCal"], list) and len(decoded["timestampCal"]) > 0:
            try:
                unix_timestamp = float(decoded["timestampCal"][0])
                dt = datetime.fromtimestamp(unix_timestamp, tz=timezone.utc)
                recorded_timestamp = dt.isoformat()
            except (ValueError, TypeError, OSError):
                recorded_timestamp = None

        merged = {**meta, **small_data, "decode_s3_key": decode_key}
        if recorded_timestamp is not None:
            merged["recordedTimestamp"] = recorded_timestamp
        item = convert_floats(merged)
        item["updatedAt"] = datetime.now(timezone.utc).isoformat()
        print(f"[decode-and-store] DynamoDB item prepared: {item}")

        # ----------- Save to DynamoDB -----------
        file_table_name = os.getenv("DDB_FILE_TABLE")
        if not file_table_name:
            print("[decode-and-store] DDB_FILE_TABLE env not set")
            return {"error": "DDB_FILE_TABLE env not set"}
        ddb = boto3.resource("dynamodb")
        file_table = ddb.Table(file_table_name)
        file_table.put_item(Item=item)
        print(f"[decode-and-store] Item stored in DynamoDB table: {file_table_name}")

        print(f"[decode-and-store] Returning success response.")
        return {
            "filename": full_file_name,
            "message": "Decode and store successful",
            "ddb_item": item,
            "decode_s3_key": decode_key
        }

    except (BotoCoreError, ClientError, Exception) as e:
        print(f"[decode-and-store] Exception: {e}")
        return {"error": str(e)}

@app.get("/get-decoded-field-direct/")
def get_decoded_field_direct(
    full_file_name: str = Query(...),
    field_name: str = Query(...)
):
    """
    Directly fetches the field from 'decode/{filename_without_ext}_decoded.json' in S3,
    skipping DynamoDB lookup.
    """
    try:
        import json, os
        decoded_key = f"decode/{os.path.splitext(full_file_name)[0]}_decoded.json"
        s3_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=decoded_key)
        decoded_data = json.loads(s3_obj["Body"].read().decode("utf-8"))
        if field_name not in decoded_data:
            raise HTTPException(status_code=404, detail=f"Field '{field_name}' not found.")
        return {
            "decode_s3_key": decoded_key,
            "field": field_name,
            "length": len(decoded_data[field_name]),
            "values": decoded_data[field_name]
        }
    except (BotoCoreError, ClientError, Exception) as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/get-decoded-file-url/")
def get_decoded_file_url(full_file_name: str = Query(...)):
    """
    Returns a presigned URL to download the decoded JSON file from S3.
    Takes the original filename and returns download URL for decode/{filename}_decoded.json
    
    IMPORTANT: Use ONLY the "download_url" field from the response. The URL must be used
    exactly as returned - any modification will break the signature.
    """
    try:
        import os
        decoded_key = f"decode/{os.path.splitext(full_file_name)[0]}_decoded.json"
        
        # Verify file exists first
        try:
            s3_client.head_object(Bucket=S3_BUCKET, Key=decoded_key)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                raise HTTPException(status_code=404, detail=f"Decoded file not found: {decoded_key}")
            raise
        
        # Generate presigned URL - use exactly as returned, don't modify!
        url = s3_client.generate_presigned_url(
            ClientMethod="get_object",
            Params={
                "Bucket": S3_BUCKET,
                "Key": decoded_key
            },
            ExpiresIn=3600  # 1 hour
        )
        
        return {"download_url": url, "s3_key": decoded_key}
    except HTTPException:
        raise
    except (BotoCoreError, ClientError, Exception) as e:
        raise HTTPException(status_code=500, detail=str(e))


# Daily aggregator endpoint - can be called manually or scheduled
@app.post("/daily-aggregator/")
def trigger_daily_aggregator(date: Optional[str] = Body(None, embed=True)):
    """
    Trigger daily aggregation for a specific date or automatically process next date.
    Uses recordedTimestamp from DynamoDB to determine actual recording date.
    """
    try:
        from daily_aggregator_handler import (
            lambda_handler,
            get_next_date_to_process
        )
        
        # Determine date to process
        target_date = date
        if not target_date:
            target_date = get_next_date_to_process()
        
        if not target_date:
            return {
                "message": "No date to process (all dates up to today are processed)",
                "last_processed": None
            }
        
        # Create event for lambda handler
        event = {"date": target_date}
        
        # Call the handler
        response = lambda_handler(event, None)
        
        # Parse response body
        body = json.loads(response["body"])
        
        if response["statusCode"] == 200:
            return body
        else:
            raise HTTPException(status_code=response["statusCode"], detail=body)
    
    except ImportError as e:
        raise HTTPException(
            status_code=500,
            detail=f"Could not import daily_aggregator_handler: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class CombinedDataFile(BaseModel):
    date: str
    patient: Optional[str] = None
    shimmer1: Optional[str] = None
    shimmer2: Optional[str] = None
    shimmer1_file: Optional[str] = None
    shimmer2_file: Optional[str] = None


@app.get("/combined-data-files/", response_model=List[CombinedDataFile])
def list_combined_data_files():
    """
    List all combined data files from S3 combinedbyDay/ folder.
    Groups by date and device, shows patient mapping and shimmer assignments.
    """
    try:
        # List files from S3 combinedbyDay/ folder
        prefix = "combinedbyDay/"
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
        contents = response.get("Contents", [])
        
        # Continue pagination if needed
        while response.get("IsTruncated"):
            response = s3_client.list_objects_v2(
                Bucket=S3_BUCKET,
                Prefix=prefix,
                ContinuationToken=response.get("NextContinuationToken")
            )
            contents.extend(response.get("Contents", []))
        
        # Parse filenames: device_date_shimmername_combined.json
        parsed_files = []
        for obj in contents:
            key = obj["Key"]
            filename = os.path.basename(key)
            
            # Skip if not a combined file
            if not filename.endswith("_combined.json"):
                continue
            
            # Parse: device_date_shimmername_combined.json
            # Remove _combined.json
            base = filename.replace("_combined.json", "")
            parts = base.split("_")
            
            if len(parts) >= 3:
                device = parts[0]
                date = parts[1]
                shimmer_name = "_".join(parts[2:])  # Handle shimmer names with underscores
                
                parsed_files.append({
                    "device": device,
                    "date": date,
                    "shimmer_name": shimmer_name,
                    "filename": filename,
                    "s3_key": key
                })
        
        # Load device-patient mapping and shimmer lists
        device_mapping = {}
        shimmer_mapping = {}
        table = _get_ddb_table()
        scan_kwargs = {"ProjectionExpression": "device, patient, shimmer1, shimmer2"}
        while True:
            dresp = table.scan(**scan_kwargs)
            for it in dresp.get("Items", []):
                dev = it.get("device")
                pat = it.get("patient")
                if dev:
                    device_mapping[dev] = pat if (pat is not None and pat != "") else None
                    # Normalize shimmer values to lists
                    shimmer1 = _normalize_shimmer_to_list(it.get("shimmer1"))
                    shimmer2 = _normalize_shimmer_to_list(it.get("shimmer2"))
                    shimmer_mapping[dev] = {
                        "shimmer1": shimmer1,
                        "shimmer2": shimmer2
                    }
            if "LastEvaluatedKey" in dresp:
                scan_kwargs["ExclusiveStartKey"] = dresp["LastEvaluatedKey"]
            else:
                break
        
        # Group by (date, device)
        from collections import defaultdict
        grouped = defaultdict(lambda: {
            "date": None,
            "device": None,
            "patient": None,
            "shimmer1": None,
            "shimmer2": None,
            "shimmer1_file": None,
            "shimmer2_file": None
        })
        
        for file_info in parsed_files:
            device = file_info["device"]
            date = file_info["date"]
            shimmer_name = file_info["shimmer_name"]
            filename = file_info["filename"]
            
            group_key = (date, device)
            group = grouped[group_key]
            
            # Set date and device
            group["date"] = date
            group["device"] = device
            
            # Get patient
            patient = device_mapping.get(device)
            group["patient"] = patient if (patient is not None and patient != "") else None
            
            # Get shimmer mapping for this device
            device_shimmer_map = shimmer_mapping.get(device, {})
            s1_list = device_shimmer_map.get("shimmer1")
            s2_list = device_shimmer_map.get("shimmer2")
            
            # Determine if this shimmer is shimmer1 or shimmer2
            shimmer_type = None
            if shimmer_name != "unknown":
                # Check if shimmer is in shimmer1 list
                if s1_list and isinstance(s1_list, list) and shimmer_name in s1_list:
                    shimmer_type = "shimmer1"
                elif s1_list and not isinstance(s1_list, list) and shimmer_name == s1_list:
                    shimmer_type = "shimmer1"
                # Check if shimmer is in shimmer2 list
                elif s2_list and isinstance(s2_list, list) and shimmer_name in s2_list:
                    shimmer_type = "shimmer2"
                elif s2_list and not isinstance(s2_list, list) and shimmer_name == s2_list:
                    shimmer_type = "shimmer2"
            
            # Assign to shimmer1 or shimmer2
            if shimmer_type == "shimmer1":
                group["shimmer1"] = shimmer_name
                group["shimmer1_file"] = filename
            elif shimmer_type == "shimmer2":
                group["shimmer2"] = shimmer_name
                group["shimmer2_file"] = filename
            else:
                # If not in mapping, assign to first available slot
                if not group["shimmer1"]:
                    group["shimmer1"] = shimmer_name
                    group["shimmer1_file"] = filename
                elif not group["shimmer2"]:
                    group["shimmer2"] = shimmer_name
                    group["shimmer2_file"] = filename
        
        # Convert to response format
        result = []
        for (date, device), group in sorted(grouped.items()):
            result.append(CombinedDataFile(
                date=group["date"],
                patient=group["patient"],
                shimmer1=group["shimmer1"],
                shimmer2=group["shimmer2"],
                shimmer1_file=group["shimmer1_file"],
                shimmer2_file=group["shimmer2_file"]
            ))
        
        return result
    
    except (BotoCoreError, ClientError, Exception) as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/get-combined-data-field/")
def get_combined_data_field(
    filename: str = Query(..., description="Combined file name, e.g., device_date_shimmername_combined.json"),
    field_name: str = Query(..., description="Field name: 'accel_wr_absolute_downsampled' or 'uwb_dis_non_zero_count'")
):
    """
    Get a specific field from a combined data file in combinedbyDay/ folder.
    Similar to get-decoded-field-direct but for combined files.
    """
    try:
        import json
        
        # Ensure filename is in combinedbyDay/ folder
        if not filename.startswith("combinedbyDay/"):
            s3_key = f"combinedbyDay/{filename}"
        else:
            s3_key = filename
        
        # Load combined data from S3
        s3_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        combined_data = json.loads(s3_obj["Body"].read().decode("utf-8"))
        
        if field_name not in combined_data:
            raise HTTPException(
                status_code=404, 
                detail=f"Field '{field_name}' not found. Available fields: {list(combined_data.keys())}"
            )
        
        field_value = combined_data[field_name]
        
        return {
            "s3_key": s3_key,
            "field": field_name,
            "length": len(field_value) if isinstance(field_value, list) else None,
            "values": field_value,  # Return full array (can be large ~58k points)
            "is_array": isinstance(field_value, list)
        }
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            raise HTTPException(status_code=404, detail=f"Combined file not found: {s3_key}")
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/get-combined-data-file/")
def get_combined_data_file(
    filename: str = Query(..., description="Combined file name, e.g., device_date_shimmername_combined.json")
):
    """
    Get the full content of a combined data file from combinedbyDay/ folder.
    Returns presigned URL for large files, or full content for small files.
    """
    try:
        import json
        
        # Ensure filename is in combinedbyDay/ folder
        if not filename.startswith("combinedbyDay/"):
            s3_key = f"combinedbyDay/{filename}"
        else:
            s3_key = filename
        
        # Check file size first
        try:
            head_response = s3_client.head_object(Bucket=S3_BUCKET, Key=s3_key)
            file_size = head_response.get('ContentLength', 0)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                raise HTTPException(status_code=404, detail=f"Combined file not found: {s3_key}")
            raise
        
        # If file is large (>5MB), return presigned URL instead
        if file_size > 5 * 1024 * 1024:  # 5MB
            url = s3_client.generate_presigned_url(
                ClientMethod="get_object",
                Params={"Bucket": S3_BUCKET, "Key": s3_key},
                ExpiresIn=3600
            )
            return {
                "s3_key": s3_key,
                "file_size": file_size,
                "download_url": url,
                "note": "File is large, use download_url to fetch"
            }
        
        # For smaller files, return full content
        s3_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        combined_data = json.loads(s3_obj["Body"].read().decode("utf-8"))
        
        return {
            "s3_key": s3_key,
            "file_size": file_size,
            "data": combined_data
        }
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            raise HTTPException(status_code=404, detail=f"Combined file not found: {s3_key}")
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))