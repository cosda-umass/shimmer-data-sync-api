"""
Daily Aggregator Handler - Can be used as a separate Lambda handler
or called from the main FastAPI app.

Uses the same dependencies as main.py (boto3, etc.)
Uses recordedTimestamp from DynamoDB to determine actual recording date.
"""

import os
import json
import boto3
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional
from botocore.exceptions import ClientError
import statistics

# Use the same environment variables as main.py
S3_BUCKET = os.getenv("S3_BUCKET")
DDB_FILE_TABLE = os.getenv("DDB_FILE_TABLE", "DecodedFileMeta")
DDB_STATE_TABLE = os.getenv("DDB_STATE_TABLE", "daily-aggregator-state")
DDB_OUTPUT_TABLE = os.getenv("DDB_OUTPUT_TABLE", "daily-aggregated-data")
OUTPUT_S3_PREFIX = os.getenv("OUTPUT_S3_PREFIX", "combinedbyDay/")

# Initialize AWS clients (same as main.py)
s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

# Constants
DOWNSAMPLE_CHUNK_SIZE = 50  # Average every 50 points into 1


def get_last_processed_date() -> Optional[str]:
    """Get the last processed date from DynamoDB state table."""
    try:
        table = dynamodb.Table(DDB_STATE_TABLE)
        response = table.get_item(Key={"id": "last_processed_date"})
        if "Item" in response:
            return response["Item"].get("date")
    except ClientError as e:
        print(f"Error reading state table: {e}")
    return None


def set_last_processed_date(date: str):
    """Update the last processed date in DynamoDB."""
    try:
        table = dynamodb.Table(DDB_STATE_TABLE)
        table.put_item(
            Item={
                "id": "last_processed_date",
                "date": date,
                "updatedAt": datetime.now(timezone.utc).isoformat()
            }
        )
    except ClientError as e:
        print(f"Error updating state table: {e}")
        raise


def extract_date_from_recorded_timestamp(recorded_ts: str) -> Optional[str]:
    """
    Extract date (YYYY-MM-DD) from recordedTimestamp.
    Handles multiple formats:
    - ISO format: "2024-09-24T22:38:36+00:00" or "2024-09-24T22:38:36Z"
    - US format: "12/11/2024, 10:02:13 PM" or "12/11/2024, 10:02:13 AM"
    """
    if not recorded_ts:
        return None
    
    # Try ISO format first (expected format from main.py)
    try:
        # Normalize Z to +00:00 for fromisoformat
        normalized_ts = recorded_ts.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized_ts)
        return dt.strftime("%Y-%m-%d")
    except (ValueError, AttributeError):
        pass
    
    # Try US format: "MM/DD/YYYY, HH:MM:SS AM/PM"
    try:
        # Handle formats like "12/11/2024, 10:02:13 PM"
        dt = datetime.strptime(recorded_ts, "%m/%d/%Y, %I:%M:%S %p")
        return dt.strftime("%Y-%m-%d")
    except (ValueError, AttributeError):
        pass
    
    # Try US format without seconds: "MM/DD/YYYY, HH:MM AM/PM"
    try:
        dt = datetime.strptime(recorded_ts, "%m/%d/%Y, %I:%M %p")
        return dt.strftime("%Y-%m-%d")
    except (ValueError, AttributeError):
        pass
    
    # Try simple US date format: "MM/DD/YYYY"
    try:
        dt = datetime.strptime(recorded_ts, "%m/%d/%Y")
        return dt.strftime("%Y-%m-%d")
    except (ValueError, AttributeError):
        pass
    
    print(f"Error parsing recordedTimestamp '{recorded_ts}': Unsupported format")
    return None


def get_files_for_date(target_date: str) -> List[Dict[str, Any]]:
    """
    Query DynamoDB DecodedFileMeta table to find all files where
    recordedTimestamp date matches the target date.
    """
    files = []
    try:
        table = dynamodb.Table(DDB_FILE_TABLE)
        
        # Scan table and filter by date extracted from recordedTimestamp
        # Use ExpressionAttributeNames to handle reserved keyword "date"
        scan_kwargs = {
            "ProjectionExpression": "full_file_name, decode_s3_key, recordedTimestamp, #date, device, experiment_name, shimmer_device",
            "ExpressionAttributeNames": {
                "#date": "date"
            }
        }
        
        print(f"Scanning DynamoDB table {DDB_FILE_TABLE} for date {target_date}...")
        scanned_count = 0
        
        while True:
            response = table.scan(**scan_kwargs)
            scanned_count += response.get("Count", 0)
            
            # Filter items by date extracted from recordedTimestamp
            for item in response.get("Items", []):
                recorded_ts = item.get("recordedTimestamp")
                
                if recorded_ts:
                    # Extract date from recordedTimestamp
                    item_date = extract_date_from_recorded_timestamp(recorded_ts)
                    if item_date == target_date:
                        files.append(item)
                else:
                    # Fallback: use date field if recordedTimestamp is missing
                    item_date = item.get("date")
                    if item_date == target_date:
                        print(f"Warning: Using fallback date field for {item.get('full_file_name')}")
                        files.append(item)
            
            if "LastEvaluatedKey" in response:
                scan_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]
            else:
                break
        
        print(f"Scanned {scanned_count} items, found {len(files)} files for date {target_date}")
        return files
    
    except ClientError as e:
        print(f"Error querying DynamoDB: {e}")
        raise


def load_decoded_data_from_s3(decode_s3_key: str) -> Optional[Dict[str, Any]]:
    """Load decoded JSON data from S3 using the decode_s3_key."""
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=decode_s3_key)
        data = json.loads(response["Body"].read().decode("utf-8"))
        return data
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            print(f"Decoded file not found in S3: {decode_s3_key}")
            return None
        print(f"Error loading decoded file {decode_s3_key}: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON from {decode_s3_key}: {e}")
        return None


def downsample_by_chunk_size(data: List[float], chunk_size: int) -> List[float]:
    """
    Downsample data by averaging every N consecutive points into 1.
    Example: [1-50] -> avg1, [51-100] -> avg2, [101-150] -> avg3, etc.
    Result will have approximately len(data) / chunk_size points.
    """
    if not data or chunk_size <= 0:
        return []
    
    if len(data) <= chunk_size:
        # If data is smaller than chunk size, just return average of all
        return [statistics.mean(data)]
    
    result = []
    
    # Process in chunks of chunk_size
    for i in range(0, len(data), chunk_size):
        chunk = data[i:i + chunk_size]
        if chunk:
            result.append(statistics.mean(chunk))
    
    return result


def count_non_zero_uwb(uwb_data: List[float]) -> int:
    """Count total number of non-zero values in uwbDis."""
    if not uwb_data:
        return 0
    return sum(1 for v in uwb_data if v != 0)


def aggregate_daily_data_by_shimmer(target_date: str, shimmer_device: str, device: str, file_records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Aggregate all hourly files for a given date and shimmer device.
    Returns aggregated results for this specific shimmer.
    """
    print(f"Processing date: {target_date}, device: {device}, shimmer: {shimmer_device}")
    
    # Filter files for this specific shimmer device
    shimmer_files = [
        f for f in file_records 
        if f.get("device") == device and f.get("shimmer_device") == shimmer_device
    ]
    
    if not shimmer_files:
        return {
            "date": target_date,
            "device": device,
            "shimmer_device": shimmer_device,
            "error": f"No files found for this shimmer device",
            "files_processed": 0
        }
    
    print(f"Found {len(shimmer_files)} files for {device}/{shimmer_device} on {target_date}")
    
    # Collect data from all files for this shimmer
    all_accel_wr = []
    total_uwb_non_zero = 0
    files_processed = 0
    files_failed = []
    
    for file_record in shimmer_files:
        decode_s3_key = file_record.get("decode_s3_key")
        full_file_name = file_record.get("full_file_name", "unknown")
        recorded_ts = file_record.get("recordedTimestamp", "unknown")
        
        # If decode_s3_key is not present, construct it from full_file_name
        if not decode_s3_key:
            # Construct decode key: decode/{filename_without_ext}_decoded.json
            base_name = full_file_name.rsplit(".", 1)[0] if "." in full_file_name else full_file_name
            decode_s3_key = f"decode/{base_name}_decoded.json"
            print(f"Constructed decode_s3_key from full_file_name: {decode_s3_key}")
        
        if not decode_s3_key:
            print(f"Warning: Could not determine decode_s3_key for file {full_file_name}")
            files_failed.append(full_file_name)
            continue
        
        # Load decoded data from S3
        decoded_data = load_decoded_data_from_s3(decode_s3_key)
        if not decoded_data:
            files_failed.append(full_file_name)
            continue
        
        print(f"Processing: {full_file_name} (recorded: {recorded_ts})")
        
        # Extract Accel_WR_Absolute
        if "Accel_WR_Absolute" in decoded_data:
            accel_data = decoded_data["Accel_WR_Absolute"]
            if isinstance(accel_data, list):
                count = len(accel_data)
                all_accel_wr.extend([float(v) for v in accel_data])
                print(f"  Accel_WR_Absolute samples: {count}")
            else:
                print(f"  Warning: Accel_WR_Absolute is not a list")
        else:
            print(f"  Warning: Accel_WR_Absolute not found in file")
        
        # Count non-zero uwbDis
        if "uwbDis" in decoded_data:
            uwb_data = decoded_data["uwbDis"]
            if isinstance(uwb_data, list):
                count = count_non_zero_uwb([float(v) for v in uwb_data])
                total_uwb_non_zero += count
                print(f"  uwbDis non-zero count: {count} (total samples: {len(uwb_data)})")
            else:
                print(f"  Warning: uwbDis is not a list")
        else:
            print(f"  Warning: uwbDis not found in file")
        
        files_processed += 1
    
    print(f"\nSummary:")
    print(f"  Files processed: {files_processed}")
    print(f"  Files failed: {len(files_failed)}")
    print(f"  Total Accel_WR_Absolute points: {len(all_accel_wr)}")
    print(f"  Total uwbDis non-zero count: {total_uwb_non_zero}")
    
    # Downsample Accel_WR_Absolute: average every 50 points into 1
    accel_downsampled = downsample_by_chunk_size(all_accel_wr, DOWNSAMPLE_CHUNK_SIZE)
    
    result = {
        "date": target_date,
        "device": device,
        "shimmer_device": shimmer_device,
        "files_processed": files_processed,
        "files_total": len(shimmer_files),
        "files_failed": len(files_failed),
        "accel_wr_absolute": {
            "original_count": len(all_accel_wr),
            "downsampled": accel_downsampled,
            "downsampled_count": len(accel_downsampled)
        },
        "uwb_dis": {
            "non_zero_count": total_uwb_non_zero
        },
        "processed_at": datetime.now(timezone.utc).isoformat()
    }
    
    if files_failed:
        result["files_failed_list"] = files_failed[:10]  # Limit to first 10 for brevity
    
    return result


def save_aggregated_data(result: Dict[str, Any]):
    """Save aggregated data to S3 with format: device_date_shimmername_combined.json"""
    date = result["date"]
    device = result.get("device", "unknown")
    shimmer_device = result.get("shimmer_device", "unknown")
    
    # Clean shimmer device name for filename (remove special characters)
    shimmer_clean = shimmer_device.replace("/", "_").replace(" ", "_")
    
    # Save simplified combined data
    # Structure: device_date_shimmername_combined.json
    combined_data = {
        "date": date,
        "device": device,
        "shimmer_device": shimmer_device,
        "accel_wr_absolute_downsampled": result.get("accel_wr_absolute", {}).get("downsampled", []),
        "uwb_dis_non_zero_count": result.get("uwb_dis", {}).get("non_zero_count", 0),
        "processed_at": result.get("processed_at")
    }
    
    s3_key = f"{OUTPUT_S3_PREFIX}{device}_{date}_{shimmer_clean}_combined.json"
    
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json.dumps(combined_data, indent=2),
            ContentType="application/json"
        )
        print(f"Saved combined data to S3: {s3_key}")
    except ClientError as e:
        print(f"Error saving to S3: {e}")
        raise
    
    # Optionally save full result for debugging
    full_result_key = f"daily-aggregated/{date}_full.json"
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=full_result_key,
            Body=json.dumps(result, indent=2),
            ContentType="application/json"
        )
        print(f"Saved full result to S3: {full_result_key}")
    except ClientError as e:
        print(f"Warning: Could not save full result: {e}")
    
    # Optionally save summary to DynamoDB
    if DDB_OUTPUT_TABLE:
        try:
            table = dynamodb.Table(DDB_OUTPUT_TABLE)
            summary = {
                "date": date,
                "files_processed": result.get("files_processed", 0),
                "accel_original_count": result.get("accel_wr_absolute", {}).get("original_count", 0),
                "accel_downsampled_count": result.get("accel_wr_absolute", {}).get("downsampled_count", 0),
                "uwb_non_zero_count": result.get("uwb_dis", {}).get("non_zero_count", 0),
                "combined_s3_key": s3_key,
                "processed_at": result.get("processed_at")
            }
            table.put_item(Item=summary)
            print(f"Saved summary to DynamoDB: {DDB_OUTPUT_TABLE}")
        except ClientError as e:
            print(f"Warning: Could not save to DynamoDB: {e}")
            # Don't fail if DynamoDB write fails


def get_next_date_to_process() -> Optional[str]:
    """
    Determine the next date to process.
    Returns the date after the last processed date, or yesterday if no last date exists.
    """
    last_date = get_last_processed_date()
    
    if last_date:
        # Process the day after the last processed date
        try:
            last_dt = datetime.strptime(last_date, "%Y-%m-%d")
            next_dt = last_dt + timedelta(days=1)
            # Don't process future dates
            today = datetime.now(timezone.utc).date()
            if next_dt.date() >= today:
                return None
            return next_dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
    
    # If no last date, process yesterday
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d")


def lambda_handler(event, context):
    """
    Lambda handler for scheduled execution.
    Can be used as a separate Lambda function or called from main.py.
    """
    print("Daily aggregator handler started")
    
    if not S3_BUCKET:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "S3_BUCKET environment variable not set"})
        }
    
    if not DDB_FILE_TABLE:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "DDB_FILE_TABLE environment variable not set"})
        }
    
    try:
        # Determine which date to process
        # Allow override via event
        target_date = event.get("date")
        if not target_date:
            target_date = get_next_date_to_process()
        
        if not target_date:
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "No date to process (all dates up to today are processed)",
                    "last_processed": get_last_processed_date()
                })
            }
        
        # Get all files for this date
        file_records = get_files_for_date(target_date)
        
        if not file_records:
            return {
                "statusCode": 404,
                "body": json.dumps({
                    "error": f"No files found for date {target_date}",
                    "date": target_date
                })
            }
        
        # Group files by (device, shimmer_device)
        from collections import defaultdict
        grouped = defaultdict(list)
        for file_record in file_records:
            device = file_record.get("device", "unknown")
            shimmer_device = file_record.get("shimmer_device", "unknown")
            # Extract shimmer_device from full_file_name if not in record
            if shimmer_device == "unknown" and file_record.get("full_file_name"):
                # Parse from filename: device__timestamp__experiment__Shimmer_XXX-YYY__filename
                parts = file_record["full_file_name"].split("__")
                if len(parts) >= 4:
                    shimmer_field = parts[3]
                    if "-" in shimmer_field:
                        shimmer_device = shimmer_field.rsplit("-", 1)[0]
                    else:
                        shimmer_device = shimmer_field
            
            grouped[(device, shimmer_device)].append(file_record)
        
        # Process each shimmer group separately
        results = []
        for (device, shimmer_device), files in grouped.items():
            result = aggregate_daily_data_by_shimmer(target_date, shimmer_device, device, file_records)
            
            if "error" not in result:
                # Save results for this shimmer
                save_aggregated_data(result)
                shimmer_clean = shimmer_device.replace("/", "_").replace(" ", "_")
                results.append({
                    "device": device,
                    "shimmer_device": shimmer_device,
                    "files_processed": result["files_processed"],
                    "accel_downsampled_count": result["accel_wr_absolute"]["downsampled_count"],
                    "uwb_non_zero_count": result["uwb_dis"]["non_zero_count"],
                    "s3_key": f"{OUTPUT_S3_PREFIX}{device}_{target_date}_{shimmer_clean}_combined.json"
                })
            else:
                results.append({
                    "device": device,
                    "shimmer_device": shimmer_device,
                    "error": result.get("error")
                })
        
        # Update last processed date
        set_last_processed_date(target_date)
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Daily aggregation completed successfully",
                "date": target_date,
                "shimmer_groups_processed": len(results),
                "results": results
            })
        }
    
    except Exception as e:
        print(f"Error in lambda_handler: {e}")
        import traceback
        traceback.print_exc()
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": str(e),
                "traceback": traceback.format_exc()
            })
        }

