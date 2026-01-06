import urllib3
import json

def lambda_handler(event, context):
    print("Lambda function started")
    
    try:
        # Create HTTP client with longer timeout
        # Set to Lambda's max timeout (900 seconds = 15 minutes)
        # But API Gateway will timeout at 30 seconds, so we need to handle that
        http = urllib3.PoolManager(
            timeout=urllib3.Timeout(connect=30.0, read=900.0)
        )
        
        print("Making POST request to backfill endpoint...")
        
        # Call the new backfill endpoint with optional limit
        # Limit to 5 dates per run to avoid timeout issues
        request_body = json.dumps({"limit": 5})
        
        response = http.request(
            'POST',
            'https://odb777ddnc.execute-api.us-east-2.amazonaws.com/daily-aggregator/backfill/',
            headers={
                'accept': 'application/json',
                'Content-Type': 'application/json'
            },
            body=request_body
        )
        
        response_data = response.data.decode('utf-8')
        print(f"API Response Status: {response.status}")
        print(f"API Response Body: {response_data}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Successfully called daily aggregator backfill API',
                'api_http_status': response.status,
                'api_response': json.loads(response_data)
            })
        }
        
    except urllib3.exceptions.ReadTimeoutError:
        # API Gateway timed out (30 second limit), but processing may continue
        print("API Gateway timeout - processing may still be running in background")
        return {
            'statusCode': 202,  # Accepted (processing)
            'body': json.dumps({
                'message': 'Request accepted - processing in progress',
                'note': 'API Gateway timeout reached, but backfill may still be processing. Check S3 combinedbyDay/ folder for results.'
            })
        }
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Failed to call API'
            })
        }