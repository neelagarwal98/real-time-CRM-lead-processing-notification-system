import json
import boto3
import datetime
import os

s3 = boto3.client('s3')
bucket_name = "crm-webhook-events"

sqs = boto3.client('sqs')
queue_url = os.environ['SQS_QUEUE_URL']

def lambda_handler(event, context):
    try:
        if "body" in event:
            body = json.loads(event["body"])  
        else:
            body = event 

        lead_id = (
            body.get("event", {}).get("lead_id")
            or body.get("event", {}).get("data", {}).get("id")
            # or "unknown"
        )

        # timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
        # key = f"crm_event_{lead_id}_{timestamp}.json"
        key = f"crm_event_{lead_id}.json"

        s3.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=json.dumps(body, indent=2),
            ContentType="application/json"
        )

        try:

            response = sqs.send_message(
                QueueUrl=queue_url,
                DelaySeconds=0,
                MessageBody=json.dumps({
                    "lead_id": lead_id,
                    "s3_key": key
                })
            )
            print(f"✅ SQS send_message response: {response}")

        except Exception as e:
            print(f"❌ Failed to send to SQS: {e}")

        return {
            "statusCode": 200,
            "body": json.dumps({"message": f"Stored webhook for lead {lead_id} in S3 and queued for delayed processing."})
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
