import boto3
import json
import urllib.request
import os
import time
import urllib.error

s3 = boto3.client('s3')

RAW_BUCKET = os.environ.get("RAW_BUCKET", "crm-webhook-events")
SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK")

def fetch_with_retries(url, retries=10, backoff=2):
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(url) as response:
                return json.loads(response.read())
        except urllib.error.HTTPError as e:
            if e.code == 403:
                print(f"⚠️ Attempt {attempt}: Got 403. Retrying in {backoff}s...")
                time.sleep(backoff)
                backoff *= 2
            else:
                raise e
        except Exception as e:
            print(f"❌ Attempt {attempt}: Other error: {e}")
            time.sleep(backoff)
            backoff *= 2
    raise Exception(f"❌ Failed to fetch URL after {retries} retries: {url}")

def send_slack_alert(enriched):
    slack_message = {
        "text": (
            f"🚨 *New Lead Alert*\n"
            f"*Name:* {enriched.get('display_name', 'N/A')}\n"
            f"*Lead ID:* {enriched.get('lead_id', 'N/A')}\n"
            f"*Created Date:* {enriched.get('date_created', 'N/A')}\n"
            f"*Label:* {enriched.get('status_label', 'N/A')}\n"
            f"*Email:* {enriched.get('lead_email', 'N/A')}\n"
            f"*Lead Owner:* {enriched.get('lead_owner', 'N/A')}\n"
            f"*Funnel:* {enriched.get('funnel', 'N/A')}"
        )
    }

    req = urllib.request.Request(
        SLACK_WEBHOOK,
        data=json.dumps(slack_message).encode("utf-8"),
        headers={"Content-Type": "application/json"}
    )
    urllib.request.urlopen(req)


def lambda_handler(event, context):
    for record in event['Records']:
        try:
            # receiving SQS message
            message = json.loads(record['body'])
            lead_id = message['lead_id']
            s3_key = message['s3_key']

            # fetching raw webhook data from s3 bucket
            raw_obj = s3.get_object(Bucket = RAW_BUCKET, Key = s3_key)
            raw_data = json.loads(raw_obj['Body'].read())

            # fetching lead owner details from public s3
            public_bucket = 'dea-lead-owner'
            file_name = f"{lead_id}.json"
            public_url = f"https://{public_bucket}.s3.us-east-1.amazonaws.com/{file_name}"
            print(file_name)
            print(public_url)

            # with urllib.request.urlopen(public_url) as response:
            #     owner_data = json.loads(response.read())

            owner_data = fetch_with_retries(public_url)

            # obj = s3.get_object(Bucket = public_bucket, Key = file_name)
            # owner_data = json.loads(obj['Body'].read())

            # merging lead owner details with raw data (enriched lead keys will take priority)
            updated_data = {**raw_data, "enriched_lead": owner_data}

            # storing the updated data to 'enriched/' folder in raw s3 bucket
            processed_key = f"enriched/{s3_key}"
            s3.put_object(
                Bucket = RAW_BUCKET,
                Key = processed_key,
                Body = json.dumps(updated_data, indent = 2),
                ContentType = 'application/json'
            )

            # Extract Slack-relevant fields
            event_data = raw_data.get("event", {}).get("data", {})

            slack_payload = {
                **owner_data,
                "lead_id": lead_id,
                "display_name": event_data.get("display_name"),
                "status_label": event_data.get("status_label"),
                "date_created": event_data.get("date_created")
            }
            
            send_slack_alert(slack_payload)

            print(f"✅ Successfully processed, saved and alerted for {processed_key}")

        except Exception as e:
            print(f"❌ Error processing message: {e}")

    return {"statusCode": 200}
