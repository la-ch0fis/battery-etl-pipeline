import json
import os
import boto3
from datetime import datetime, timezone

sfn = boto3.client("stepfunctions")

STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]
RAW_BUCKET = os.environ["RAW_BUCKET"]
PROCESSED_BUCKET = os.environ["PROCESSED_BUCKET"]
ICEBERG_BUCKET = os.environ["ICEBERG_BUCKET"]


def handler(event: dict, context: object) -> dict:
    execution_name = f"etl-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"

    # Callers (EventBridge or manual) can override bucket names via the event payload
    payload = {
        "raw_bucket": event.get("raw_bucket", RAW_BUCKET),
        "processed_bucket": event.get("processed_bucket", PROCESSED_BUCKET),
        "iceberg_bucket": event.get("iceberg_bucket", ICEBERG_BUCKET),
    }

    response = sfn.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        name=execution_name,
        input=json.dumps(payload),
    )

    print(f"Started execution: {response['executionArn']}")

    return {
        "statusCode": 200,
        "executionArn": response["executionArn"],
        "startDate": response["startDate"].isoformat(),
    }
