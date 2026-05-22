SHELL := /bin/bash
.PHONY: all deploy upload-scripts upload-data trigger validate clean

PROJECT   ?= battery-etl
ENV       ?= dev
REGION    ?= us-east-1
STACK     ?= $(PROJECT)-$(ENV)

ACCOUNT_ID     := $(shell aws sts get-caller-identity --query Account --output text)
SCRIPTS_BUCKET := $(PROJECT)-scripts-$(ACCOUNT_ID)
RAW_BUCKET     := $(PROJECT)-raw-$(ACCOUNT_ID)
SAM_BUCKET     := $(PROJECT)-sam-artifacts-$(ACCOUNT_ID)

# ── Targets ──────────────────────────────────────────────────────────────────

all: deploy upload-scripts upload-data

# Create the SAM artifacts bucket if it doesn't exist (required by sam deploy)
bootstrap:
	@echo "Creating SAM artifacts bucket if needed..."
	@aws s3api head-bucket --bucket $(SAM_BUCKET) 2>/dev/null || \
		aws s3api create-bucket \
			--bucket $(SAM_BUCKET) \
			--region $(REGION) \
			$$([ "$(REGION)" != "us-east-1" ] && echo "--create-bucket-configuration LocationConstraint=$(REGION)" || echo "")
	@aws s3api put-bucket-encryption --bucket $(SAM_BUCKET) \
		--server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
	@echo "SAM artifacts bucket ready: $(SAM_BUCKET)"

# Build and deploy the SAM stack (creates all S3 buckets, IAM roles, Glue jobs, Lambda, Step Functions)
deploy: bootstrap
	@echo "Building SAM application..."
	sam build
	@echo "Deploying stack $(STACK) to $(REGION)..."
	sam deploy \
		--stack-name $(STACK) \
		--s3-bucket $(SAM_BUCKET) \
		--region $(REGION) \
		--capabilities CAPABILITY_NAMED_IAM \
		--parameter-overrides \
			ProjectName=$(PROJECT) \
			Environment=$(ENV) \
		--no-fail-on-empty-changeset
	@echo "Stack deployed."

# Upload Glue PySpark scripts to the scripts bucket (run AFTER deploy)
upload-scripts:
	@echo "Uploading Glue scripts to s3://$(SCRIPTS_BUCKET)/scripts/"
	aws s3 cp src/glue/etl_job.py     s3://$(SCRIPTS_BUCKET)/scripts/etl_job.py
	aws s3 cp src/glue/iceberg_job.py s3://$(SCRIPTS_BUCKET)/scripts/iceberg_job.py
	@echo "Scripts uploaded."

# Upload raw data files to the raw data bucket (run AFTER deploy)
upload-data:
	@echo "Uploading data files to s3://$(RAW_BUCKET)/"
	aws s3 cp battery14_df.csv s3://$(RAW_BUCKET)/battery14_df.csv
	aws s3 cp degrees.csv      s3://$(RAW_BUCKET)/degrees.csv
	@echo "Data files uploaded."

# Manually trigger the Lambda (bypasses EventBridge schedule for immediate testing)
trigger:
	$(eval FUNCTION_NAME := $(shell aws cloudformation describe-stacks \
		--stack-name $(STACK) --region $(REGION) \
		--query "Stacks[0].Outputs[?OutputKey=='StartETLFunctionArn'].OutputValue" \
		--output text | awk -F: '{print $$NF}'))
	@echo "Invoking Lambda: $(FUNCTION_NAME)"
	aws lambda invoke \
		--function-name $(FUNCTION_NAME) \
		--region $(REGION) \
		--payload '{}' \
		--cli-binary-format raw-in-base64-out \
		/tmp/lambda_response.json
	@cat /tmp/lambda_response.json | python3 -m json.tool

# Query the Iceberg table via Athena (Athena engine v3 required)
validate:
	$(eval DB := battery_etl_db)
	$(eval TABLE := battery_results)
	$(eval OUTPUT := s3://$(RAW_BUCKET)/athena-results/)
	@echo "Running Athena query on $(DB).$(TABLE)..."
	aws athena start-query-execution \
		--query-string "SELECT user_id, age, gender, description, raw_score FROM \"$(DB)\".\"$(TABLE)\" LIMIT 10;" \
		--query-execution-context Database=$(DB) \
		--result-configuration OutputLocation=$(OUTPUT) \
		--region $(REGION) \
		--work-group primary

# Tear down the full stack
clean:
	@echo "WARNING: This will delete the CloudFormation stack and ALL resources."
	@read -p "Type 'yes' to confirm: " ans && [ "$$ans" = "yes" ]
	aws cloudformation delete-stack --stack-name $(STACK) --region $(REGION)
	@echo "Stack deletion initiated."
