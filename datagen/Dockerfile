# Lakebench Data Generator
# Produces synthetic Customer360 data for lakehouse benchmarking
# Supports batch and continuous generation modes
#
# Usage (CLI args):
#   python generate.py --target-tb 1 --mode batch --bucket lb-bronze
#   python generate.py --target-tb 10 --mode continuous --bucket lb-bronze
#
# S3 credentials from environment:
#   AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_ENDPOINT
# Node ID from K8s Indexed Job:
#   JOB_COMPLETION_INDEX (auto-set)

FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy generation code
COPY generate.py .

ENTRYPOINT ["python", "generate.py"]
