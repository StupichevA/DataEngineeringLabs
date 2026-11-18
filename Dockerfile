FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir pandas boto3
COPY mapper.py reducer.py s3_utils.py mr_lib.py profiler.py run_single.py mr_k8s.py ./