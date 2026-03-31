FROM python:3.11-slim
WORKDIR /app
COPY tenant_listener/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY tenant_listener/app /app/app
CMD ["python", "-u", "app/main.py"]
