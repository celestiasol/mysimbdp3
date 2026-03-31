FROM python:3.11-slim
WORKDIR /app
COPY streamanalyticsapp/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY streamanalyticsapp/app /app/app
CMD ["python", "-u", "app/main.py"]
