FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    APP_ADMIN_USER=admin \
    APP_ADMIN_PASS=admin \
    CONSENSUS_AUTO_FETCH=1

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

EXPOSE 8000

CMD ["python", "-m", "webapp.app"]
