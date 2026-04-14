# ── Python Cognitive Node — Docker Build ──────────────────────────────────────
FROM python:3.12-slim-bookworm

WORKDIR /app

# System deps for psycopg2 native build (if needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps first (cached layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY . .

CMD ["python", "-u", "agent_q_loop.py"]
