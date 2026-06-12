# Financial News Tool — application image (pipeline + Streamlit dashboard).
FROM python:3.12-slim

# Lean, predictable Python in containers.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install dependencies first so this layer is cached unless requirements change.
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# spaCy model used by nlp_processor.py (baked into the image, so no runtime download).
RUN python -m spacy download en_core_web_sm

# Copy the application code.
COPY . .

# Run as a non-root user (avoids container-root footguns).
RUN useradd --create-home appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8501

# Default command runs the dashboard. --server.headless skips the email/telemetry
# prompt that the interactive `streamlit run` shows. The scheduler service in
# docker-compose.yml overrides this command.
CMD ["streamlit", "run", "app.py", \
     "--server.address=0.0.0.0", \
     "--server.port=8501", \
     "--server.headless=true", \
     "--browser.gatherUsageStats=false"]
