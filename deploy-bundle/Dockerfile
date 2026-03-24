FROM python:3.12-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV GROUP_FILM_NIGHT_DIR=/app

COPY app.py /app/app.py
COPY group-state.json /app/group-state.json
COPY dashboard /app/dashboard

EXPOSE 8048
CMD ["sh", "-c", "python app.py --host 0.0.0.0 --port ${PORT:-8048}"]
