FROM python:3.14-alpine

LABEL org.opencontainers.image.title="renta-es-crypt"
LABEL org.opencontainers.image.description="CLI tool to calculate Spanish crypto capital gains and losses using FIFO."

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN adduser -D worker
WORKDIR /app

COPY pyproject.toml README.md LICENSE /app/
COPY src /app/src

RUN python -m pip install --no-cache-dir .

USER worker

ENTRYPOINT ["renta-es-crypt"]
