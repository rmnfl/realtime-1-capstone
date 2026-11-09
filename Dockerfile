FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:0.12.3 /uv /usr/local/bin/uv

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src/api/ ./src/api/
USER 65532:65532
EXPOSE 8000
CMD ["/app/.venv/bin/uvicorn", "api.app:app", "--app-dir", "src", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
