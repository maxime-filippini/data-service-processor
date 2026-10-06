FROM python:3.13-slim
COPY --from=ghcr.io/astral-sh/uv:0.9.21 /uv /usr/local/bin/uv
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 UV_COMPILE_BYTECODE=1
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --locked --no-dev --no-install-project
COPY src ./src
RUN uv sync --locked --no-dev && useradd --uid 10001 --create-home processor
USER processor
ENV PATH="/app/.venv/bin:$PATH"
EXPOSE 8080
CMD ["uvicorn", "data_service_processor.app:app", "--host", "0.0.0.0", "--port", "8080"]
