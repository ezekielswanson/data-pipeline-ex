FROM python:3.13
WORKDIR /app
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir uv --root-user-action=ignore
COPY pyproject.toml README.md ./
COPY src/ ./src/
RUN uv pip install --system .
ENTRYPOINT ["migrator"]