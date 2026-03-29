FROM python:3.13.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /app

# Add virtual environment to PATH so we can use installed packages
ENV PATH="/app/.venv/bin:$PATH"

# Copy dependency files first (better layer caching)
COPY "pyproject.toml" "uv.lock" ".python-version" ./

RUN uv sync --locked

#RUN pip install pandas pyarrow

#WORKDIR /code
COPY pipeline.py .
 
ENTRYPOINT [ "uv" , "run", "python","pipeline.py"]