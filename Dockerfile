FROM python:3.12-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# copy source code
COPY mds/src/ /fairscape/src/
WORKDIR /fairscape/src/

COPY pyproject.toml .
COPY uv.lock .
RUN uv sync --locked


#RUN export PYTHONPATH="$PYTHONPATH:/fairscape/src"
ENV PYTHONPATH="/fairscape/src"
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /fairscape/src/

# run using uvicorn
CMD ["uv", "run", "uvicorn", "fairscape_mds.main:app", "--host", "0.0.0.0", "--port", "8080"]
