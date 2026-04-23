FROM python:3.12-slim-bookworm

# The installer requires curl (and certificates) to download the release archive
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates

# Download the latest installer
ADD https://astral.sh/uv/install.sh /uv-installer.sh

# Run the installer then remove it
RUN sh /uv-installer.sh && rm /uv-installer.sh

# Ensure the installed binary is on the `PATH`
ENV PATH="/root/.local/bin/:$PATH"

# copy source code
COPY fairscape_server/mds/src/ /fairscape/src/
COPY fairscape_graph_tools/ /fairscape/fairscape_graph_tools/
WORKDIR /fairscape/src/

COPY fairscape_server/pyproject.toml .
COPY fairscape_server/uv.lock .
RUN uv sync --locked


#RUN export PYTHONPATH="$PYTHONPATH:/fairscape/src"
ENV PYTHONPATH="/fairscape/src"
ENV PATH="/fairscape/src/.venv/bin:$PATH"

WORKDIR /fairscape/src/

# run using uvicorn
CMD ["uv", "run", "uvicorn", "fairscape_mds.main:app","--no-access-log", "--host", "0.0.0.0", "--port", "8080"]
