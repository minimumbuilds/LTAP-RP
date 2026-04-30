FROM python:3.12-slim

WORKDIR /app

# Install LTAP-SDK from sibling directory (build context is the parent dir)
COPY LTAP-SDK /ltap-sdk
RUN pip install --no-cache-dir /ltap-sdk

# Install LTAP-RP and its dependencies
COPY LTAP-RP/pyproject.toml /app/pyproject.toml
COPY LTAP-RP/ltap_rp /app/ltap_rp
RUN pip install --no-cache-dir -e /app
