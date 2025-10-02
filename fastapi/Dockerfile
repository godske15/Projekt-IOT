FROM python:3.9-slim-buster
RUN <<EOF
pip install asyncpg
pip install fastapi[standard]
EOF

WORKDIR /api
COPY . /api

# COPY mainapi.py /mainapi.py

CMD ["fastapi", "dev", "/mainapi.py"]
