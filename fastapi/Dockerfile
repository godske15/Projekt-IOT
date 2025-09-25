FROM python:3.9-slim-buster
RUN <<EOF
pip install asyncpg
pip install fastapi[standard]
EOF

COPY mainapi.py /mainapi.py
EXPOSE 8812

CMD ["fastapi", "dev", "/mainapi.py"]
