FROM python:3.11-slim
WORKDIR /app

# Устанавливаем wheel напрямую с files.pythonhosted.org (обход таймаутов к индексу pypi.org внутри сборки)
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && curl -fsSL -o /tmp/kafka_python-2.0.2-py2.py3-none-any.whl \
        "https://files.pythonhosted.org/packages/75/68/dcb0db055309f680ab2931a3eeb22d865604b638acf8c914bedf4c1a0c8c/kafka_python-2.0.2-py2.py3-none-any.whl" \
    && pip install --no-cache-dir /tmp/kafka_python-2.0.2-py2.py3-none-any.whl \
    && rm /tmp/kafka_python-2.0.2-py2.py3-none-any.whl \
    && apt-get purge -y curl \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

COPY producer.py .
ENTRYPOINT ["python", "/app/producer.py"]
