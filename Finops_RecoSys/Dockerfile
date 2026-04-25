FROM python:3.12-slim

LABEL maintainer="FinOps Team"
LABEL description="EC2 Cost Optimisation Tool"
LABEL version="1.3"

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py                  .
COPY excel_export.py         .
COPY instance_api.py         .
COPY instance_families.py    .
COPY data_loader.py          .
COPY os_resolve.py           .
COPY sheet_merger.py         .
COPY processor.py            .
COPY recommender.py          .
COPY rds_recommender.py      .
COPY rds_mysql_sa_prices.py  .
COPY pricing_engine.py       .
COPY pricing_normalize.py    .
COPY ec2_ondemand_public.py  .

RUN mkdir -p /root/.streamlit
COPY .streamlit/config.toml /root/.streamlit/config.toml

EXPOSE 8501

HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8501/_stcore/health', timeout=5)" || exit 1

CMD ["streamlit", "run", "app.py", \
     "--server.port=8501", \
     "--server.address=0.0.0.0", \
     "--server.headless=true", \
     "--browser.gatherUsageStats=false"]
