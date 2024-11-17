FROM quay.io/astronomer/astro-runtime:12.3.0

# dbt
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
   pip install --no-cache-dir dbt-core dbt-postgres && deactivate
