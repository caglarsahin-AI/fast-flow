ARG BASE_IMAGE
FROM ${BASE_IMAGE} 
COPY requirements.txt /tmp/requirements.txt
COPY plugins/send_email_custom.py /opt/airflow/plugins/send_email_custom.py
RUN pip install -r /tmp/requirements.txt