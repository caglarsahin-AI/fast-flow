from airflow.utils.email import send_email_smtp

def failure_alert(context):
    ti = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    task_id = ti.task_id
    execution_date = context.get("execution_date")
    log_url = ti.log_url
    #hostname = ti.hostname
    hostname = "10.62.150.102 - PROD"
    duration = f"{round(ti.duration, 2)} saniye" if ti.duration else "Hesaplanamadı"
    exception = str(context.get("exception", "Belirlenemedi"))

    subject = f"[HATA] DWH - AIRFLOW DAG: {dag_id} | Task: {dag_id} | Zaman: {execution_date}"

    html_content = f"""
    <html>
        <body>
            <h3 style="color:red;">🚨 Airflow Görev Hata Bildirimi</h3>
            <p><strong>DAG:</strong> {dag_id}</p>
            <p><strong>Task:</strong> {task_id}</p>
            <p><strong>Çalışma Zamanı:</strong> {execution_date}</p>
            <p><strong>Hostname:</strong> {hostname}</p>
            <p><strong>Süre:</strong> {duration}</p>
            <hr>
            <p style="font-size:16px; color:red;"><strong>DWH NÖBETÇİSİNİ ARAYINIZ.</strong></p>
            <p style="font-size:11px;color:gray;">Bu e-posta Airflow tarafından otomatik olarak gönderilmiştir.</p>
            <hr>
            <p><strong>Hata:</strong><br><pre style="color:red;">{exception}</pre></p>
        </body>
    </html>
    """

    send_email_smtp(
        to=["mert.celikan@paycore.com", "caglar.sahin@paycore.com"],
        subject=subject,
        html_content=html_content
    )
