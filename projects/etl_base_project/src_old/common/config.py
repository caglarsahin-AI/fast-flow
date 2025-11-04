import os
from dotenv import load_dotenv

# .env dosyasını yükle
load_dotenv()

# Kaynak DB bağlantı bilgileri
SOURCE_DB_CONFIG = {
    "host": os.getenv("SOURCE_DB_HOST", "localhost"),
    "port": int(os.getenv("SOURCE_DB_PORT", "5432")),
    "database": os.getenv("SOURCE_DB_NAME", "source_db"),
    "user": os.getenv("SOURCE_DB_USER", "airflow"),
    "password": os.getenv("SOURCE_DB_PASSWORD", "airflow"),
}

# Hedef DB bağlantı bilgileri
TARGET_DB_CONFIG = {
    "host": os.getenv("TARGET_DB_HOST", "localhost"),
    "port": int(os.getenv("TARGET_DB_PORT", "5432")),
    "database": os.getenv("TARGET_DB_NAME", "target_db"),
    "user": os.getenv("TARGET_DB_USER", "postgres"),
    "password": os.getenv("TARGET_DB_PASS", "sifre"),
}
