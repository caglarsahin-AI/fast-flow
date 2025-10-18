import os
import json
import psycopg2
import pandas as pd
from io import StringIO, BytesIO
import paramiko
from dotenv import load_dotenv
from datetime import datetime, timedelta
import requests
import warnings
warnings.filterwarnings("ignore")

def load_env_or_airflow_config(key, is_airflow_env, default_port = 5432):
    """Genel amaçlı config yükleyici: DB, API, SFTP hepsi için çalışır."""
    if is_airflow_env:
        from airflow.models import Variable
        print(f"[INFO] Loading config from Airflow Variable: {key}")
        print("json.loads(Variable.get(key)): ", json.loads(Variable.get(key)))
        return json.loads(Variable.get(key))
    else:
        load_dotenv()
        if key == "detoken_auth":
            return {
                "base_url": os.getenv("DETOKEN_BASE_URL"),
                "user_code": os.getenv("DETOKEN_USER_CODE"),
                "password": os.getenv("DETOKEN_PASSWORD"),
                "mbr_id": 1
            }
        elif key == "sftp_auth":
            return {
                "host": os.getenv("SFTP_HOST"),
                "port": int(os.getenv("SFTP_PORT", 22)),
                "username": os.getenv("SFTP_USER"),
                "password": os.getenv("SFTP_PASSWORD")
            }
        else:  # DB config
            return {
                "host": os.getenv(f"{key.upper()}_HOST"),
                "port": int(os.getenv(f"{key.upper()}_PORT", default_port)),
                "database": os.getenv(f"{key.upper()}_DB"),
                "user": os.getenv(f"{key.upper()}_USER"),
                "password": os.getenv(f"{key.upper()}_PASSWORD")
            }

class DatabaseConnection:
    def __init__(self, host, port, database, user, password):
        print(f"[INFO] Connecting to DB: {host}:{port}/{database} as {user}")
        self.conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )

    def execute_query(self, query):
        print("[INFO] Executing SELECT query.")
        return pd.read_sql(query, self.conn)

    def close(self):
        print("[INFO] Closing DB connection.")
        self.conn.close()

class ApiManager:
    def __init__(self, base_url, user_code, password, mbr_id, session_timeout = 30):
        self.base_url = base_url
        self.payload = {
            "userCode": user_code,
            "password": password,
            "mbrId": mbr_id,
            "sessionTimeout": session_timeout
        }
        self.token = None
        self.expires_at = datetime.utcnow()

    def get_token(self):
        print("[API] Getting token...")
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        r = requests.post(f"{self.base_url}/api/authentication/token", json=self.payload, headers=headers, verify=False)
        r.raise_for_status()
        data = r.json()['result']
        self.token = data['token']
        print(f"[API] Token received: {self.token[:10]}... (length: {len(self.token)})")
        self.expires_at = datetime.utcnow() + timedelta(minutes=29)
        print("[API] Token got.")

    def ensure_token(self):
        if not self.token or datetime.utcnow() >= self.expires_at:
            self.get_token()

    def get_card_tokens(self, card_list):
        self.ensure_token()
        headers = {
            "x-token": self.token,
            "Content-Type": "application/json",
            "Accept": "*/*"
        }
        payload = {"cardTokens": [{"cardToken": card} for card in card_list]}
        r = requests.post(
            f"{self.base_url}/api/token/getClearCardsAsync",
            headers=headers,
            json=payload,
            timeout=None,
            verify=False
        )
        r.raise_for_status()
        return {item["cardToken"]: item["cardNo"] for item in r.json()["result"]["cardWithTokens"]}


def load_sql(file_name):
    base_path = os.path.dirname(__file__)
    file_path = os.path.join(base_path, 'sql', file_name)
    print(f"[INFO] Loading SQL file: {file_path}")
    with open(file_path, 'r', encoding='utf-8') as f:
        return f.read()



def upload_to_sftp(file_buffer, remote_path):
    """
    SFTP (SSH) kullanarak in-memory (StringIO) veriyi
    uzaktaki 'remote_path' konumuna yükler.
    Sabit fingerprint doğrulaması yapılır (karşılaştırmasız).
    """
    print(f"[INFO] Preparing SFTP connection...")
    is_airflow_env = os.getenv("AIRFLOW_CTX_DAG_ID") is not None
    sftp_config = load_env_or_airflow_config("sftp_auth", is_airflow_env)

    host = sftp_config.get("host")
    port = sftp_config.get("port", 22)
    username = sftp_config.get("username")
    password = sftp_config.get("password")

    #expected_fingerprint = "05:c9:..."  # sabit fingerprint sadece log için
    #print("expected_fingerprint:", expected_fingerprint)

    try:
        transport = paramiko.Transport((host, port))
        transport.connect(username=username, password=password)

        # SFTP oturumu başlat
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Veriyi upload et
        data_bytes = file_buffer.getvalue().encode("utf-8")
        bio = BytesIO(data_bytes)
        sftp.putfo(bio, remote_path)
        bio.close()

        print(f"[INFO] File uploaded to SFTP: {remote_path}")
        sftp.close()
        transport.close()

    except Exception as e:
        print(f"[ERROR] Failed to upload via SFTP: {e}")


def run_etl(ocean_config, api_config):
    ocean_db = DatabaseConnection(**ocean_config)
    api = ApiManager(**api_config)

    try:
        print("[STEP 1] Fetching masked cards")
        select_sql = load_sql("select_masked_cards.sql")
        df = ocean_db.execute_query(select_sql)

        if df.empty:
            print("[INFO] No masked cards found.")
            return


        print("[STEP 2] Calling Detoken API in batches")
        all_results = []
        batch_size = 500

        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            card_list = batch["card_no"].tolist()
            print(f"[INFO] Sending batch {i // batch_size + 1}: {len(card_list)} cards")
            token_map = api.get_card_tokens(card_list)
            batch["unmasked_card_no"] = batch["card_no"].map(token_map)
            all_results.append(batch)

        result_df = pd.concat(all_results)

        print("[STEP 3] Preparing in-memory TXT stream")
        buffer = StringIO()
        result_df.to_csv(buffer, index=False, sep=";")
        buffer.seek(0)  # Dosya başına dön

        file_date = datetime.now().strftime("%Y%m%d")
        filename = f"Tum_Kartlar{file_date}.txt"
        remote_path = f"/ATBank/PROD/BI_REPORTS/Tum_Kartlar/{filename}"

        print(f"[STEP 4] Uploading to FTP: {remote_path}")
        upload_to_sftp(buffer, remote_path)

        print("[INFO] ETL + Detoken + FTP upload completed (no local file).")
    finally:
        ocean_db.close()

def main():
    is_airflow_env = os.getenv("AIRFLOW_CTX_DAG_ID") is not None
    print(f"[INFO] Airflow environment: {is_airflow_env}")

    ocean_config = load_env_or_airflow_config("ocean", is_airflow_env)
    api_config = load_env_or_airflow_config("detoken_auth", is_airflow_env)

    run_etl(ocean_config, api_config)



if __name__ == "__main__":
    main()
