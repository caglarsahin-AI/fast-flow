import os
import json
#import pysftp
import psycopg2
import pandas as pd
from io import StringIO, BytesIO
#from ftplib import FTP, FTP_TLS, error_perm
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

def pad_column(series, length, pad_char=' ', right=True):
    # NaN değerleri boş string yap
    series = series.fillna('')
    # Hepsini string'e çevir
    series = series.astype(str)
    # Pad işlemi: sağdan mı soldan mı doldurulacak
    if pad_char == '0':
        return series.str.zfill(length)
    elif right:
        return series.str.pad(width=length, side='left', fillchar=pad_char)
    else:
        return series.str.pad(width=length, side='right', fillchar=pad_char)


def run_etl(ocean_config, api_config):
    ocean_db = DatabaseConnection(**ocean_config)
    api = ApiManager(**api_config)

    try:
        select_header_sql = load_sql("header_data.sql")
        select_detail_sql = load_sql("select_detail_data.sql")
        select_footer_sql = load_sql("footer_data.sql")

        df_header = ocean_db.execute_query(select_header_sql)
        df_header["DATA"] = df_header
        df_detail = ocean_db.execute_query(select_detail_sql)
        df_footer = ocean_db.execute_query(select_footer_sql)
        df_footer["DATA"] = df_footer

        print(df_header)
        #df_header.to_csv("df_header.csv")
        print("-------------------------------")
        print(df_detail)
        #df_detail.to_csv("df_detail.csv")
        print("-------------------------------")
        print(df_footer)

# 1- detail sorgsu sonucu gelen data içerisinden, kart nolar detoken yapılacak. 
# Bunun için detail data da ayrı bir df te kart_nolar tekilleştirilmesi faydalı olacaktır.
# 2. detail data özel bir formatta detoken kart no ile birlikte, ftp ye yazılacak
# 3. header, footer ve formatlanmış detail data birliştirilip FTP klasörüne gönderilecek.

        distinct_cardno_df = df_detail[['card_no']].drop_duplicates(subset='card_no')

        print("[STEP 2] Calling Detoken API in batches")
        all_results = []
        batch_size = 500
        for i in range(0, len(distinct_cardno_df), batch_size):
            batch = distinct_cardno_df.iloc[i:i+batch_size]
            card_list = batch["card_no"].tolist()
            print(f"[INFO] Sending batch {i // batch_size + 1}: {len(card_list)} cards")
            token_map = api.get_card_tokens(card_list)
            batch["unmasked_card_no"] = batch["card_no"].map(token_map)
            all_results.append(batch)
        result_detokened_cardno_df = pd.concat(all_results)
        #print("result_detokened_cardno_df:", result_detokened_cardno_df)
        #result_detokened_cardno_df.to_csv("my_csv.csv",)

        # Join işlemi
        df_merged_data = pd.merge(df_detail, result_detokened_cardno_df, on=["card_no"], how="inner")

        # 1. full data

        buffer = StringIO()
        df_merged_data.to_csv(buffer,index=False, header=False,sep=";")
        """buffer.seek(0)  # Dosya başına dön
        file_date = datetime.now().strftime("%Y-%m-%d")
        filename = f"merged_data_GGBank_GunlukAcqIslemBildirim_{file_date}.csv"
        remote_path = f"/ggbank/PROD/BI_Reports/GunlukAcqIslemler/{filename}"
      
        print(f"[STEP 3] Uploading merged_data to FTP: {remote_path}")
        upload_to_sftp(buffer, remote_path)
        #
        """
        for col in ["txn_guid","block_no", "batch_no", "txn_date", "txn_time","txn_amount", "txn_amount", "currency_code", "otc","ots","install_count",
                    "stan","commission_rate",	"loyalty_commission_rate",	"block_day1","link_guid","card_country",
                    "settlement_amount","settlement_currency", "clearing_commission_amount", "bank_point", "merchant_point"]:
            df_merged_data[col] = df_merged_data[col].apply(lambda x: str(x).replace('.0', '') if pd.notnull(x) and str(x).strip() != '' else '')

        # DATA sütununu oluştur
        df_merged_data["DATA"] = (
            pad_column(df_merged_data["txn_guid"], 16, '0') +
            pad_column(df_merged_data["block_no"], 10, '0') +
            pad_column(df_merged_data["unmasked_card_no"], 19, ' ') +
            pad_column(df_merged_data["merchant_code"], 15, ' ') +
            pad_column(df_merged_data["terminal_code"], 8, ' ') +
            pad_column(df_merged_data["batch_no"], 6, '0') +
            pad_column(df_merged_data["txn_date"], 8, '0') +
            pad_column(df_merged_data["txn_time"], 9, '0') +
            pad_column(df_merged_data["txn_amount"], 20, '0') +
            pad_column(df_merged_data["currency_code"], 3, '0') +
            pad_column(df_merged_data["settlement_amount"], 20, '0') +
            pad_column(df_merged_data["settlement_currency"], 3, '0') +
            pad_column(df_merged_data["clearing_commission_amount"], 20, '0') +
            pad_column(df_merged_data["otc"], 4, '0') +
            pad_column(df_merged_data["ots"], 4, '0') +
            pad_column(df_merged_data["install_type"], 1, ' ') +
            pad_column(df_merged_data["banking_txn_code"], 30, ' ') +
            pad_column(df_merged_data["txn_effect"], 1, ' ') +
            pad_column(df_merged_data["install_count"], 3, '0') +
            pad_column(df_merged_data["txn_source"], 1, ' ') +
            pad_column(df_merged_data["txn_region"], 1, ' ') +
            pad_column(df_merged_data["bin_product_type"], 1, ' ') +
            pad_column(df_merged_data["card_segment_code"], 4, ' ') +
            pad_column(df_merged_data["txn_terminal_type"], 2, ' ') +
            pad_column(df_merged_data["txn_entry"], 1, ' ') +
            pad_column(df_merged_data["card_source"], 1, ' ') +
            pad_column(df_merged_data["card_brand"], 1, ' ') +
            pad_column(df_merged_data["card_dci"], 1, ' ') +
            pad_column(df_merged_data["f43"], 40, ' ') +
            pad_column(df_merged_data["f43_name"], 25, ' ') +
            pad_column(df_merged_data["f43_city"], 13, ' ') +
            pad_column(df_merged_data["f43_state"], 2, ' ') +
            pad_column(df_merged_data["f43_country"], 3, ' ') +
            pad_column(df_merged_data["auth_code"], 6, ' ') +
            pad_column(df_merged_data["stan"], 6, '0') +
            pad_column(df_merged_data["rrn"], 12, ' ') +
            pad_column(df_merged_data["mcc"], 4, ' ') +
            pad_column(df_merged_data["bank_point"], 20, '0') +
            pad_column(df_merged_data["merchant_point"], 20, '0') +
            pad_column(df_merged_data["lyl_add_inst_cnt"], 3, '0') +
            pad_column(df_merged_data["lyl_defered_days"], 3, '0') +
            pad_column(df_merged_data["commission_rate"], 7, '0') +
            pad_column(df_merged_data["loyalty_commission_rate"], 7, '0') +
            pad_column(df_merged_data["block_day1"], 3, '0') +
            pad_column(df_merged_data["block_type"], 2, ' ') +
            pad_column(df_merged_data["link_guid"], 16, '0') +
            pad_column(df_merged_data["bkm_id"], 8, ' ') +
            pad_column(df_merged_data["card_country"], 3, '0') +
            pad_column(df_merged_data["customer_name"], 60, ' ') +
            pad_column(df_merged_data["customer_surname"], 30, ' ') +
            pad_column(df_merged_data["customer_nationality"], 30, ' ')
        )
        
        df_header_data_only= pd.DataFrame(df_header["DATA"]) 
        df_detail_data_only = pd.DataFrame(df_merged_data["DATA"])
        df_footer_data_only= pd.DataFrame(df_footer["DATA"]) 

        df_full = pd.concat([df_header_data_only, df_detail_data_only, df_footer_data_only], ignore_index=True)
        
        buffer = StringIO()
        df_full.to_csv(buffer, index=False, header=False)
        #df_full.to_csv("my_last_csv.csv", index=False, header=False)

       # with open("my_last_csv.csv", "w", encoding="utf-8", newline='') as f:
       #     df_full.to_csv(f, index=False, header=False)

        buffer.seek(0)  # Dosya başına dön
        file_date = datetime.now().strftime("%Y-%m-%d")
        filename = f"GGBank_GunlukAcqIslemBildirim_{file_date}.txt"
        remote_path = f"/ggbank/PROD/BI_Reports/GunlukAcqIslemler/{filename}"

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
