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
        # 1) SQL'leri oku ve verileri çek
        select_header_sql = load_sql("header_data.sql")
        select_detail_sql = load_sql("select_detail_data.sql")
        select_footer_sql = load_sql("footer_data.sql")

        df_header = ocean_db.execute_query(select_header_sql)
        df_detail = ocean_db.execute_query(select_detail_sql)
        df_footer = ocean_db.execute_query(select_footer_sql)

        # Mevcut kodunuzdaki gibi DATA kolonunu ayarlamaya devam (header/footer için hazır geldiğini varsayıyorum)
        # Eğer header/footer için DATA önceden formatlanmıyorsa, aşağıdaki atamalar alınabilir:
        # df_header["DATA"] = df_header["DATA"]  # (VARSA)
        # df_footer["DATA"] = df_footer["DATA"]  # (VARSA)

        print("[INFO] Data snapshot:")
        print("HEADER empty:", df_header.empty, "| DETAIL empty:", df_detail.empty, "| FOOTER empty:", df_footer.empty)

        # 2) DETAIL boşsa detoken/merge adımını atla, sadece header/footer ile ilerle
        df_merged_data = pd.DataFrame()  # varsayılan boş

        if not df_detail.empty:
            # --- Detoken hazırlığı ---
            distinct_cardno_df = (
                df_detail[['card_no']]
                .dropna(subset=['card_no'])
                .astype({'card_no': str})
                .drop_duplicates(subset='card_no')
                .reset_index(drop=True)
            )

            print(f"[STEP 2] Calling Detoken API in batches | unique cards: {len(distinct_cardno_df)}")

            all_results = []
            batch_size = 500

            # Sadece gerçekten kart varsa API çağır
            if len(distinct_cardno_df) > 0:
                for i in range(0, len(distinct_cardno_df), batch_size):
                    batch = distinct_cardno_df.iloc[i:i+batch_size].copy()
                    card_list = batch["card_no"].tolist()
                    print(f"[INFO] Sending batch {i // batch_size + 1}: {len(card_list)} cards")

                    try:
                        token_map = api.get_card_tokens(card_list)
                    except Exception as e:
                        # API patlarsa akışı kesmeyelim, unmasked_card_no'yu boş geçelim
                        print(f"[WARN] Detoken API failed for this batch: {e}. Continuing with empty unmasked_card_no.")
                        token_map = {}

                    batch["unmasked_card_no"] = batch["card_no"].map(token_map).fillna('')
                    all_results.append(batch)

                # Boş liste concat hatasına karşı koruma
                if all_results:
                    result_detokened_cardno_df = pd.concat(all_results, ignore_index=True)
                else:
                    result_detokened_cardno_df = pd.DataFrame(columns=["card_no", "unmasked_card_no"])
            else:
                # Hiç kart yoksa güvenli boş DF
                result_detokened_cardno_df = pd.DataFrame(columns=["card_no", "unmasked_card_no"])

            # --- Join (detoken sonucu yoksa bile güvenli) ---
            # Left merge: detoken gelmemişse unmasked_card_no '' kalır
            df_merged_data = pd.merge(
                df_detail.copy(),
                result_detokened_cardno_df,
                on="card_no",
                how="left"
            )

            if "unmasked_card_no" not in df_merged_data.columns:
                df_merged_data["unmasked_card_no"] = ''

            # Tip/format temizlikleri (boş/NaN güvenli)
            for col in [
                "txn_guid","block_no","batch_no","txn_date","txn_time","txn_amount","currency_code",
                "otc","ots","install_count","stan","commission_rate","loyalty_commission_rate",
                "block_day1","link_guid","card_country","settlement_amount","settlement_currency",
                "clearing_commission_amount","bank_point","merchant_point"
            ]:
                if col in df_merged_data.columns:
                    df_merged_data[col] = df_merged_data[col].apply(
                        lambda x: str(x).replace('.0', '') if pd.notnull(x) and str(x).strip() != '' else ''
                    )

            # DATA alanını sadece DETAIL varsa üret
            # (Kolonlar yoksa pad_column zaten boş string üretir)
            def safe_pad(col_name, length, pad_char=' ', right=True):
                if col_name in df_merged_data.columns:
                    return pad_column(df_merged_data[col_name], length, pad_char, right)
                # olmayan kolon için boş pad
                return pad_column(pd.Series([''] * len(df_merged_data)), length, pad_char, right)

            df_merged_data["DATA"] = (
                safe_pad("txn_guid", 16, '0') +
                safe_pad("block_no", 10, '0') +
                safe_pad("unmasked_card_no", 19, ' ') +
                safe_pad("merchant_code", 15, ' ') +
                safe_pad("terminal_code", 8, ' ') +
                safe_pad("batch_no", 6, '0') +
                safe_pad("txn_date", 8, '0') +
                safe_pad("txn_time", 9, '0') +
                safe_pad("txn_amount", 20, '0') +
                safe_pad("currency_code", 3, '0') +
                safe_pad("settlement_amount", 20, '0') +
                safe_pad("settlement_currency", 3, '0') +
                safe_pad("clearing_commission_amount", 20, '0') +
                safe_pad("otc", 4, '0') +
                safe_pad("ots", 4, '0') +
                safe_pad("install_type", 1, ' ') +
                safe_pad("banking_txn_code", 30, ' ') +
                safe_pad("txn_effect", 1, ' ') +
                safe_pad("install_count", 3, '0') +
                safe_pad("txn_source", 1, ' ') +
                safe_pad("txn_region", 1, ' ') +
                safe_pad("bin_product_type", 1, ' ') +
                safe_pad("card_segment_code", 4, ' ') +
                safe_pad("txn_terminal_type", 2, ' ') +
                safe_pad("txn_entry", 1, ' ') +
                safe_pad("card_source", 1, ' ') +
                safe_pad("card_brand", 1, ' ') +
                safe_pad("card_dci", 1, ' ') +
                safe_pad("f43", 40, ' ') +
                safe_pad("f43_name", 25, ' ') +
                safe_pad("f43_city", 13, ' ') +
                safe_pad("f43_state", 2, ' ') +
                safe_pad("f43_country", 3, ' ') +
                safe_pad("auth_code", 6, ' ') +
                safe_pad("stan", 6, '0') +
                safe_pad("rrn", 12, ' ') +
                safe_pad("mcc", 4, ' ') +
                safe_pad("bank_point", 20, '0') +
                safe_pad("merchant_point", 20, '0') +
                safe_pad("lyl_add_inst_cnt", 3, '0') +
                safe_pad("lyl_defered_days", 3, '0') +
                safe_pad("commission_rate", 7, '0') +
                safe_pad("loyalty_commission_rate", 7, '0') +
                safe_pad("block_day1", 3, '0') +
                safe_pad("block_type", 2, ' ') +
                safe_pad("link_guid", 16, '0') +
                safe_pad("bkm_id", 8, ' ') +
                safe_pad("card_country", 3, '0') +
                safe_pad("customer_name", 60, ' ') +
                safe_pad("customer_surname", 30, ' ') +
                safe_pad("customer_nationality", 30, ' ')
            )

        # 3) Final dosyayı mevcut parçalarla oluştur
        final_parts = []

        if not df_header.empty and "DATA" in df_header.columns:
            final_parts.append(pd.DataFrame(df_header["DATA"]))
        else:
            print("[WARN] HEADER missing or has no DATA column; skipping.")

        if not df_merged_data.empty and "DATA" in df_merged_data.columns:
            final_parts.append(pd.DataFrame(df_merged_data["DATA"]))
        else:
            print("[INFO] DETAIL part is empty; skipping DETAIL in final file.")

        if not df_footer.empty and "DATA" in df_footer.columns:
            final_parts.append(pd.DataFrame(df_footer["DATA"]))
        else:
            print("[WARN] FOOTER missing or has no DATA column; skipping.")

        if not final_parts:
            print("[WARN] No parts (header/detail/footer) available to export. Nothing to upload.")
            return  # nazik çıkış

        df_full = pd.concat(final_parts, ignore_index=True)

        buffer = StringIO()
        df_full.to_csv(buffer, index=False, header=False)

        buffer.seek(0)
        file_date = datetime.now().strftime("%Y-%m-%d")
        filename = f"GGBank_GunlukAcqIslemBildirim_{file_date}.txt"
        remote_path = f"/ggbank/PROD/BI_Reports/GunlukAcqIslemler/{filename}"

        print(f"[STEP 4] Uploading to FTP: {remote_path}")
        upload_to_sftp(buffer, remote_path)
        print("[INFO] Completed successfully (partial-safe).")

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
