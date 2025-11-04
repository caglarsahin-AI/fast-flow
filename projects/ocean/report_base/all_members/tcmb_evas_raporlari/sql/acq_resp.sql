SELECT 
    current_timestamp,
    'ACQ_RESP' AS report_code,
    card_source AS trx_source,
    to_timestamp(cast(request_date as text), 'YYYYMMDDHH24MISS') AS trx_datetime,
    mbr_id AS bank_code,
    f39 AS trx_resp_code,
    count(*) AS trx_resp_count
FROM 
    ocn_acq.txn_acquirer_all, ocn_cfg.cfg_time_dim tm
WHERE 
    request_date = tm.time_id 
    AND tm.time_id BETWEEN replace(cast(date_trunc('month', current_date - interval '1' month) as varchar(10)), '-', '')::numeric
                      AND replace(cast(date_trunc('month', now()) - interval '1' day as varchar(10)), '-', '')::numeric
    AND mbr_id IN (28)
    AND LTRIM(f39) <> '' AND f39 IS NOT NULL 
    AND card_source IN ('I','D')
GROUP BY trx_datetime, bank_code, trx_resp_code, card_source
