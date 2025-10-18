SELECT 
    "ocn_iss"."txn_issuer_security"."txn_guid",
    "ocn_iss"."txn_issuer_security"."cavv_ind",
    "ocn_iss"."txn_issuer_security"."cavv_data",
    "ocn_iss"."txn_issuer_security"."security_level_indicator",
    to_char(current_date , 'yyyymmdd')::numeric as  insert_date
FROM "ocn_iss"."txn_issuer_security"
WHERE EXISTS (
    SELECT 1 
    FROM ocn_iss.txn_issuer_onl tio
    WHERE txn_issuer_security.txn_guid = tio.guid
)