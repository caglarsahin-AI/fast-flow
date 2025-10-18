SELECT
    "ocn_iss"."txn_issuer_security"."txn_guid",
    "ocn_iss"."txn_issuer_security"."cavv_ind",
    "ocn_iss"."txn_issuer_security"."cavv_data",
    "ocn_iss"."txn_issuer_security"."security_level_indicator",
    to_char(current_date , 'yyyymmdd')::numeric as insert_date
FROM "ocn_iss"."txn_issuer_security"
WHERE exists (
    select 1 
    from ocn_iss.txn_info_issuer tii
    where txn_issuer_security.txn_guid = tii.guid
      and tii.eod_date >= to_char(current_date - 2, 'yyyymmdd')::numeric
)