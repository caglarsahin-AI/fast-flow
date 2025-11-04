select 'F' || to_char(now() - INTERVAL '1 DAY','YYYYMMDD') || LPAD((SELECT cast(COUNT(*) as varchar) 
FROM
(select
mtp.txn_guid
from ocn_acq.mrc_txn_pool mtp
inner join ocn_acq.mrc_txn_blocked_transaction mtbt on mtp.guid = mtbt.pool_guid
inner join ocn_cfg.txn_def_member tdm on mtp.txn_def_guid = tdm.txn_def_guid
inner join ocn_acq.mrc_merchant mm on mm.guid = mtp.merchant_guid
inner join ocn_cfg.txn_def td on td.guid = mtp.txn_def_guid
left outer join ocn_acq.mrc_product_segment_def mpsd on mpsd.guid = mtp.product_segment_guid
inner join ocn_acq.txn_acquirer ta on ta.guid = mtp.txn_guid
where
mtp.is_processed = 1 and
mtp.eod_date = to_number(to_char(now() - INTERVAL '1 DAY','YYYYMMDD'),'99999999') and
mtp.mbr_id = 301 and mm.nsw_unique_id is not null
union all
select
mtp.txn_guid
from ocn_acq.mrc_txn_pool mtp
inner join ocn_acq.mrc_txn_blocked_transaction mtbt on mtp.guid = mtbt.pool_guid
inner join ocn_cfg.member_definition md on mtp.mbr_id = md.mbr_id
inner join ocn_cfg.txn_def_member tdm on mtp.txn_def_guid = tdm.txn_def_guid
inner join ocn_acq.mrc_merchant mm on mm.guid = mtp.merchant_guid
inner join ocn_cfg.txn_def td on td.guid = mtp.txn_def_guid
left outer join ocn_acq.mrc_product_segment_def mpsd on mpsd.guid = mtp.product_segment_guid
inner join ocn_iss.txn_issuer ti on ti.guid = mtp.txn_guid
inner join ocn_iss.crd_card crd on crd.guid = ti.card_guid
inner join ocn_iss.cst_customer cst on cst.customer_no = crd.customer_no
inner join ocn_iss.cst_customer_identity id on id.customer_no = cst.customer_no
where
mtp.is_processed = 1 and
ti.txn_stt = 'N' and
mtp.eod_date = to_number(to_char(now() - INTERVAL '1 DAY','YYYYMMDD'),'99999999') and 
mtp.mbr_id = 301 and mm.nsw_unique_id is not null) as count_table), 10, '0') F

