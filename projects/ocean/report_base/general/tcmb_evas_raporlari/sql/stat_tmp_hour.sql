select
	current_timestamp as current_timestamp,
	a.card_source as trx_source,
	to_timestamp(a.request_date::text, 'YYYYMMDD') as trx_datetime,
	substring(lpad(a.request_time::text, 9, '0'), 1, 2)::int as trx_hour,
	a.mbr_id as bank_code,
	count(*) as trx_resp_count
from
	ocn_acq.txn_acquirer_all a
join ocn_cfg.cfg_time_dim tm
  on
	a.request_date = tm.time_id
where
	tm.time_id between
      (to_char(date_trunc('month', current_date - interval '1 month'), 'YYYYMMDD'))::numeric
    and
      (to_char(date_trunc('month', current_date) - interval '1 day', 'YYYYMMDD'))::numeric
	and a.mbr_id = 28
	and a.f39 is not null
	and a.f39 <> ''
	and a.card_source in ('I', 'D')
group by
	trx_source,
	trx_datetime,
	trx_hour,
	bank_code
order by
	trx_datetime,
	trx_hour,
	trx_source;