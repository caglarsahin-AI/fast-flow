select ti.guid txn_guid,ti.card_guid,ti.billing_amount,c.customer_no,c.physical_type,'{trx_date}' txn_date,'Succesfull' txn_def
from ocn_iss.txn_issuer ti
inner join ocn_iss.crd_card c on ti.card_guid = c.guid
LEFT OUTER JOIN ocn_cfg.txn_def d ON (ti.txn_def_guid=d.guid)
where ti.request_date >=  {start_date} and ti.request_date < {finish_date}
and ti.mbr_id=86
and ti.f39='00'
and ti.txn_settle='Y'
AND ocn_cfg.fn_get_language_text(d.description ,'TR') NOT IN  ('Sifre Atama(IVR)','PartialCvvCheck','Para  Transfer  Alici','Inaktiflik Ücreti')
and d.is_financial = 1
union all
select ti.guid txn_guid,ti.card_guid,ti.billing_amount,c.customer_no,c.physical_type,'{trx_date}' txn_date,'Pending' txn_def
from ocn_iss.txn_issuer_onl ti
inner join ocn_iss.crd_card c on ti.card_guid = c.guid
LEFT OUTER JOIN ocn_cfg.txn_def d ON (ti.txn_def_guid=d.guid)
where ti.request_date >= {start_date} and ti.request_date < {finish_date}
and ti.mbr_id=86
and ti.f39='00'
and ti.txn_settle='Y'
AND ocn_cfg.fn_get_language_text(d.description ,'TR') NOT IN  ('Sifre Atama(IVR)','PartialCvvCheck','Para  Transfer  Alici','Inaktiflik Ücreti')
and d.is_financial = 1