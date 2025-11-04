select t1.txn_guid , t1.card_guid , t1.billing_amount , t1.customer_no , t1.physical_type, t1.txn_date , 'out 12 months' txn_def 
from edw.vodafone_aktif_kullanici_raporu t1
where t1.txn_def not like 'out%months'
and not exists (select 1 from edw.vodafone_aktif_kullanici_raporu t2 
					where t2.txn_def not like 'out%months' and t1.customer_no=t2.customer_no 
					and t2.txn_date >= '" + context.my_parameter_12 + "'
					 and t2.txn_date < '" + context.my_parameter + "')
and t1.txn_date = '" + context.my_parameter + "'