select
	a.card_no
from
	ocn_iss.crd_card a
inner join ocn_iss.crd_card_no_name b on
	a.guid = b.card_guid
	and b.personalization_date is not null
where
	a.mbr_id = 306
	and a.physical_type <> 'V'
union
select
	c.card_no
from
	ocn_iss.crd_card c
inner join ocn_iss.crd_product p on
	c.product_guid = p.guid
	and p.code = 'MAA01'
where
	c.mbr_id = 306
	and c.physical_type <> 'V'
union
select
	d.card_no
from
	ocn_iss.crd_card d
where
	d.mbr_id = 306
	and d.physical_type = 'V'