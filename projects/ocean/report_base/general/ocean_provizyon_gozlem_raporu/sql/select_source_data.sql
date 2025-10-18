with cte_TXN_ACQUIRER_ALL as (
select
	taa.*
from
	OCN_ACQ.TXN_ACQUIRER_ALL taa
where
	taa.mbr_id in (28, 78, 80, 301, 302)
	and taa.request_date = {{etl_date}}
	and taa.STATUS = 1
)
select
	txnacquire0_.request_date as acquiring_islem_tarihi,
	txnacquire0_.request_time as acquiring_islem_saati,
	txnacquire0_.eod_date as sistem_gun_sonu_tarihi,
	txnacquire0_.txn_settle_date as pos_gun_sonu_tarihi,
	txnacquire0_.f2 as kart_no,
	td.otc as otc,
	td.ots as ots,
	substring(td.description from 'TR:=(.*?);;') as islem_adi,
	txnacquire0_.f43 as islem_aciklamasi,
	txnacquire0_.f4 as orjinal_tutar,
	tcd.code as orjinal_para_birimi_kodu,
	substring(tcd.description from 'TR:=(.*?);;') as orjinal_para_birimi_aciklamasi,
	txnacquire0_.f38 as otorizasyon_no,
	tctd.code as cvm_kodu,
	substring(tctd.description from 'TR:=(.*?);;') as cvm_kodu_aciklamasi,
	tetd.code as islem_giris_kodu,
	substring(tetd.description from 'TR:=(.*?);;') as islem_giris_kodu_aciklamasi,
	txnacquire0_.f37 as rrn,
	substring(trd.description from 'TR:=(.*?);;') as bolge,
	ccsd.code as islem_kaynak_kodu,
	substring(ccsd.description from 'TR:=(.*?);;') as islem_kaynak_kodu_aciklamasi,
	tttd.code as terminal_tipi_kodu,
	substring(tttd.description from 'TR:=(.*?);;') as terminal_tipi_aciklamasi,
	trmtermina2_.code as terminal_no,
	txnacquire0_.f11_trm as terminal_stan,
	tmd.code as mcc_kodu,
	substring(tmd.description from 'TR:=(.*?);;') as mcc_kodu_aciklamasi,
	mrcmerchan3_.code as isyeri_no,
	txnacquire0_.f43_country as isyeri_ulke,
	txnacquire0_.txn_settle as islem_settle_mi,
	txnacquire0_.batch_no as batch_no,
	txnacquire6_.order_no as order_no,
	tsd.code as islem_statu_kodu,
	substring(tsd.description from 'TR:=(.*?);;') as islem_statu_kodu_aciklamasi,
	trcd.code as cevap_kodu,
	substring(trcd.description from 'TR:=(.*?);;') as cevap_kodu_aciklamasi,
	txnacquire0_.irc as irc,
	txnacquire0_.MBR_ID as kurum_kodu,
	mrcmerchan3_.customer_no as uye_isyeri_musteri_no,
	--Alias_of_mrc_txn_blocked_transaction.commission_amount as uye_isyeri_komisyon_tutari, neslihan provizyon ekranında kullanılmıyor dedi 20250625
	--Alias_of_mrc_txn_blocked_transaction.release_date as uye_isyerine_odeme_tarihi, neslihan provizyon ekranında kullanılmıyor dedi 20250625
	mrcmerchan3_.opening_date as isyeri_acilis_tarihi,
	mrcmerchan3_.stat as isyeri_statu_kodu,
	txnacquire0_.f43_name as isyeri_adi,
	trmtermina2_.stat_code as terminal_statu_kodu
from
	cte_TXN_ACQUIRER_ALL txnacquire0_
left join ocn_cfg.txn_def td on
	td.guid = txnacquire0_.txn_def_guid
left join ocn_cfg.txn_currency_def tcd on
	tcd.code = txnacquire0_.f49
left join ocn_cfg.txn_cvm_type_def tctd on
	tctd.code = txnacquire0_.cvm_type
left join ocn_cfg.txn_entry_type_def tetd on
	tetd.code = txnacquire0_.txn_entry
left join ocn_cfg.txn_region_def trd on
	trd.code = txnacquire0_.txn_region
left join ocn_cfg.cfg_card_source_def ccsd on
	ccsd.code = txnacquire0_.card_source
left join ocn_cfg.txn_terminal_type_def tttd on
	tttd.code = txnacquire0_.terminal_type
left join ocn_cfg.txn_mcc_def tmd on
	tmd.code = txnacquire0_.f18::text
left join ocn_cfg.txn_stat_def tsd on
	tsd.code = txnacquire0_.txn_stt
left join ocn_cfg.txn_response_code_def trcd on
	trcd.code = txnacquire0_.f39
left join OCN_ACQ.TRM_TERMINAL trmtermina2_ on
	txnacquire0_.terminal_guid = trmtermina2_.guid
	and txnacquire0_.mbr_id = trmtermina2_.MBR_ID
left join OCN_ACQ.MRC_MERCHANT mrcmerchan3_ on
	trmtermina2_.merchant_guid = mrcmerchan3_.guid
	and trmtermina2_.MBR_ID = mrcmerchan3_.MBR_ID
left outer join OCN_ACQ.TXN_ACQUIRER_EXT txnacquire6_ on
	txnacquire0_.guid = txnacquire6_.txn_guid