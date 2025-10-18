select 'H' || to_char(now() - INTERVAL '1 DAY','YYYYMMDD') ||
        LPAD(cast(coalesce((select "sequence" + 1 from ocn_btc.file_pattern_sequence 
               where "key" = 'AcquiringTxnReport' and execution_date = to_number(to_char(now() - INTERVAL '1 DAY','YYYYMMDD'),'99999999') limit 1), 1) as varchar),
                   16, '0') || LPAD('V00001', 12, '0') H;