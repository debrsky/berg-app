WITH params AS (
    SELECT $1::date AS start_date, 
           $2::date + 1 AS end_date
)
SELECT DISTINCT
    id_with_prefix                               AS id,
    full_name,
    inn,
    kpp,
    ogrn,
    address,
    bank_account                                 AS rs,      -- расчётный счёт
    correspondent_account                        AS ks,      -- кор. счёт
    bik,
    bank_name,
	memo
FROM (
    -- Продавцы (Bosses) — с префиксом seller_
    SELECT 
        ('seller_' || bs."ID")::text              AS id_with_prefix,
        bs."Name"                                 AS full_name,
        bs."INN"                                  AS inn,
        bs."KPP"                                  AS kpp,
        bs."OGRN"                                 AS ogrn,
        bs."Address"                              AS address,
	    bs."RS"                                   AS bank_account,
        bs."KS"                                   AS correspondent_account,
        bs."BIK"                                  AS bik,
        bs."Bank"                                 AS bank_name,
		bs."Mem"                                  AS memo
    FROM berg."XInvoices" inv
    JOIN berg."Applications" app ON app."ID" = inv."ID_Application"
    JOIN berg."Bosses" bs ON bs."ID" = inv."ID_Boss"
    CROSS JOIN params p
    WHERE inv."Date" >= p.start_date
      AND inv."Date" < p.end_date
      AND inv."Nomer" <> 0

    UNION ALL

    -- Покупатели (Customers) — без префикса, просто числовой ID
    SELECT 
        cp."ID"::text                             AS id_with_prefix,
        cp."NameShort"                            AS full_name,
        cp."INN"                                  AS inn,
        cp."KPP"                                  AS kpp,
        cp."OGRN"                                 AS ogrn,
        cp."Address"                              AS address,
        cp."RS"                                   AS bank_account,
        cp."KS"                                   AS correspondent_account,
        cp."BIK"                                  AS bik,
        cp."Bank"                                 AS bank_name,
		cp."Mem"                                  AS memo
    FROM berg."XInvoices" inv
    JOIN berg."Applications" app ON app."ID" = inv."ID_Application"
    JOIN berg."Customers" cp ON cp."ID" = app."ID_CustomerPay"
    CROSS JOIN params p
    WHERE inv."Date" >= p.start_date
      AND inv."Date" < p.end_date
      AND inv."Nomer" <> 0
) t
-- WHERE full_name IS NOT NULL
ORDER BY full_name;