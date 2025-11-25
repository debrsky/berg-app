WITH params AS (
    SELECT
		$1::date AS start_date,
		$2::date AS end_date
),
measure_units AS (
    SELECT * FROM (VALUES
        (0, 'шт.'), (1, 'кг'), (2, 'м³'), (3, 'час'), (4, 'рейс')
    ) AS t(type_ed, mu_name)
),
main AS (
    SELECT
        inv."ID"                    AS inv_id,
        inv."Nomer"                 AS inv_nomer,
        to_char(inv."Date", 'YYYY-MM-DD') AS inv_date,
        inv."Cost"                  AS inv_cost,
        inv."Name"                  AS inv_subject,
        inv."IsFixed"               AS is_contract,
        inv."Mem"                   AS inv_mem,

        bs."BossName"               AS seller_name,
        bs."Name"                   AS seller_full_name,
        bs."INN"                    AS seller_inn,
        bs."KPP"                    AS seller_kpp,
        bs."OGRN"                   AS seller_ogrn,
        bs."Address"                AS seller_address,

        cp."ID"                     AS buyer_id,
        cp."NameShort"              AS buyer_name,
        cp."Address"                AS buyer_address,
        cp."INN"                    AS buyer_inn,
        cp."KPP"                    AS buyer_kpp,
        cp."OGRN"                   AS buyer_ogrn,

        to_char(now(), 'YYYY-MM-DD"T"HH24:MI:SS') AS generation_date
    FROM berg."XInvoices" inv
    JOIN berg."Applications" app ON app."ID" = inv."ID_Application"
    JOIN berg."Bosses" bs        ON bs."ID"  = inv."ID_Boss"
    JOIN berg."Customers" cp     ON cp."ID"  = app."ID_CustomerPay"
    CROSS JOIN params p
    WHERE
		inv."Date"::date BETWEEN p.start_date AND p.end_date
		AND inv."Nomer" <> 0
    ORDER BY inv."ID"
),
items_raw AS (
    SELECT
        invd."ID_XInvoice"          AS invoice_id,
        invd."Pos"                  AS pos,
        invd."Name"                 AS name,
        invd."Amount"               AS amount,
        invd."Price"                AS price,
        invd."Cost"                 AS cost,
        COALESCE(crs."Nick", '')    AS car_nick,
        invd."Hours"                AS hours,
        invd."Len"                  AS len,
        invd."TypeEd"               AS type_ed
    FROM berg."XInvoiceDatas" invd
    LEFT JOIN berg."Cars" crs ON crs."ID" = invd."ID_Car"
    WHERE invd."IsFree" = false
      AND invd."ID_XInvoice" IN (SELECT inv_id FROM main)
)

-- Возвращаем по одному счёту за раз!
SELECT
    m.inv_id,
    jsonb_build_object(
        'inv_id',           m.inv_id,
        'inv_nomer',        m.inv_nomer,
        'inv_date',         m.inv_date,
        'inv_cost',         m.inv_cost,
        'inv_subject',      m.inv_subject,
        'is_contract',      m.is_contract,
        'inv_mem',          m.inv_mem,
        'seller_name',      m.seller_name,
        'seller_full_name', m.seller_full_name,
        'seller_inn',       m.seller_inn,
        'seller_kpp',       m.seller_kpp,
        'seller_ogrn',      m.seller_ogrn,
        'seller_address',   m.seller_address,
        'buyer_id',         m.buyer_id,
        'buyer_name',       m.buyer_name,
        'buyer_address',    m.buyer_address,
        'buyer_inn',        m.buyer_inn,
        'buyer_kpp',        m.buyer_kpp,
        'buyer_ogrn',       m.buyer_ogrn,
        'generation_date',  m.generation_date,

        'DatasArray', CASE
            WHEN m.is_contract THEN
                jsonb_build_array(jsonb_build_object(
                    'ID_XInvoice', m.inv_id,
                    'Pos', 0,
                    'Name', m.inv_subject,
                    'TypeEd', 0,
                    'Type', 0,
                    'Amount', 1,
                    'Price', m.inv_cost,
                    'Cost', m.inv_cost,
                    'mU', (SELECT mu_name FROM measure_units WHERE type_ed = 0)
                ))
            ELSE
                COALESCE((
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'ID_XInvoice', ir.invoice_id,
                            'Pos',         ir.pos,
                            'Name',        ir.name,
                            'Amount',      ir.amount,
                            'Price',       ir.price,
                            'Cost',        ir.cost,
                            'car_nick',    ir.car_nick,
                            'hours',       ir.hours,
                            'len',         ir.len,
                            'TypeEd',      ir.type_ed,
                            'Type',        0,
                            'mU',          COALESCE(mu.mu_name, 'шт.')
                        ) ORDER BY ir.pos
                    )
                    FROM items_raw ir
                    LEFT JOIN measure_units mu ON mu.type_ed = ir.type_ed
                    WHERE ir.invoice_id = m.inv_id
                ), '[]'::jsonb)
        END
    ) AS invoice_json
FROM main m;
