WITH params AS (
    SELECT
        $1::date AS start_date,
        $2::date + 1 AS end_date
),
measure_units AS (
    VALUES (0,'шт.'), (1,'кг'), (2,'м³'), (3,'час'), (4,'рейс')
),
lines AS (
    SELECT
        invd."ID_XInvoice" AS inv_id,
        jsonb_agg(
            jsonb_build_object(
                -- 'ID_XInvoice', invd."ID_XInvoice",
                'Pos',         invd."Pos",
                'Name',        invd."Name",
                'Amount',      invd."Amount",
                'Price',       invd."Price",
                'Cost',        invd."Cost",
                -- 'car_nick',    COALESCE(crs."Nick", ''),
                -- 'hours',       invd."Hours",
                -- 'len',         invd."Len",
                'TypeEd',      invd."TypeEd",
                'Type',        0,
                'mU',          COALESCE(mu.column2, 'шт.')
            )
            ORDER BY invd."Pos"
        ) AS datas_array
    FROM berg."XInvoiceDatas" invd
    LEFT JOIN berg."Cars" crs ON crs."ID" = invd."ID_Car"
    LEFT JOIN measure_units mu ON mu.column1 = invd."TypeEd"   -- вот правильный синтаксис
    WHERE invd."IsFree" = false
    GROUP BY invd."ID_XInvoice"
)
SELECT
    inv."ID" AS inv_id,
    jsonb_build_object(
        'inv_id',          inv."ID",
        'inv_nomer',       inv."Nomer",
        'inv_date',        to_char(inv."Date", 'YYYY-MM-DD'),
        'inv_cost',        inv."Cost",
        -- 'inv_subject',     inv."Name",
        'is_contract',     inv."IsFixed",
        'inv_mem',         inv."Mem",
        'seller_id',       'seller_' || inv."ID_Boss",
        'buyer_id',        app."ID_CustomerPay"::text,
        'generation_date', to_char(now(), 'YYYY-MM-DD"T"HH24:MI:SS'),
        'DatasArray',
            CASE 
                WHEN inv."IsFixed" THEN
                    jsonb_build_array(jsonb_build_object(
                        -- 'ID_XInvoice', inv."ID",
                        'Pos', 0,
                        'Name', inv."Name",
                        'TypeEd', 0,
                        'Type', 0,
                        'Amount', 1,
                        'Price', inv."Cost",
                        'Cost', inv."Cost",
                        'mU', 'шт.'
                    ))
                ELSE COALESCE(l.datas_array, '[]'::jsonb)
            END
    ) AS invoice_json
FROM berg."XInvoices" inv
JOIN berg."Applications" app ON app."ID" = inv."ID_Application"
CROSS JOIN params p
LEFT JOIN lines l ON l.inv_id = inv."ID"
WHERE inv."Date" >= p.start_date
  AND inv."Date" < p.end_date
  AND inv."Nomer" <> 0
ORDER BY inv."ID";