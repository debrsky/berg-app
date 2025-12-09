DROP MATERIALIZED VIEW IF EXISTS bergapp.invoices;

CREATE MATERIALIZED VIEW IF NOT EXISTS bergapp.invoices
AS
    SELECT 
        -- inv.*,
        inv."ID" AS id_invoice,
        inv."ID_Boss" AS id_seller,
        app."ID_CustomerPay" AS id_payer,
        inv."Nomer" AS nomer,
        inv."Date"::date AS inv_date,
        inv."Date" AS inv_date_ts,
        ROUND(inv."Cost"::numeric, 2) AS amount,
        inv."Name" AS description,
        inv."ID_Application" AS id_app
    FROM 
        bergauto."XInvoices" inv 
        JOIN bergauto."Applications" app ON inv."ID_Application" = app."ID"
    WHERE inv."Nomer" IS NOT NULL AND inv."Nomer" > 0
    ORDER BY inv."Date" DESC
WITH DATA;
