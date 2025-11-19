/*
 * Возвращает список счетов, у которых сумма к оплате отличается
 * от итога по позициям в счете, и при этом нет договора
 */
SELECT
    "ID",
    "ID_Application",
    "Nomer",
    "Date",
    "Cost"                  AS "InvCost",
    "IsFixed"               AS "IsContract",
    "Pays"                  AS "InvPays",
    "Name",
    "Mem",
    "ID_Boss"               AS "ID_boss",
    "IsUseSecondAccount",
    "BossName",
    "PayerID",
    "PayerName",
    "SumItemCost"
FROM (
    SELECT
        inv."ID",
        inv."ID_Application",
        inv."Nomer",
        inv."Date",
        inv."Cost",
        inv."IsFixed",
        inv."Pays",
        inv."Name",
        inv."Mem",
        inv."ID_Boss",
        inv."IsUseSecondAccount",
        bs."Name"                  AS "BossName",
        app."ID_CustomerPay"       AS "PayerID",
        c."NameShort"              AS "PayerName",
        (
            SELECT SUM(xd."Cost")
            FROM berg."XInvoiceDatas" xd
            WHERE xd."ID_XInvoice" = inv."ID"
              AND xd."IsFree" = FALSE
        ) AS "SumItemCost"

    FROM berg."XInvoices" inv
    INNER JOIN berg."Bosses" bs              ON bs."ID" = inv."ID_Boss"
    INNER JOIN berg."Applications" app       ON app."ID" = inv."ID_Application"
    INNER JOIN berg."Customers" c            ON c."ID" = app."ID_CustomerPay"

    WHERE inv."Nomer" <> 0
      AND inv."IsFixed" = FALSE          -- И НЕ договор
) sub
WHERE ABS(COALESCE("SumItemCost", 0) - "Cost") >= 0.01
  AND "SumItemCost" IS NOT NULL
ORDER BY "Date" DESC;