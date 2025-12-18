DROP MATERIALIZED VIEW IF EXISTS bergapp.counterparties;
CREATE MATERIALIZED VIEW bergapp.counterparties
AS
 SELECT 
 	"ID" AS id_counterparty,
    "NameShort" AS name,
    "Tel" AS tel,
    "Address" AS address,
    "INN" AS inn,
    -- "OGRN",
    "KPP" AS kpp
    -- "RS",
    -- "KS",
    -- "BIK",
    -- "Bank",
    -- "BaseCode",
    -- "ID_Ref",
    -- "ToExport"
   FROM bergauto."Customers"
WHERE 
	"ID" IN (SELECT DISTINCT id_payer FROM bergapp.invoices)
	OR "ID" IN (SELECT DISTINCT id_consigner FROM bergapp.invoices)
	OR "ID" IN (SELECT DISTINCT id_consignee FROM bergapp.invoices)
WITH DATA
;

