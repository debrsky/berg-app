-- View: bergapp.sellers

DROP MATERIALIZED VIEW IF EXISTS bergapp.sellers;

CREATE MATERIALIZED VIEW bergapp.sellers
AS
 SELECT "ID" AS id_seller,
    -- "Name",
    "BossName" AS name
    -- "Mem",
    -- "Tel",
    -- "Address",
    -- "INN",
    -- "OGRN",
    -- "KPP",
    -- "RS",
    -- "KS",
    -- "BIK",
    -- "Bank",
    -- "RS2",
    -- "KS2",
    -- "BIK2",
    -- "Bank2",
    -- "BuhName",
    -- "NextInvoiceNomer",
    -- "NDS",
    -- "BaseCode",
    -- "ID_Ref",
    -- "ToExport"
   FROM bergauto."Bosses"
  WHERE ("ID" IN ( SELECT DISTINCT operations.id_seller
           FROM bergapp.operations))
WITH DATA;
