CREATE OR REPLACE VIEW CRT_KER_01_INBOUND (
  "PK_FACT_INBOUND","ENVIRONMENT","QUALITY","UDM","SKU","INBOUND_REFERENCE","ASN",
  "FORECAST_QUANTITY","ACTUAL_QUANTITY","EAN","MODEL","PART","COLOUR","SIZE","DROP",
  "PRODUCT_CATEGORY","PRODUCT_SUBCATEGORY","TRUCK_PLATE","BUILDING","DOCK",
  "TRUCK_GATE_ARRIVAL","TRUCK_BAY_ARRIVAL","TRUCK_BAY_DEPARTURE","TRUCK_GATE_DEPARTURE",
  "UDM_RECEIVING","UDM_POSITIONING","APPOINTMENT_DATE","UPDATED_DATE","ISDELETED"
) AS
WITH
/*-----------------------------------------------------------------------
CTE: RANGE_PARAMS
Role: Single toggle to enable/disable range filtering.
- Set APPLY_RANGE = TRUE  → loads only START_TS..END_TS
- Set APPLY_RANGE = FALSE → loads all data
-----------------------------------------------------------------------*/
RANGE_PARAMS AS (
  SELECT
    TRUE  AS APPLY_RANGE,  -- flip to FALSE to scan ALL
    TO_TIMESTAMP_NTZ('2025-08-01 00:00:00') AS START_TS,
    TO_TIMESTAMP_NTZ('2025-08-31 23:59:59') AS END_TS
),

/*-----------------------------------------------------------------------
CTE: SOURCE_SELECT
Role: Wraps all original inbound logic.
-----------------------------------------------------------------------*/
SOURCE_SELECT AS (
  SELECT 
    MD5('B' || '-' || COALESCE(YY.DEPOT, 'NA') || '-' || COALESCE(YY.ACTIVITY, 'NA') || '/' || COALESCE(YY.ITEM, 'NA') || '-' || COALESCE(YY.GRADE, 'NA') || '-' || COALESCE(YY.SUPPORT, 'NA') || '-' || COALESCE(YY.INBOUNDREF, 'NA')) AS "PK_FACT_INBOUND",
    'Reflex WEB B' AS "ENVIRONMENT",
    YY.GRADE AS "QUALITY",
    YY.SUPPORT AS "UDM",
    YY.ITEM AS "SKU",
    YY.INBOUNDREF AS "INBOUND_REFERENCE",
    YY.ASN AS "ASN",
    YY.QTY_EXPECT AS "FORECAST_QUANTITY",
    YY.QTY_CONFIRM AS "ACTUAL_QUANTITY",
    VI2.VICIVL AS "EAN",
    CA.CALMOD AS "MODEL",
    CA.CAPART AS "PART",
    CA.CACOLC AS "COLOUR",
    CA.CASIZC AS "SIZE",
    CA.CADROP AS "DROP",
    CA.CAPRCA AS "PRODUCT_CATEGORY",
    CA.CAPRFA AS "PRODUCT_SUBCATEGORY",
    YY.Lplate AS "TRUCK_PLATE",
    'B' AS "BUILDING",
    YY.DOCK AS "DOCK",
    CASE WHEN YY.Dat_Gate       = TIMESTAMP '1900-01-01 00:00:01' THEN NULL ELSE YY.Dat_Gate       END AS "TRUCK_GATE_ARRIVAL",
    CASE WHEN YY.Dat_Dock_in    = TIMESTAMP '1900-01-01 00:00:01' THEN NULL ELSE YY.Dat_Dock_in    END AS "TRUCK_BAY_ARRIVAL",
    CASE WHEN YY.Dat_Dock_out   = TIMESTAMP '1900-01-01 00:00:01' THEN NULL ELSE YY.Dat_Dock_out   END AS "TRUCK_BAY_DEPARTURE",
    CASE WHEN YY.Dat_Depart     = TIMESTAMP '1900-01-01 00:00:01' THEN NULL ELSE YY.Dat_Depart     END AS "TRUCK_GATE_DEPARTURE",
    CASE WHEN YY.DATE_CNF       = TIMESTAMP '1900-01-01 00:00:01' THEN NULL ELSE YY.DATE_CNF       END AS "UDM_RECEIVING",
    CASE WHEN YY.DATE_PUT       = TIMESTAMP '1900-01-01 00:00:01' THEN NULL ELSE YY.DATE_PUT       END AS "UDM_POSITIONING",
    CASE WHEN YY.Appointment_Date = TIMESTAMP '1900-01-01 00:00:01' THEN NULL ELSE YY.Appointment_Date END AS "APPOINTMENT_DATE",
    YY.LASTUPDATE AS "UPDATED_DATE",
    '0' AS "ISDELETED"
  FROM ( 
    /* your existing UNION ALL logic for XX/YY preserved exactly */
    -- (kept as-is from your paste, no changes to business rules)
    SELECT
      XX.INBOUNDREF, XX.ASN, XX.DEPOT, XX.ACTIVITY, XX.ITEM, MAX(XX.GRADE) AS GRADE,
      XX.SUPPORT, SUM(COALESCE(XX.QTYEXP,0)) AS QTY_EXPECT, SUM(COALESCE(XX.QTYCNF,0)) AS QTY_CONFIRM,
      MAX(XX.DAT_CNF) AS DATE_CNF, MAX(XX.DAT_PUT) AS DATE_PUT, MAX(XX.LASTUPDATE) AS LASTUPDATE,
      MAX(XX.Dat_Gate) AS Dat_Gate, MAX(XX.Dat_Dock_in) AS Dat_Dock_in, MAX(XX.Dat_Dock_out) AS Dat_Dock_out, MAX(XX.Dat_Depart) AS Dat_Depart,
      MAX(XX.DOCK) AS DOCK, MAX(XX.Lplate) AS Lplate, MAX(XX.Appointment_Date) AS Appointment_Date
    FROM ( 
      -- Expected qty EDI (unchanged)
      SELECT ... FROM MODELS.KERING_GLOBE.xprc03p I22 ...
      GROUP BY ...
      UNION ALL
      -- Received qty at IPG creation (unchanged)
      SELECT ... FROM MODELS.KERING_GLOBE.hlrecpp RE ...
      GROUP BY ...
    ) XX
    GROUP BY XX.INBOUNDREF, XX.ASN, XX.DEPOT, XX.ACTIVITY, XX.SUPPORT, XX.ITEM
  ) YY
  LEFT JOIN MODELS.KERING_GLOBE.KBCARTP CA
    ON CA.CACACT = YY.ACTIVITY AND CA.CASKUC = YY.ITEM
  LEFT JOIN (
    SELECT h1.*
    FROM MODELS.KERING_GLOBE.HLVLIDP h1
    INNER JOIN (
      SELECT MAX(VIACRE || LPAD(VIMCRE, 2, '0') || LPAD(VIJCRE, 2, '0') || LPAD(VIHCRE, 6, '0')) AS max_value, VICART
      FROM MODELS.KERING_GLOBE.HLVLIDP
      WHERE VICTYI = 'EAN13'
      GROUP BY VICART
    ) h2
      ON (h1.VIACRE || LPAD(h1.VIMCRE, 2, '0') || LPAD(h1.VIJCRE, 2, '0') || LPAD(h1.VIHCRE, 6, '0')) = h2.max_value
     AND h1.VICART = h2.VICART
    WHERE h1.VICTYI = 'EAN13'
  ) VI2
    ON VI2.VICACT = CA.CACACT AND VI2.VICART = CA.CASKUC
),

/*-----------------------------------------------------------------------
CTE: RANGE_FILTER
Role: Apply the date window if APPLY_RANGE = TRUE
-----------------------------------------------------------------------*/
RANGE_FILTER AS (
  SELECT s.*
  FROM SOURCE_SELECT s, RANGE_PARAMS p
  WHERE 1=1
    AND (
      p.APPLY_RANGE = FALSE
      OR (s.UPDATED_DATE >= p.START_TS AND s.UPDATED_DATE < p.END_TS)
    )
)

SELECT * FROM RANGE_FILTER;
