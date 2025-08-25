CREATE OR REPLACE VIEW MODELS.KERING_GLOBE.CRT_BILLING_VAS (
    VERSION, QUANTITY_PRE, QUANTITY_FIX, ACTIVITY_DATE, PREPARATION_CHILD, PREPARATION_MOTHER, 
    SAP_ORDERID, VAS_CLUSTER, BRAND, OWNER, GRADE,
    P1CDPO, P1CACT, P1NANP, P1NPRE, VAS_CODE, UPDATED_TS  -- Added for procedure compatibility and filtering
) AS
-- Optimized CTE Structure (KER)
-- This view is refactored into clear chunks for readability and testability.
WITH
/* Chunk 05: params — optional dev/test gates
   How it fits: These parameters enable on-demand pruning of data when testing.
   - Set START_TS and END_TS to filter on UPDATED_TS (timestamp of changes).
   - Uncomment and set values for testing (e.g., to load data between specific dates). Comment out for full load.
   - Key filters (e.g., FILTER_DOC) are optional for narrowing by business keys.
   Leave them NULL in production to disable filtering.
*/
params AS (
  SELECT
    CAST(NULL AS TIMESTAMP_NTZ) AS START_TS,  -- e.g., '2025-01-01 00:00:00'; NULL disables lower bound
    CAST(NULL AS TIMESTAMP_NTZ) AS END_TS,    -- e.g., '2025-02-01 00:00:00'; NULL disables upper bound
    CAST(NULL AS VARCHAR)       AS FILTER_DOC -- e.g., '4500000000'; NULL disables
),
/* Chunk 20: source_select — wraps the original view logic
   How it fits: Encapsulates the entire original SELECT (including WITH clauses).
   - Computes core business logic: aggregates prep quantities, un pivots VAS codes, joins for clusters/brands.
   - Adds P1* keys, VAS_CODE, and UPDATED_TS (approximated as MAX(SH.HVR_CHANGE_TIME) over groups) for procedure and filtering.
   - Role: Provides the raw computed data before any dev filtering.
   Performance: Aggregation happens here; joins are pushed down.
*/
source_select AS (
  WITH PREPLINES AS (
    -- Role: Aggregates positive prep quantities from picked lines, joining shipments for date filtering.
    -- Filters to recent activity based on OBJECT_RUN (assumes table exists in Snowflake).
    SELECT P1CDPO, P1CACT, P1NANP, P1NPRE, P1CART, P1CPRP, P1CQAL, SUM(P1QPRE) AS P1QPRE
    FROM MODELS.KERING_GLOBE.HLPRPLP P1
    INNER JOIN MODELS.KERING_GLOBE.KBSHIPP SH ON SH.SHCDEP = P1.P1CDPO AND SH.SHCACT = P1.P1CACT AND SH.SHNANN = P1.P1NANP AND SH.SHNPRP = P1.P1NPRE
    WHERE P1QPRE > 0
      AND SH.SHDTKP >= (SELECT DATEADD(DAY, -1, LAST_UPDATE) FROM SMART_BI.OBJECT_RUN WHERE "OBJECT" = 'FACT_BILLING_VAS')  -- Adjusted for Snowflake
    GROUP BY P1CDPO, P1CACT, P1NANP, P1NPRE, P1CART, P1CPRP, P1CQAL
  ),
  VASCODE AS (
    -- Role: Unpivots VAS columns, aggregates quantities, derives type/cluster.
    -- Joins to extract non-null VAS codes from picked lines only.
    SELECT P1CDPO, P1CACT, P1NANP, P1NPRE, CLCART, CLRCDE, CLNANP, CLNPRE, SUM(CLQPPP) AS QTY_VAS_LANCIATA, 
           PREPA_TYPE, VAS_CODE, REPLACE(SUBSTR(VTDVAS, INSTR(VTDVAS, '#') + 1), ')') AS VAS_CLUSTER
    FROM (
      -- Unpivot VAS columns (CLVA01 to CLVA25).
      SELECT P1CDPO, P1CACT, P1NANP, P1NPRE, CLCART, CLRCDE, CLNANP, CLNPRE, CLQPPP, PREPA_TYPE, TRIM(SUBSTR(VAS_CODE, 1, 7)) AS VAS_CODE
      FROM (
        -- Extract non-null VAS codes per prep line, filtered by picked lines.
        SELECT P1.P1CDPO, P1.P1CACT, P1.P1NANP, P1.P1NPRE, CL.*, 
               CASE WHEN KP_CHILD.KPRODP IS NULL THEN 'MOTHER' ELSE 'CHILD' END AS PREPA_TYPE
        FROM PREPLINES P1
        LEFT JOIN (SELECT KPCDEP, KPCACT, KPRODP, KPNAOR, KPNPOR, KPNACR, KPNPCR FROM MODELS.KERING_GLOBE.KBSPLIP GROUP BY KPCDEP, KPCACT, KPRODP, KPNAOR, KPNPOR, KPNACR, KPNPCR) KP_CHILD
          ON KP_CHILD.KPCDEP = P1.P1CDPO AND KP_CHILD.KPCACT = P1.P1CACT AND KP_CHILD.KPNACR = P1.P1NANP AND KP_CHILD.KPNPCR = P1.P1NPRE
        LEFT JOIN (SELECT KPCDEP, KPCACT, KPRODP, KPNAOR, KPNPOR FROM MODELS.KERING_GLOBE.KBSPLIP GROUP BY KPCDEP, KPCACT, KPRODP, KPNAOR, KPNPOR) KP_MOTHER 
          ON KP_MOTHER.KPCDEP = P1.P1CDPO AND KP_MOTHER.KPCACT = P1.P1CACT AND KP_MOTHER.KPNAOR = P1.P1NANP AND KP_MOTHER.KPNPOR = P1.P1NPRE
        LEFT JOIN (
          SELECT CLCDPO, CLCACT, CLRCDE, CLNANP, CLNPRE, CLNLPR, CLCART, SUM(CLQPPP) CLQPPP,
                 CLVA01, CLVA02, CLVA03, CLVA04, CLVA05, CLVA06, CLVA07, CLVA08, CLVA09, CLVA10,
                 CLVA11, CLVA12, CLVA13, CLVA14, CLVA15, CLVA16, CLVA17, CLVA18, CLVA19, CLVA20,
                 CLVA21, CLVA22, CLVA23, CLVA24, CLVA25
          FROM MODELS.KERING_GLOBE.KBMCDLP WHERE CLSLAN <> ' '
          GROUP BY CLCDPO, CLCACT, CLRCDE, CLNANP, CLNPRE, CLNLPR, CLCART, 
                   CLVA01, CLVA02, CLVA03, CLVA04, CLVA05, CLVA06, CLVA07, CLVA08, CLVA09, CLVA10,
                   CLVA11, CLVA12, CLVA13, CLVA14, CLVA15, CLVA16, CLVA17, CLVA18, CLVA19, CLVA20,
                   CLVA21, CLVA22, CLVA23, CLVA24, CLVA25
        ) CL ON CL.CLCACT = P1.P1CACT AND CL.CLCDPO = P1.P1CDPO AND CL.CLCART = P1.P1CART
                AND CL.CLRCDE = COALESCE(KP_CHILD.KPRODP, KP_MOTHER.KPRODP)
                AND CL.CLNANP = COALESCE(KP_CHILD.KPNAOR, KP_MOTHER.KPNAOR) 
                AND CL.CLNPRE = COALESCE(KP_CHILD.KPNPOR, KP_MOTHER.KPNPOR)
      ) UNPIVOT (VAS_CODE FOR POSITION IN (
        CLVA01, CLVA02, CLVA03, CLVA04, CLVA05, CLVA06, CLVA07, CLVA08, CLVA09, CLVA10,
        CLVA11, CLVA12, CLVA13, CLVA14, CLVA15, CLVA16, CLVA17, CLVA18, CLVA19, CLVA20,
        CLVA21, CLVA22, CLVA23, CLVA24, CLVA25
      ))
      WHERE TRIM(SUBSTR(VAS_CODE, 1, 7)) IS NOT NULL
    ) LEFT JOIN MODELS.KERING_GLOBE.KBTVASP ON VAS_CODE = TRIM(VTCTVA)
    GROUP BY P1CDPO, P1CACT, P1NANP, P1NPRE, CLCART, CLRCDE, CLNANP, CLNPRE, PREPA_TYPE, VAS_CODE, REPLACE(SUBSTR(VTDVAS, INSTR(VTDVAS, '#') + 1), ')')
  )
  SELECT
    '1.0' AS VERSION,  -- Constant version; update GROUP BY if changed
    SUM(P1.P1QPRE) AS QUANTITY_PRE,
    SUM(CASE WHEN VS.QTY_VAS_LANCIATA > P1.P1QPRE THEN P1.P1QPRE ELSE VS.QTY_VAS_LANCIATA END) AS QUANTITY_FIX,
    TRUNC(SH.SHDTKP) AS ACTIVITY_DATE,
    COALESCE(LPAD(P1.P1NANP, 2, 0) || '/' || LPAD(P1.P1NPRE, 9, 0), ' ') AS PREPARATION_CHILD,
    COALESCE(LPAD(VS.CLNANP, 2, 0) || '/' || LPAD(VS.CLNPRE, 9, 0), ' ') AS PREPARATION_MOTHER,
    VS.CLRCDE AS SAP_ORDERID,
    VS.VAS_CLUSTER,
    CASE 
      WHEN CA.CABRDC = '12' THEN 'GG'
      WHEN CA.CABRDC = '13' THEN 'BV'
      WHEN CA.CABRDC = '18' THEN 'BAL'
      WHEN CA.CABRDC = '14' THEN 'YSL'
      WHEN CA.CABRDC = '15' THEN 'AMQ' 
      ELSE TO_CHAR(CA.CABRDC) 
    END AS BRAND,
    COALESCE(P1.P1CPRP, ' ') AS OWNER,
    COALESCE(P1.P1CQAL, ' ') AS GRADE,
    -- Added for procedure: Keys and timestamp
    P1.P1CDPO,
    P1.P1CACT,
    P1.P1NANP,
    P1.P1NPRE,
    VS.VAS_CODE,
    MAX(SH.HVR_CHANGE_TIME) AS UPDATED_TS  -- Approximation; captures shipment changes (main activity driver)
  FROM PREPLINES P1
  INNER JOIN VASCODE VS ON P1.P1CDPO = VS.P1CDPO AND P1.P1CACT = VS.P1CACT AND P1.P1NANP = VS.P1NANP AND P1.P1NPRE = VS.P1NPRE AND P1.P1CART = VS.CLCART
  INNER JOIN MODELS.KERING_GLOBE.KBSHIPP SH ON SH.SHCDEP = P1.P1CDPO AND SH.SHCACT = P1.P1CACT AND SH.SHNANN = P1.P1NANP AND SH.SHNPRP = P1.P1NPRE
  INNER JOIN MODELS.KERING_GLOBE.HLPRENP PE ON PE.PECDPO = P1.P1CDPO AND PE.PECACT = P1.P1CACT AND PE.PENANN = P1.P1NANP AND PE.PENPRE = P1.P1NPRE
  LEFT JOIN MODELS.KERING_GLOBE.KBCARTP CA ON P1.P1CART = CA.CASKUC
  WHERE NOT (PE.PET1PP = 0 OR (PE.PETSOL = '1' AND PE.PET1PP > 0))  -- Exclude cancels
    AND SH.SHTTKP = 1  -- TK05 pack exists
    AND SH.SHTHMK <> 1
  GROUP BY
    '1.0',
    TRUNC(SH.SHDTKP),
    COALESCE(LPAD(P1.P1NANP, 2, 0) || '/' || LPAD(P1.P1NPRE, 9, 0), ' '),
    COALESCE(LPAD(VS.CLNANP, 2, 0) || '/' || LPAD(VS.CLNPRE, 9, 0), ' '),
    VS.CLRCDE,
    VS.VAS_CLUSTER,
    CASE 
      WHEN CA.CABRDC = '12' THEN 'GG'
      WHEN CA.CABRDC = '13' THEN 'BV'
      WHEN CA.CABRDC = '18' THEN 'BAL'
      WHEN CA.CABRDC = '14' THEN 'YSL'
      WHEN CA.CABRDC = '15' THEN 'AMQ' 
      ELSE TO_CHAR(CA.CABRDC) 
    END,
    COALESCE(P1.P1CPRP, ' '),
    COALESCE(P1.P1CQAL, ' '),
    -- Added for grouping on keys
    P1.P1CDPO,
    P1.P1CACT,
    P1.P1NANP,
    P1.P1NPRE,
    VS.VAS_CODE
),
/* Chunk 30: dev_filters — optional filters for test runs
   How it fits: Applies time-based gates (START_TS to END_TS) and optional key gates.
   - Role: Prunes rows early for dev/testing, reducing compute. No-op in prod if params NULL.
   Performance: Pushes down filters to limit scanned data.
*/
dev_filters AS (
  SELECT s.*
  FROM source_select s, params p
  WHERE (p.START_TS IS NULL OR s.UPDATED_TS >= p.START_TS)
    AND (p.END_TS IS NULL OR s.UPDATED_TS < p.END_TS)  -- < for exclusive upper bound
    AND (p.FILTER_DOC IS NULL OR s.SAP_ORDERID = p.FILTER_DOC)
)
/* Chunk 99: final — return filtered or full results
   How it fits: Exposes the final rows; if params are NULL, behaves like the original view.
*/
SELECT *
FROM dev_filters;
