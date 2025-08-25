CREATE OR REPLACE PROCEDURE MODELS.KERING_GLOBE.SP_KER_02_OUTBOUND_LOAD(
    p_start_ts   TIMESTAMP_NTZ,
    p_end_ts     TIMESTAMP_NTZ,
    p_full_reload BOOLEAN DEFAULT FALSE
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_start TIMESTAMP_NTZ := p_start_ts;
    v_end   TIMESTAMP_NTZ := p_end_ts;
BEGIN
    ----------------------------------------------------------------------
    -- 0) Safety checks
    ----------------------------------------------------------------------
    IF v_start IS NULL AND NOT p_full_reload THEN
        RAISE STATEMENT_ERROR USING
          MESSAGE = 'p_start_ts is required when p_full_reload = FALSE';
    END IF;

    IF v_end IS NULL AND NOT p_full_reload THEN
        RAISE STATEMENT_ERROR USING
          MESSAGE = 'p_end_ts is required when p_full_reload = FALSE';
    END IF;

    ----------------------------------------------------------------------
    -- 1) Ensure target table exists (schema = view output)
    --    If it doesn't exist, create it empty but with the same columns.
    ----------------------------------------------------------------------
    EXECUTE IMMEDIATE $$
        CREATE TABLE IF NOT EXISTS MODELS.KERING_GLOBE.FCT_KER_01_INBOUND AS
        SELECT *
        FROM MODELS.KERING_GLOBE.CRT_KER_01_INBOUND
        WHERE 1=0
    $$;

    ----------------------------------------------------------------------
    -- 2) Stage source rows (fix: use UPDATED_DATE instead of creation_order_date)
    --    NOTE: we *also* filter here even if the view has its own range guard.
    ----------------------------------------------------------------------
    CREATE OR REPLACE TEMP TABLE _SRC AS
    SELECT *
    FROM MODELS.KERING_GLOBE.CRT_KER_01_INBOUND S
    WHERE p_full_reload
          OR (S.UPDATED_DATE >= v_start AND S.UPDATED_DATE < v_end);

    ----------------------------------------------------------------------
    -- 3) MERGE into target on business key (PK_FACT_INBOUND)
    --    Update on any material change; insert new rows.
    ----------------------------------------------------------------------
    MERGE INTO MODELS.KERING_GLOBE.FCT_KER_01_INBOUND T
    USING _SRC S
       ON T.PK_FACT_INBOUND = S.PK_FACT_INBOUND
    WHEN MATCHED AND (
           NVL(T.ENVIRONMENT,'')           <> NVL(S.ENVIRONMENT,'')
        OR NVL(T.QUALITY,'')               <> NVL(S.QUALITY,'')
        OR NVL(T.UDM,'')                   <> NVL(S.UDM,'')
        OR NVL(T.SKU,'')                   <> NVL(S.SKU,'')
        OR NVL(T.INBOUND_REFERENCE,'')     <> NVL(S.INBOUND_REFERENCE,'')
        OR NVL(T.ASN,'')                   <> NVL(S.ASN,'')
        OR NVL(T.FORECAST_QUANTITY,0)      <> NVL(S.FORECAST_QUANTITY,0)
        OR NVL(T.ACTUAL_QUANTITY,0)        <> NVL(S.ACTUAL_QUANTITY,0)
        OR NVL(T.EAN,'')                   <> NVL(S.EAN,'')
        OR NVL(T.MODEL,'')                 <> NVL(S.MODEL,'')
        OR NVL(T.PART,'')                  <> NVL(S.PART,'')
        OR NVL(T.COLOUR,'')                <> NVL(S.COLOUR,'')
        OR NVL(T.SIZE,'')                  <> NVL(S.SIZE,'')
        OR NVL(T.DROP,'')                  <> NVL(S.DROP,'')
        OR NVL(T.PRODUCT_CATEGORY,'')      <> NVL(S.PRODUCT_CATEGORY,'')
        OR NVL(T.PRODUCT_SUBCATEGORY,'')   <> NVL(S.PRODUCT_SUBCATEGORY,'')
        OR NVL(T.TRUCK_PLATE,'')           <> NVL(S.TRUCK_PLATE,'')
        OR NVL(T.BUILDING,'')              <> NVL(S.BUILDING,'')
        OR NVL(T.DOCK,'')                  <> NVL(S.DOCK,'')
        OR NVL(T.TRUCK_GATE_ARRIVAL,TO_TIMESTAMP_NTZ('1900-01-01 00:00:01')) <> NVL(S.TRUCK_GATE_ARRIVAL,TO_TIMESTAMP_NTZ('1900-01-01 00:00:01'))
        OR NVL(T.TRUCK_BAY_ARRIVAL, TO_TIMESTAMP_NTZ('1900-01-01 00:00:01')) <> NVL(S.TRUCK_BAY_ARRIVAL, TO_TIMESTAMP_NTZ('1900-01-01 00:00:01'))
        OR NVL(T.TRUCK_BAY_DEPARTURE,TO_TIMESTAMP_NTZ('1900-01-01 00:00:01')) <> NVL(S.TRUCK_BAY_DEPARTURE,TO_TIMESTAMP_NTZ('1900-01-01 00:00:01'))
        OR NVL(T.TRUCK_GATE_DEPARTURE,TO_TIMESTAMP_NTZ('1900-01-01 00:00:01')) <> NVL(S.TRUCK_GATE_DEPARTURE,TO_TIMESTAMP_NTZ('1900-01-01 00:00:01'))
        OR NVL(T.UDM_RECEIVING,TO_TIMESTAMP_NTZ('1900-01-01 00:00:01'))       <> NVL(S.UDM_RECEIVING,TO_TIMESTAMP_NTZ('1900-01-01 00:00:01'))
        OR NVL(T.UDM_POSITIONING,TO_TIMESTAMP_NTZ('1900-01-01 00:00:01'))     <> NVL(S.UDM_POSITIONING,TO_TIMESTAMP_NTZ('1900-01-01 00:00:01'))
        OR NVL(T.APPOINTMENT_DATE,TO_TIMESTAMP_NTZ('1900-01-01 00:00:01'))    <> NVL(S.APPOINTMENT_DATE,TO_TIMESTAMP_NTZ('1900-01-01 00:00:01'))
        OR NVL(T.UPDATED_DATE,TO_TIMESTAMP_NTZ('1900-01-01 00:00:01'))        <> NVL(S.UPDATED_DATE,TO_TIMESTAMP_NTZ('1900-01-01 00:00:01'))
        OR NVL(T.ISDELETED,'')             <> NVL(S.ISDELETED,'')
    )
    THEN UPDATE SET
        ENVIRONMENT         = S.ENVIRONMENT,
        QUALITY             = S.QUALITY,
        UDM                 = S.UDM,
        SKU                 = S.SKU,
        INBOUND_REFERENCE   = S.INBOUND_REFERENCE,
        ASN                 = S.ASN,
        FORECAST_QUANTITY   = S.FORECAST_QUANTITY,
        ACTUAL_QUANTITY     = S.ACTUAL_QUANTITY,
        EAN                 = S.EAN,
        MODEL               = S.MODEL,
        PART                = S.PART,
        COLOUR              = S.COLOUR,
        SIZE                = S.SIZE,
        DROP                = S.DROP,
        PRODUCT_CATEGORY    = S.PRODUCT_CATEGORY,
        PRODUCT_SUBCATEGORY = S.PRODUCT_SUBCATEGORY,
        TRUCK_PLATE         = S.TRUCK_PLATE,
        BUILDING            = S.BUILDING,
        DOCK                = S.DOCK,
        TRUCK_GATE_ARRIVAL  = S.TRUCK_GATE_ARRIVAL,
        TRUCK_BAY_ARRIVAL   = S.TRUCK_BAY_ARRIVAL,
        TRUCK_BAY_DEPARTURE = S.TRUCK_BAY_DEPARTURE,
        TRUCK_GATE_DEPARTURE= S.TRUCK_GATE_DEPARTURE,
        UDM_RECEIVING       = S.UDM_RECEIVING,
        UDM_POSITIONING     = S.UDM_POSITIONING,
        APPOINTMENT_DATE    = S.APPOINTMENT_DATE,
        UPDATED_DATE        = S.UPDATED_DATE,
        ISDELETED           = S.ISDELETED
    WHEN NOT MATCHED THEN
        INSERT (
            PK_FACT_INBOUND, ENVIRONMENT, QUALITY, UDM, SKU, INBOUND_REFERENCE, ASN,
            FORECAST_QUANTITY, ACTUAL_QUANTITY, EAN, MODEL, PART, COLOUR, SIZE, DROP,
            PRODUCT_CATEGORY, PRODUCT_SUBCATEGORY, TRUCK_PLATE, BUILDING, DOCK,
            TRUCK_GATE_ARRIVAL, TRUCK_BAY_ARRIVAL, TRUCK_BAY_DEPARTURE, TRUCK_GATE_DEPARTURE,
            UDM_RECEIVING, UDM_POSITIONING, APPOINTMENT_DATE, UPDATED_DATE, ISDELETED
        )
        VALUES (
            S.PK_FACT_INBOUND, S.ENVIRONMENT, S.QUALITY, S.UDM, S.SKU, S.INBOUND_REFERENCE, S.ASN,
            S.FORECAST_QUANTITY, S.ACTUAL_QUANTITY, S.EAN, S.MODEL, S.PART, S.COLOUR, S.SIZE, S.DROP,
            S.PRODUCT_CATEGORY, S.PRODUCT_SUBCATEGORY, S.TRUCK_PLATE, S.BUILDING, S.DOCK,
            S.TRUCK_GATE_ARRIVAL, S.TRUCK_BAY_ARRIVAL, S.TRUCK_BAY_DEPARTURE, S.TRUCK_GATE_DEPARTURE,
            S.UDM_RECEIVING, S.UDM_POSITIONING, S.APPOINTMENT_DATE, S.UPDATED_DATE, S.ISDELETED
        );

    ----------------------------------------------------------------------
    -- 4) Return a small payload for orchestration logs
    ----------------------------------------------------------------------
    RETURN OBJECT_CONSTRUCT(
        'status', 'OK',
        'full_reload', p_full_reload,
        'start_ts', v_start,
        'end_ts', v_end,
        'src_rows', (SELECT COUNT(*) FROM _SRC)
    );
END;
$$;
