create or replace view CRT_KER_02_OUTBOUND as
with

/*-----------------------------------------------------------------------
CTE: PREP_LINES
Role: Pre-aggregates preparation-line level info for each order detail:
      - grade (client+quality), qty ordered, wave string, product category,
        flag for packaging jewelry, and a roll-up change timestamp.
Inputs: MODELS.KERING_GLOBE.HLPRPLP (P1), KBCARTP (CA), HLLANCP (LC)
Output grain: (PECDOD, P1CACT, P1NANO, P1NODP, P1CDPO, P1NANP, P1NPRE)
-----------------------------------------------------------------------*/
PREP_LINES as (
    select
        P1.PECDOD,
        P1.P1CACT,
        P1.P1NANO,
        P1.P1NODP,
        P1.P1CDPO,
        P1.P1NANP,
        P1.P1NPRE,
        max(P1.P1CPRP || P1.P1CQAL) as GRADE,
        sum(P1.P1QODP) as QTY_ORDERED,
        min(
            case
              when P1.P1JOCA > 0 then
                lpad(P1.P1SSCA, 2, '0') || lpad(P1.P1ANCA, 2, '0') || '-' ||
                lpad(P1.P1MOCA, 2, '0') || '-' || lpad(P1.P1JOCA, 2, '0') ||
                ' ' ||
                substr(lpad(coalesce(LC.LCHDCA,0)::varchar,6,'0'),1,2) || ':' ||
                substr(lpad(coalesce(LC.LCHDCA,0)::varchar,6,'0'),3,2) ||
                ' ' || P1.P1CDLA
              else ' '
            end
        ) as WAVE,
        max(CA.CAPRCA) as PRODCAT,
        max(case when P1.P1CART like '7%' then 1 else 0 end) as FLAG_PACKAGING_JEWELRY_SUB,
        max(P1.HVR_CHANGE_TIME) as LAST_UPDATE
    from MODELS.KERING_GLOBE.HLPRPLP P1
    join MODELS.KERING_GLOBE.KBCARTP CA
      on CA.CACACT = P1.P1CACT and P1.P1CART = CA.CASKUC
    left join MODELS.KERING_GLOBE.HLLANCP LC
      on LC.LCCDPO=P1.P1CDPO and LC.LCSSCA=P1.P1SSCA and LC.LCANCA=P1.P1ANCA
     and LC.LCMOCA=P1.P1MOCA and LC.LCJOCA=P1.P1JOCA and LC.LCCDLA=P1.P1CDLA
    where P1.P1CACT = '100' and P1.P1CDPO='001'
    group by P1.PECDOD, P1.P1CACT, P1.P1NANO, P1.P1NODP, P1.P1CDPO, P1.P1NANP, P1.P1NPRE
),

/*-----------------------------------------------------------------------
CTE: PE
Role: Header-level preparation entity; brings in free-text comments.
Inputs: HLPRENP (PE), HLCOMMP (CO)
Output grain: row per PE entry; filters to CACT=100, CDPO=001
-----------------------------------------------------------------------*/
PE as (
    select PE.*,
           CO.Commenti
    from MODELS.KERING_GLOBE.HLPRENP PE
    left join (
    --ERROR
            SELECT
        CONCOM, 
        LISTAGG(COTXTC, ' | ') WITHIN GROUP (ORDER BY CONLCO) AS Commenti
        FROM MODELS.KERING_GLOBE.HLCOMMP
        group by CONCOM
    ) CO
      on PE.PENCOM = CO.CONCOM
    where PE.PECACT='100' and PE.PECDPO='001'
),

/*-----------------------------------------------------------------------
CTE: HU
Role: TK35 transmission hour aggregation (per doc/line).
Inputs: KB35HUP
Output grain: (HUCDPO, HUDNUM, HUPORD) with TK35HEURE + LAST_UPDATE
-----------------------------------------------------------------------*/
HU as (
    select HUCDPO, HUDNUM, HUPORD, max(HUDMAJ) as TK35HEURE, max(HVR_CHANGE_TIME) as LAST_UPDATE
    from MODELS.KERING_GLOBE.KB35HUP
    where HUCDPO = '001'
    group by HUCDPO, HUDNUM, HUPORD
),

/*-----------------------------------------------------------------------
CTE: PREP_LINES_QTY
Role: Quantities (packed, DMS2, carton/parcel counts, loaded pieces).
Inputs: HLLPRGP (LG), HLGEINP (GE), HLSUPPP (SU), HLEMPLP (EM), HLPRPLP (P1)
Output grain: (P1CDPO,P1CACT,P1NANP,P1NPRE)
-----------------------------------------------------------------------*/
PREP_LINES_QTY as (
    select P1.P1CDPO, P1.P1CACT, P1.P1NANP, P1.P1NPRE,
           sum(LG.LGQLPG) as QTY_PACK,
           sum(case when EM.EMC1EM = 'DMS' then LG.LGQLPG else 0 end) as QTY_DMS2,
           count(distinct GE.GENCOL) as QTY_COL,
           count(distinct case when SU.SUTSCG = '1' then GE.GENCOL else null end) as QTY_COL_LOAD,
           sum(case when SU.SUTSCG = '1' then LG.LGQLPG else 0 end) as QTY_PIE_LOAD,
           max(coalesce(GE.HVR_CHANGE_TIME, LG.HVR_CHANGE_TIME, SU.HVR_CHANGE_TIME, EM.HVR_CHANGE_TIME)) as LAST_UPDATE
    from MODELS.KERING_GLOBE.HLPRPLP P1
    join MODELS.KERING_GLOBE.HLLPRGP LG
      on LG.LGCDPO = P1.P1CDPO and LG.LGCACT = P1.P1CACT and LG.LGNANN = P1.P1NANN and LG.LGNLPR = P1.P1NLPR
    join MODELS.KERING_GLOBE.HLGEINP GE
      on GE.GENGEI = LG.LGNGEI and LG.LGCDPO = GE.GECDPO and LG.LGCACT = GE.GECACT and GE.GENCOL <> ' '
    join MODELS.KERING_GLOBE.HLSUPPP SU
      on SU.SUCDPO = LG.LGCDPO and SU.SUNSUP = GE.GENSUP
    join MODELS.KERING_GLOBE.HLEMPLP EM
      on SU.SUCDPO = EM.EMCDPO and SU.SUNEMP = EM.EMNEMP
    group by P1.P1CDPO, P1.P1CACT, P1.P1NANP, P1.P1NPRE
),

/*-----------------------------------------------------------------------
CTE: DMS_SUB
Role: DMS quantity based on ARV mappings (value '03' and FPR='DMS').
Inputs: HLPRPLP, HLARVLP
Output grain: (P1CACT, P1CDPO, P1NANP, P1NPRE)
-----------------------------------------------------------------------*/
DMS_SUB as (
    select P1CACT, P1CDPO, P1NANP, P1NPRE, sum(P1QODP) as QTY_DMS
    from MODELS.KERING_GLOBE.HLPRPLP
    join MODELS.KERING_GLOBE.HLARVLP
      on P1CACT=VLCACT and P1CART=VLCART and VLCVLA = '03'
    where VLCFPR in ('DMS')
    group by P1CACT, P1CDPO, P1NANP, P1NPRE
),

/*-----------------------------------------------------------------------
CTE: GS
Role: Quantities for export scenario at shipment (exp cartons/pieces).
Inputs: HLGESOP (GS) joined to HLPRPLP for scope
Output grain: (GSCDPO, GSCACT, GSNAPP, GSNPRE)
-----------------------------------------------------------------------*/
GS as (
    select GS.GSCDPO, GS.GSCACT, GS.GSNAPP, GS.GSNPRE,
           count(distinct GS.GSNCOL) as QTY_COL_EXP,
           sum(GS.GSQGEI) as QTY_PIE_EXP,
           max(GS.HVR_CHANGE_TIME) as LAST_UPDATE
    from MODELS.KERING_GLOBE.HLPRPLP P1
    join MODELS.KERING_GLOBE.HLGESOP GS
      on GS.GSCACT = P1.P1CACT and GS.GSCDPO = P1.P1CDPO and GS.GSNALI = P1.P1NANN and GS.GSNLPR = P1.P1NLPR
    where P1.P1CACT='100' and P1.P1CDPO='001'
    group by GS.GSCDPO, GS.GSCACT, GS.GSNAPP, GS.GSNPRE
),

/*-----------------------------------------------------------------------
CTE: CO
Role: Determines whether order header was inserted/updated (SENT flag).
Inputs: KBDORDP (OR_), KBMCLPP (CO)
Output grain: distinct CORCDE values
-----------------------------------------------------------------------*/
CO as (
    select distinct CO.CORCDE
    from MODELS.KERING_GLOBE.KBDORDP OR_
    join MODELS.KERING_GLOBE.KBMCLPP CO
      on CO.COCDPO = OR_.ORCDPO and CO.COCACT = OR_.ORCACT and CO.COSTSE = OR_.ORORID
    where OR_.ORCDPO = '001' and OR_.ORCACT = '100'
),

/*-----------------------------------------------------------------------
CTE: SG1 / SG2
Role: Two decoding paths for shipping request detection (DONE flag).
Inputs: KBDSSLP (SG)
Output grain: decoded keys for joining to PE load fields
-----------------------------------------------------------------------*/
SG1 as (
    select substr(SG.SGSRID, 1, 6) as CGCOD,
           substr(SG.SGSRID, 7, 2) as CGSIE,
           substr(SG.SGSRID, 9, 2) as CGANN,
           substr(SG.SGSRID,11, 2) as CGMOI,
           substr(SG.SGSRID,13, 2) as CGJOU,
           max(SG.HVR_CHANGE_TIME) as LAST_UPDATE
    from MODELS.KERING_GLOBE.KBDSSLP SG
    where SG.SGCDPO = '001' and SG.SGCACT = '100' and substr(SG.SGSRID, 9, 1) <> '_'
    group by substr(SG.SGSRID, 1, 6), substr(SG.SGSRID,7,2), substr(SG.SGSRID,9,2), substr(SG.SGSRID,11,2), substr(SG.SGSRID,13,2)
),
SG2 as (
    select substr(SG.SGSRID,10, 6) as CGCOD,
           substr(SG.SGSRID, 1, 2) as CGSIE,
           substr(SG.SGSRID, 3, 2) as CGANN,
           substr(SG.SGSRID, 5, 2) as CGMOI,
           substr(SG.SGSRID, 7, 2) as CGJOU,
           max(SG.HVR_CHANGE_TIME) as LAST_UPDATE
    from MODELS.KERING_GLOBE.KBDSSLP SG
    where SG.SGCDPO = '001' and SG.SGCACT = '100' and substr(SG.SGSRID, 9, 1) = '_'
    group by substr(SG.SGSRID,10,6), substr(SG.SGSRID,1,2), substr(SG.SGSRID,3,2), substr(SG.SGSRID,5,2), substr(SG.SGSRID,7,2)
),

/*-----------------------------------------------------------------------
CTE: YMX
Role: YMS (yard management) timings for gate/bay/departure.
Inputs: GG_KERIYMS_PRD: TIEADRSLOTTASK, HISTORYMOVEMENT, DOCK, TIEADRSLOT
Output grain: by TDTREFB (appointment reference)
-----------------------------------------------------------------------*/
YMX as (
    select
      TD.TDTREFB,
      max(YMA.MO_ARRIVAL_DATE)   as MO_ARRIVAL_DATE,
      max(YMB.MO_BAY_DATE)       as MO_BAY_DATE,
      max(YMD.MO_DEPARTURE_DATE) as MO_DEPARTURE_DATE
    from TIEADRSLOTTASK TD
    left join (
        select YM1.IDSITE, YM1.IDDEPOT, YM1.MOSTATE, YM1.MOAPPOINTMENT,
               min(MOARRIVALDATE) as MO_ARRIVAL_DATE,
               max(YM1.MOLICENCEPLATE1) as MOLICENCEPLATE1,
               TS.TSLID
        from HISTORYMOVEMENT YM1
        join TIEADRSLOT TS
          on YM1.MOAPPOINTMENT = TS.TSLNUM
        where YM1.IDSITE = '1' and YM1.IDDEPOT = '4' and YM1.MOSTATE = 20
        group by YM1.IDSITE, YM1.IDDEPOT, YM1.MOSTATE, YM1.MOAPPOINTMENT, TS.TSLID
    ) YMA on YMA.TSLID = TD.TDTTSLID
    left join (
        select YM2.IDSITE, YM2.IDDEPOT, YM2.MOSTATE, YM2.MOAPPOINTMENT,
               min(YM2.HYTIMESTAMP) as MO_BAY_DATE,
               min(D.DONAME) as DOCK,
               TS.TSLID
        from HISTORYMOVEMENT YM2
        join DOCK D
          on D.IDDOCK = YM2.IDDOCK
        join TIEADRSLOT TS
          on YM2.MOAPPOINTMENT = TS.TSLNUM
        where YM2.IDSITE = '1' and YM2.IDDEPOT = '4' and YM2.MOSTATE = 60 and YM2.IDSTATUS = 3
        group by YM2.IDSITE, YM2.IDDEPOT, YM2.MOSTATE, YM2.MOAPPOINTMENT, TS.TSLID
    ) YMB on YMB.TSLID = TD.TDTTSLID
    left join (
        select YM4.IDSITE, YM4.IDDEPOT, YM4.MOSTATE, YM4.MOAPPOINTMENT,
               min(HYTIMESTAMP) as MO_DEPARTURE_DATE,
               min(D.DONAME) as DOCK,
               TS.TSLID
        from HISTORYMOVEMENT YM4
        join DOCK D
          on D.IDDOCK = YM4.IDDOCK
        join TIEADRSLOT TS
          on YM4.MOAPPOINTMENT = TS.TSLNUM
        where YM4.IDSITE = '1' and YM4.IDDEPOT = '4' and YM4.MOSTATE = 60 and YM4.IDSTATUS = 11
        group by YM4.IDSITE, YM4.IDDEPOT, YM4.MOSTATE, YM4.MOAPPOINTMENT, TS.TSLID
    ) YMBE on YMBE.TSLID = TD.TDTTSLID
    left join (
        select YM3.IDSITE, YM3.IDDEPOT, YM3.MOSTATE, YM3.MOAPPOINTMENT,
               min(YM3.MODEPARTUREDATE) as MO_DEPARTURE_DATE,
               TS.TSLID
        from HISTORYMOVEMENT YM3
        join TIEADRSLOT TS
          on YM3.MOAPPOINTMENT = TS.TSLNUM
        where YM3.IDSITE = '1' and YM3.IDDEPOT = '4' and YM3.MOSTATE = 99
        group by YM3.IDSITE, YM3.IDDEPOT, YM3.MOSTATE, YM3.MOAPPOINTMENT, TS.TSLID
    ) YMD on YMD.TSLID = TD.TDTTSLID
    group by TD.TDTREFB
),

/*-----------------------------------------------------------------------
CTE: BASE
Role: Single, normalized dataset with all joins performed and a
      computed per-row CHANGE_TS used for optional filtering and exposed
      as UPDATED_DATE in the endpoint.
Output grain: one row per outbound fact.
-----------------------------------------------------------------------*/
BASE as (
select
    -- PK
    UPPER(md5('B' || OE.OENANN || OE.OENODP || PE.PECACT || PE.PECDPO || PE.PERODP || PE.PENANN || PE.PENPRE)) as PK_FACT_OUTBOUND,

    -- Timestamps (rendered as strings to keep parity with original)
    to_char(MOTHER.SHDCOR,'DD/MM/YYYY') as INTEGRATION_DATE,
    to_char(MOTHER.SHDCOR,'HH24:MI')    as INTEGRATION_TIME,

    -- Static context
    'Reflex WEB B' as ENVIRONMENT,
    'B' as BUILDING,

    -- Order/Client detail
    substr(PREP_LINES.GRADE, 1, 3) as REFLEX_CLIENT,
    PE.PECDES as REFLEX_DESTINATION,
    substr(PREP_LINES.WAVE, 1, 16) as WAVING_DATE,
    substr(PREP_LINES.WAVE, 18, 3) as WAVING_CODE,

    -- Status flags inferred
    case when CO.CORCDE is null then ' ' else 'SENT' end as ORDER_INSERT_UPDATE,
    case when coalesce(SG1.CGCOD,SG2.CGCOD) is null then ' ' else 'DONE' end as SHIPPING_REQUEST,

    -- Brand/SAP references
    case when MOTHER.SHCDOR is null then CHILD.SHCDOR else MOTHER.SHCDOR end as BRAND,
    case when MOTHER.SHRDOR is null then CHILD.SHRDOR else MOTHER.SHRDOR end as SAP_ORDERID,
    OE.OECDES as CUSTOMER_CODE,
    case when MOTHER.SHPAYD is null then CHILD.SHPAYD else MOTHER.SHPAYD end as COUNTRY,

    -- Carrier / transport
    MOTHER.SHCTRP as SAP_CARRIER,
    CG.CGCTRP as REFLEX_CARRIER,
    TP.TPLTRP as CARRIER_NAME,
    substr(PREP_LINES.GRADE, 1, 3) as OWNER,
    substr(PREP_LINES.GRADE, 4, 3) as QUALITY,

    -- Doc numbers
    case when CHILD.SHCACT is null then lpad(PE.PENANN, 2 , '0') || '/' || lpad(PE.PENPRE, 9, '0')
         else lpad(CHILD.SHNANN, 2 , '0') || '/' || lpad(CHILD.SHNPRP, 9, '0') end as PREPARATION_NUMBER,

    -- Dates / codes
    case when MOTHER.SHDDEL is null then to_char(CHILD.SHDDEL, 'DD/MM/YYYY') else to_char(MOTHER.SHDDEL, 'DD/MM/YYYY') end as DELIVERY_DATE,
    lpad(PE.PEJOCA, 2, '0') || '/' || lpad(PE.PEMOCA, 2, '0') || '/' || lpad(PE.PESSCA, 2, '0') || lpad(PE.PEANCA, 2, '0')  as LOAD_DATE,
    PE.PECCHA as LOAD_CODE,
    case when U9.U9NARV > 0 then lpad(U9.U9NARV, 2, '0') || '/' || lpad(U9.U9NRDV, 9, '0') else ' ' end as RDV,
    CG.CGNPLC as PLATE_NUMBER,
    ' ' as TRUCK_DEPARTURE, -- field currently unavailable

    -- TK35/TK05 & invoice dates
    case when HU.HUCDPO is not null then to_char(TK35HEURE, 'DD/MM/YYYY HH24:MI') else ' ' end as TK35_TRANSMISSION,
    case when CHILD.SHCACT is null then ' '
         else case when to_char(CHILD.SHTTKP) = '0' then ' ' else to_char(CHILD.SHDTKP, 'DD/MM/YYYY HH24:MI') end end as TK05_PACKED_DATE,
    case when CHILD.SHCACT is null then ' '
         else case when to_char(CHILD.SHTTKS) = '0' then ' ' else to_char(CHILD.SHDTKS, 'DD/MM/YYYY HH24:MI') end end as TK05_SHIPPED_DATE,
    case when CHILD.SHCACT is null then MOTHER.SHCINV else CHILD.SHCINV end as INVOICE_CODE,

    -- YMS timings
    YMX.MO_ARRIVAL_DATE   as TRUCK_GATE_ARRIVAL,
    YMX.MO_BAY_DATE       as TRUCK_BAY_ARRIVAL,
    YMX.MO_DEPARTURE_DATE as EVENT_490,

    -- Invoice date (string)
    case when CHILD.SHCACT is null then
           case when to_char(MOTHER.SHDINV, 'DD/MM/YYYY HH24:MI') = '01/01/0001 00:00' then ' ' else to_char(MOTHER.SHDINV, 'DD/MM/YYYY HH24:MI') end
         else
           case when to_char(CHILD.SHDINV, 'DD/MM/YYYY HH24:MI') = '01/01/0001 00:00' then ' ' else to_char(CHILD.SHDINV, 'DD/MM/YYYY HH24:MI') end
    end as INVOICE_DATE,

    -- Quantities
    case when PE.PETSOP = '1' and PE.PETSOL = '1' then 0 else PREP_LINES.QTY_ORDERED end as QUANTITY_ORDERED,
    case when PE.PETSOP = '1' then PE.PETBPV else PE.PETBPV end as QUANTITY_PICKED,
    case when PE.PETSOP = '1' then PE.PETBPV else coalesce(PREP_LINES_QTY.QTY_PACK,0) end as QUANTITY_PACKED,
    case when PE.PET1PP = 0 or (PE.PETSOL = '1' and PE.PET1PP > 0) then 0
         when DMS_SUB.QTY_DMS is null then 0 else DMS_SUB.QTY_DMS end as QUANTITY_DMS,
    case when PE.PETSOP = '1' then coalesce(GS.QTY_COL_EXP,0) else coalesce(PREP_LINES_QTY.QTY_COL_LOAD,0) end as PARCELS_LOADED,
    case when PE.PETSOP = '1' then coalesce(GS.QTY_PIE_EXP,0) else coalesce(PREP_LINES_QTY.QTY_PIE_LOAD,0) end as PIECES_LOADED,
    case when PE.PETSOP = '1' then coalesce(GS.QTY_COL_EXP,0) else coalesce(PREP_LINES_QTY.QTY_COL,0) end as CARTONS,

    -- Cancellation flags
    case when PE.PET1PP = 0 or (PE.PETSOL = '1' and PE.PET1PP > 0) then 'CANCEL' else ' ' end as TK05_CANCEL_FLAG,
    case
      when CHILD.SHCACT is null then
        case when to_char(MOTHER.SHTTKP) = '0' and (PE.PET1PP = 0 or (PE.PETSOL = '1' and PE.PET1PP > 0)) then ' '
             else to_char(MOTHER.SHDTKP, 'DD/MM/YYYY HH24:MI') end
      else
        case when to_char(CHILD.SHTTKP) = '0' and (PE.PET1PP = 0 or (PE.PETSOL = '1' and PE.PET1PP > 0)) then ' '
             else case when to_char(CHILD.SHTTKP) = '0' then ' '
                       else to_char(CHILD.SHDTKP, 'DD/MM/YYYY HH24:MI') end end
    end as TK05_CANCEL_DATE,

    -- Planning windows
    case when MOTHER.SHDIPA is null then (case when to_char(CHILD.SHDIPA, 'DD/MM/YYYY HH24:MI') = '00/00/0000 00:00' then ' ' else to_char(CHILD.SHDIPA, 'DD/MM/YYYY') end)
         else (case when to_char(MOTHER.SHDIPA, 'DD/MM/YYYY') = '00/00/0000 00:00' then ' ' else to_char(MOTHER.SHDIPA, 'DD/MM/YYYY') end) end as INITIAL_PLANNED_PACKING_DATE,
    case when MOTHER.SHDIPA is null then (case when to_char(CHILD.SHDIPA, 'DD/MM/YYYY HH24:MI') = '00/00/0000 00:00' then ' ' else to_char(CHILD.SHDIPA, 'HH24:MI') end)
         else (case when to_char(MOTHER.SHDIPA, 'HH24:MI') = '00/00/0000 00:00' then ' ' else to_char(MOTHER.SHDIPA, 'HH24:MI') end) end as INITIAL_PLANNED_PACKING_TIME,
    case when MOTHER.SHDPPA is null then (case when to_char(CHILD.SHDPPA, 'DD/MM/YYYY HH24:MI') = '00/00/0000 00:00' then ' ' else to_char(CHILD.SHDPPA, 'DD/MM/YYYY') end)
         else (case when to_char(MOTHER.SHDPPA, 'DD/MM/YYYY') = '00/00/0000 00:00' then ' ' else to_char(MOTHER.SHDPPA, 'DD/MM/YYYY') end) end as PLANNED_PACKING_DATE,
    case when MOTHER.SHDPPA is null then (case when to_char(CHILD.SHDPPA, 'DD/MM/YYYY HH24:MI') = '00/00/0000 00:00' then ' ' else to_char(CHILD.SHDPPA, 'HH24:MI') end)
         else (case when to_char(MOTHER.SHDPPA, 'HH24:MI') = '00/00/0000 00:00' then ' ' else to_char(MOTHER.SHDPPA, 'HH24:MI') end) end as PLANNED_PACKING_TIME,
    case when MOTHER.SHDILP is null then (case when to_char(CHILD.SHDILP, 'DD/MM/YYYY HH24:MI') = '00/00/0000 00:00' then ' ' else to_char(CHILD.SHDILP, 'DD/MM/YYYY') end)
         else (case when to_char(MOTHER.SHDILP, 'DD/MM/YYYY') = '00/00/0000 00:00' then ' ' else to_char(MOTHER.SHDILP, 'DD/MM/YYYY') end) end as INITIAL_LATEST_PLANNED_PACKING_DATE,
    case when MOTHER.SHDILP is null then (case when to_char(CHILD.SHDILP, 'DD/MM/YYYY HH24:MI') = '00/00/0000 00:00' then ' ' else to_char(CHILD.SHDILP, 'HH24:MI') end)
         else (case when to_char(MOTHER.SHDILP, 'HH24:MI') = '00/00/0000 00:00' then ' ' else to_char(MOTHER.SHDILP, 'HH24:MI') end) end as INITIAL_LATEST_PLANNED_PACKING_TIME,
    case when MOTHER.SHDLPA is null then (case when to_char(CHILD.SHDLPA, 'DD/MM/YYYY HH24:MI') in ('00/00/0000 00:00','01/01/0001 00:00') then ' ' else to_char(CHILD.SHDLPA, 'DD/MM/YYYY') end)
         else (case when to_char(MOTHER.SHDLPA, 'DD/MM/YYYY') in ('00/00/0000 00:00','01/01/0001 00:00') then ' ' else to_char(MOTHER.SHDLPA, 'DD/MM/YYYY') end) end as LATEST_PLANNED_PACKING_DATE,
    case when MOTHER.SHDLPA is null then (case when to_char(CHILD.SHDLPA, 'DD/MM/YYYY HH24:MI') in ('00/00/0000 00:00','01/01/0001 00:00') then ' ' else to_char(CHILD.SHDLPA, 'HH24:MI') end)
         else (case when to_char(MOTHER.SHDLPA, 'HH24:MI') in ('00/00/0000 00:00','01/01/0001 00:00') then ' ' else to_char(MOTHER.SHDLPA, 'HH24:MI') end) end as LATEST_PLANNED_PACKING_TIME,
    case when MOTHER.SHDIPU is null then (case when to_char(CHILD.SHDIPU, 'DD/MM/YYYY HH24:MI') in ('00/00/0000 00:00','01/01/0001 00:00') then ' ' else to_char(CHILD.SHDIPU, 'DD/MM/YYYY') end)
         else (case when to_char(MOTHER.SHDIPU, 'DD/MM/YYYY') in ('00/00/0000 00:00','01/01/0000 00:00') then ' ' else to_char(MOTHER.SHDIPU, 'DD/MM/YYYY') end) end as INITIAL_PICKUP_DATE,
    case when MOTHER.SHDIPU is null then (case when to_char(CHILD.SHDIPU, 'DD/MM/YYYY HH24:MI') in ('00/00/0000 00:00','01/01/0001 00:00') then ' ' else to_char(CHILD.SHDIPU, 'HH24:MI') end)
         else (case when to_char(MOTHER.SHDIPU, 'HH24:MI') in ('00/00/0000 00:00','01/01/0001 00:00') then ' ' else to_char(MOTHER.SHDIPU, 'HH24:MI') end) end as INITIAL_PICKUP_TIME,
    case when MOTHER.SHDPUP is null then (case when to_char(CHILD.SHDPUP, 'DD/MM/YYYY HH24:MI') = '00/00/0000 00:00' then ' ' else to_char(CHILD.SHDPUP, 'DD/MM/YYYY') end)
         else (case when to_char(MOTHER.SHDPUP, 'DD/MM/YYYY') = '00/00/0000 00:00' then ' ' else to_char(MOTHER.SHDPUP, 'DD/MM/YYYY') end) end as PICKUP_DATE,
    '00:00' as PICKUP_TIME,

    -- Flow
    PE.PECMOP as FLOW,
    case
        when PE.PECMOP = 'C1'  then 'CA'
        when PE.PECMOP = 'EDO' then 'Export Doc (GB Malpensa)'
        when PE.PECMOP = 'ESD' then 'Export Doc & Stop&Go'
        when PE.PECMOP = 'EXP' then 'Export Doc (Internal)'
        when PE.PECMOP = 'FCA' then 'FCA/EXW'
        when PE.PECMOP = 'FES' then 'EXW - Export Doc & Stop'
        when PE.PECMOP = 'FEX' then 'EXW - Export Doc'
        when PE.PECMOP = 'FOG' then 'FCA + OK to Ship'
        when PE.PECMOP = 'FOS' then 'FCA + OK to Ship & Stop&G'
        when PE.PECMOP = 'FSG' then 'FCA + Stop&Go'
        when PE.PECMOP = 'HAL' then 'Hallmarking'
        when PE.PECMOP = 'MTM' then 'Make to Order, Make to Measure'
        when PE.PECMOP = 'TK9' then ''
        when PE.PECMOP = 'VAN' then 'Vanilla'
        when PE.PECMOP = 'VIN' then 'Invoice Required'
        when PE.PECMOP = 'VOK' then 'OK to Ship'
        when PE.PECMOP = 'VOS' then 'Stop & GO & OK to Ship'
        when PE.PECMOP = 'VSI' then 'Invoice Required & Stop'
        when PE.PECMOP = 'VST' then 'Vanilla Stop & GO'
        when PE.PECMOP = 'WAN' then 'do not delete'
        when PE.PECMOP = 'WTP' then 'do not delete'
        else null end as FLOW_DESCRIPTION,

    -- Compliance flags
    case when MOTHER.SHTHZM is null then CHILD.SHTHZM else MOTHER.SHTHZM end as FLAG_HAZMAT,
    case when MOTHER.SHTFSC is null then CHILD.SHTFSC else MOTHER.SHTFSC end as FLAG_FSC,
    case when MOTHER.SHTJEW is null then CHILD.SHTJEW else MOTHER.SHTJEW end as FLAG_JEWELLERY,
    FLAG_PACKAGING_JEWELRY_SUB as FLAG_PACKAGING_JEWELLERY,
    case when MOTHER.SHTHMK is null then CHILD.SHTHMK else MOTHER.SHTHMK end as FLAG_HALMARKING,
    case when MOTHER.SHSHMK is null then CHILD.SHSHMK else MOTHER.SHSHMK end as HALMARKING_STATUS,
    case when MOTHER.SHTCIT is null then CHILD.SHTCIT else MOTHER.SHTCIT end as FLAG_IMPACT_CITES,
    case when MOTHER.SHSCIT is null then CHILD.SHSCIT else MOTHER.SHSCIT end as CITES_STATUS,

    -- VAS
    case when VAS.VAS_CODE is null then '0' else '1' end as VAS_FLAG,
    VAS.VAS_CODE   as VAS_CODE,
    VAS.VAS_CLUSTER as VAS_CLUSTER,

    -- Transport + channel
    MOTHER.SHMOTR as SAP_MEAN_OF_TRANSPORT,
    CG.CGCTMT as REFLEX_MEAN_OF_TRANSPORT,
    case when MOTHER.SHCLUS is null then CHILD.SHCLUS else MOTHER.SHCLUS end as CLUSTER,
    case when MOTHER.SHCHAN is null then CHILD.SHCHAN else MOTHER.SHCHAN end as CHANNEL,
    case when MOTHER.SHTCEE is null then (case when CHILD.SHTCEE = '1' then 'CEE' else 'EXTRA' end)
         else (case when MOTHER.SHTCEE = '1' then 'CEE' else 'EXTRA' end) end as FLAG_CEE,
    case when MOTHER.SHTTSP is null then CHILD.SHTTSP else MOTHER.SHTTSP end as FLAG_IS_TO_SHIP,
    case when MOTHER.SHTSTP is null then CHILD.SHTSTP else MOTHER.SHTSTP end as FLAG_IS_STOP,
    case when PE.PET1PP = 0 or (PE.PETSOL = '1' and PE.PET1PP > 0) then '1' else '0' end as FLAG_IS_CANCELLED,
    case when MOTHER.SHTMAA is null then CHILD.SHTMAA else MOTHER.SHTMAA end as FLAG_MAX_ATTENTION,
    case when MOTHER.SHNPRI is null then CHILD.SHNPRI else MOTHER.SHNPRI end as PRIORITY,
    case when MOTHER.SHDOCT is null then CHILD.SHDOCT else MOTHER.SHDOCT end as DOCUMENT_TYPE,
    case when MOTHER.SHFLOT is null then CHILD.SHFLOT else MOTHER.SHFLOT end as FLOW_TYPE,
    case when MOTHER.SHLROU is null then CHILD.SHLROU else MOTHER.SHLROU end as ROUTE,
    case when MOTHER.SHLSHC is null then CHILD.SHLSHC else MOTHER.SHLSHC end as SHIPPING_CONDITION,
    case when MOTHER.SHCDEG is null then CHILD.SHCDEG else MOTHER.SHCDEG end as CODE_DELIVERY_GROUP,
    case when CHILD.SHLDEB is null then MOTHER.SHLDEB else CHILD.SHLDEB end as DELIVERY_BLOCK,
    case when CHILD.SHCACT is null then MOTHER.SHLSHB else CHILD.SHLSHB end as SHIPMENT_BLOCKED,
    case when MOTHER.SHTREO is null then CHILD.SHTREO else MOTHER.SHTREO end as FLAG_IS_RELEASE_OD,
    case when CHILD.SHCACT is null then MOTHER.SHREXT else CHILD.SHREXT end as EXTERNAL_ID,
    case when CHILD.SHCACT is null then MOTHER.SHREX5 else CHILD.SHREX5 end as ORDER_ID_SENT_IN_TK05,
    case when CHILD.SHTURG is null then MOTHER.SHTURG else CHILD.SHTURG end as FLAG_URGENT,
    case when CHILD.SHTCUT is null then MOTHER.SHTCUT else CHILD.SHTCUT end as FLAG_CUT_OFF,
    case when CHILD.SHTDMM is null then MOTHER.SHTDMM else CHILD.SHTDMM end as FLAG_DMM_RECALCULATION,
    case when MOTHER.SHDCPU is null then (case when to_char(CHILD.SHDCPU, 'DD/MM/YYYY HH24:MI') = '00/00/0000 00:00' then ' ' else to_char(CHILD.SHDCPU, 'DD/MM/YYYY HH24:MI') end)
         else (case when to_char(MOTHER.SHDCPU, 'DD/MM/YYYY HH24:MI') = '00/00/0000 00:00' then ' ' else to_char(MOTHER.SHDCPU, 'DD/MM/YYYY HH24:MI') end) end as DMM_PICKUP,
    case when MOTHER.SHDCPA is null then (case when to_char(CHILD.SHDCPA, 'DD/MM/YYYY HH24:MI') = '00/00/0000 00:00' then ' ' else to_char(CHILD.SHDCPA, 'DD/MM/YYYY HH24:MI') end)
         else (case when to_char(MOTHER.SHDCPA, 'DD/MM/YYYY HH24:MI') = '00/00/0000 00:00' then ' ' else to_char(MOTHER.SHDCPA, 'DD/MM/YYYY HH24:MI') end) end as DMM_PACKING,

    -- Misc
    --substr(PE."Commenti",1,70) as "Commenti",
    PE.PETOPD as FLAG_ORDER_DESACTIVATED,

    -- Computed per-row change timestamp (used for optional filtering & exposed)
    greatest(
        coalesce(OE.HVR_CHANGE_TIME, to_timestamp('1900-01-01 00:00:01')),
        coalesce(PREP_LINES.LAST_UPDATE, to_timestamp('1900-01-01 00:00:01')),
        coalesce(MOTHER.HVR_CHANGE_TIME, to_timestamp('1900-01-01 00:00:01')),
        coalesce(CHILD.HVR_CHANGE_TIME,  to_timestamp('1900-01-01 00:00:01')),
        coalesce(CG.HVR_CHANGE_TIME,     to_timestamp('1900-01-01 00:00:01')),
        coalesce(U9.HVR_CHANGE_TIME,     to_timestamp('1900-01-01 00:00:01')),
        coalesce(U6.HVR_CHANGE_TIME,     to_timestamp('1900-01-01 00:00:01')),
        coalesce(HU.LAST_UPDATE,         to_timestamp('1900-01-01 00:00:01')),
        coalesce(GS.LAST_UPDATE,         to_timestamp('1900-01-01 00:00:01'))
    ) as CHANGE_TS,

    -- Soft-delete + product
    '0' as ISDELETED,
    PREP_LINES.PRODCAT as PRODUCT_CATEGORY

from MODELS.KERING_GLOBE.HLODPEP OE
join PREP_LINES
  on PREP_LINES.PECDOD = OE.OECDPO and PREP_LINES.P1CACT = OE.OECACT and PREP_LINES.P1NANO = OE.OENANN and PREP_LINES.P1NODP = OE.OENODP
join PE
  on PREP_LINES.P1CDPO = PE.PECDPO and PREP_LINES.P1CACT = PE.PECACT and PE.PENANN = PREP_LINES.P1NANP and PE.PENPRE = PREP_LINES.P1NPRE
left join MODELS.KERING_GLOBE.KBSHIPP MOTHER
  on MOTHER.SHCDEP = OECDPO and MOTHER.SHCACT = OECACT and MOTHER.SHCDOR = OECDDO and MOTHER.SHRDOR = OERODP and SHNANN = 0
left join MODELS.KERING_GLOBE.KBSHIPP CHILD
  on CHILD.SHCDEP = PECDPO and CHILD.SHCACT = PECACT and CHILD.SHNANN = PENANN and CHILD.SHNPRP = PENPRE
left join MODELS.KERING_GLOBE.HLCHARP CG
  on CG.CGCDPO = PE.PECDPO and PE.PESSCA = CG.CGSSCA and PE.PEANCA = CG.CGANCA and PE.PEMOCA = CG.CGMOCA and PE.PEJOCA = CG.CGJOCA and PE.PECCHA = CG.CGCCHA
left join MODELS.KERING_GLOBE.HLRDVCP U9
  on U9.U9CDPO = CG.CGCDPO and U9.U9SSCA = CG.CGSSCA and U9.U9ANCA = CG.CGANCA and U9.U9MOCA = CG.CGMOCA and U9.U9JOCA = CG.CGJOCA and U9.U9CCHA = CG.CGCCHA
left join MODELS.KERING_GLOBE.HLRDVTP U6
  on U9.U9CDPO = U6.U6CDPO and U9.U9NARV = U6.U6NARV and U9.U9NRDV = U6.U6NRDV
left join MODELS.KERING_GLOBE.HLTRSPP TP
  on CG.CGCTRP = TP.TPCTRP
left join HU
  on PE.PECDPO = HU.HUCDPO and PE.PERODP = HU.HUDNUM and HU.HUPORD = lpad(PE.PENANN, 2, '0') || lpad(PE.PENPRE, 9, '0')
left join PREP_LINES_QTY
  on PREP_LINES_QTY.P1CDPO = PE.PECDPO and PREP_LINES_QTY.P1CACT = PE.PECACT and PREP_LINES_QTY.P1NANP = PE.PENANN and PREP_LINES_QTY.P1NPRE = PE.PENPRE
left join DMS_SUB
  on DMS_SUB.P1CDPO = PE.PECDPO and DMS_SUB.P1CACT = PE.PECACT and DMS_SUB.P1NANP = PE.PENANN and DMS_SUB.P1NPRE = PE.PENPRE
left join GS
  on GS.GSCDPO = PE.PECDPO and GS.GSCACT = PE.PECACT and GS.GSNAPP = PE.PENANN and GS.GSNPRE = PE.PENPRE
left join FACT_KER_02_OUTBOUND_VAS VAS
  on PE.PECDPO = VAS.P1CDPO and PE.PECACT = VAS.P1CACT and PE.PENANN = VAS.P1NANP and PE.PENPRE = VAS.P1NPRE
left join CO on CO.CORCDE = PE.PERODP
left join SG1 on SG1.CGCOD = PE.PECCHA and SG1.CGSIE = PE.PESSCA and SG1.CGANN = PE.PEANCA and SG1.CGMOI = PE.PEMOCA and SG1.CGJOU = PE.PEJOCA
left join SG2 on SG2.CGCOD = PE.PECCHA and SG2.CGSIE = PE.PESSCA and SG2.CGANN = PE.PEANCA and SG2.CGMOI = PE.PEMOCA and SG2.CGJOU = PE.PEJOCA
left join YMX on YMX.TDTREFB = U6.U6LRDV
where OE.OECDPO = '001' and OE.OECACT = '100'
)

/*-----------------------------------------------------------------------
ENDPOINT SELECT
Role: Final projection. CHANGE_TS is exposed as UPDATED_DATE.
Optional filter (commented) allows selecting data loaded on/after a
specific timestamp. Just uncomment the WHERE clause and set a datetime.
-----------------------------------------------------------------------*/
select
  PK_FACT_OUTBOUND,
  INTEGRATION_DATE,
  INTEGRATION_TIME,
  ENVIRONMENT,
  BUILDING,
  REFLEX_CLIENT,
  REFLEX_DESTINATION,
  WAVING_DATE,
  WAVING_CODE,
  ORDER_INSERT_UPDATE,
  SHIPPING_REQUEST,
  BRAND,
  SAP_ORDERID,
  CUSTOMER_CODE,
  COUNTRY,
  SAP_CARRIER,
  REFLEX_CARRIER,
  CARRIER_NAME,
  OWNER,
  QUALITY,
  PREPARATION_NUMBER,
  DELIVERY_DATE,
  LOAD_DATE,
  LOAD_CODE,
  RDV,
  PLATE_NUMBER,
  TRUCK_DEPARTURE,
  TK35_TRANSMISSION,
  TK05_PACKED_DATE,
  TK05_SHIPPED_DATE,
  INVOICE_CODE,
  TRUCK_GATE_ARRIVAL,
  TRUCK_BAY_ARRIVAL,
  EVENT_490,
  INVOICE_DATE,
  QUANTITY_ORDERED,
  QUANTITY_PICKED,
  QUANTITY_PACKED,
  QUANTITY_DMS,
  PARCELS_LOADED,
  PIECES_LOADED,
  CARTONS,
  TK05_CANCEL_FLAG,
  TK05_CANCEL_DATE,
  INITIAL_PLANNED_PACKING_DATE,
  INITIAL_PLANNED_PACKING_TIME,
  PLANNED_PACKING_DATE,
  PLANNED_PACKING_TIME,
  INITIAL_LATEST_PLANNED_PACKING_DATE,
  INITIAL_LATEST_PLANNED_PACKING_TIME,
  LATEST_PLANNED_PACKING_DATE,
  LATEST_PLANNED_PACKING_TIME,
  INITIAL_PICKUP_DATE,
  INITIAL_PICKUP_TIME,
  PICKUP_DATE,
  PICKUP_TIME,
  FLOW,
  FLOW_DESCRIPTION,
  FLAG_HAZMAT,
  FLAG_FSC,
  FLAG_JEWELLERY,
  FLAG_PACKAGING_JEWELLERY,
  FLAG_HALMARKING,
  HALMARKING_STATUS,
  FLAG_IMPACT_CITES,
  CITES_STATUS,
  VAS_FLAG,
  VAS_CODE,
  VAS_CLUSTER,
  SAP_MEAN_OF_TRANSPORT,
  REFLEX_MEAN_OF_TRANSPORT,
  CLUSTER,
  CHANNEL,
  FLAG_CEE,
  FLAG_IS_TO_SHIP,
  FLAG_IS_STOP,
  FLAG_IS_CANCELLED,
  FLAG_MAX_ATTENTION,
  PRIORITY,
  DOCUMENT_TYPE,
  FLOW_TYPE,
  ROUTE,
  SHIPPING_CONDITION,
  CODE_DELIVERY_GROUP,
  DELIVERY_BLOCK,
  SHIPMENT_BLOCKED,
  FLAG_IS_RELEASE_OD,
  EXTERNAL_ID,
  ORDER_ID_SENT_IN_TK05,
  FLAG_URGENT,
  FLAG_CUT_OFF,
  FLAG_DMM_RECALCULATION,
  DMM_PICKUP,
  DMM_PACKING,
  --"Commenti",
  FLAG_ORDER_DESACTIVATED,
  /* expose the change timestamp */
  CHANGE_TS as UPDATED_DATE,
  ISDELETED,
  PRODUCT_CATEGORY
from BASE

-- Use this to select data loaded/changed on or after a specific moment.
-- Example: on/after 2025-08-01 00:00:00
WHERE CHANGE_TS >= to_timestamp('2025-08-25 16:00:00')
-- Or use a session variable:
--   set LOAD_FROM_TS = to_timestamp('2025-08-01 00:00:00');
--   select * from MODELS.KERING_GLOBE.CRT_KER_02_OUTBOUND where CHANGE_TS >= $LOAD_FROM_TS;

;
