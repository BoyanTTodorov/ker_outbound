with
receiving AS (
select DateRcv,
sum(case when CAPO = 'CAPO_A' and Grade <>'T49' AND Grade <>'ZAL' then Units else 0 end) as UNLOADING_GOH,
sum(case when CAPO <>'CAPO_A' and Grade <>'T49' AND Grade <>'ZAL' then Cartons else 0 end) as FLAT_CARTONS,
sum(case when Grade IN ('T49','ZAL') then Units else 0 end) as SAMPLES_UNITS
from (
select 
DateRcv,
VGCQAL as Grade,
CAPOS as CAPO,
count(distinct HD) as Cartons,
sum(item_qty) as Units
from
(SELECT
( digits(dasie) || digits(daann) || '-' || digits(damois) || '-' || digits(dajour)) as DateRcv,
VGCQAL,
B.A2CFAR as CAPOS,
VGNSUP as HD,
Qty as item_qty
FROM 
infsql.nddatep
INNER JOIN HLMVTGP ON VGSCRE = dasie and VGACRE = daann and VGMCRE = damois and VGJCRE = dajour
inner join GUEPRDDB.hlcdfap B ON (VGCART = B.A2CART) AND VGCACT = B.A2CACT AND B.A2CFAN = 'CAPO'
inner join (select VGNGEI as GEI,
VGJCRE as DAY1, VGMCRE as MONTH1, VGSCRE as SICLE1 , VGACRE as YEAR1,
sum(case when VGSMVG = '+' then VGQMVG else (VGQMVG * -1) end) as Qty from HLMVTGP where ((VGCTVG = '220' and VGCTMS = '250') or VGCTVG = '100')  group by VGNGEI, VGJCRE, VGMCRE, VGSCRE, VGACRE ) xx on VGNGEI = xx.GEI and xx.DAY1 = VGJCRE and xx.MONTH1 =  VGMCRE and xx.SICLE1 = VGSCRE and xx.YEAR1 = VGACRE
WHERE VGCTVG = '100' and
( digits(dasie) || digits(daann) || '-' || digits(damois) || '-' || digits(dajour)) = (current_date - 1 day)
group by ( digits(dasie) || digits(daann) || '-' || digits(damois) || '-' || digits(dajour)),VGCQAL ,B.A2CFAR,VGCUCR,VGNSUP,VGCART,VGQMVG,VGNGEI,Qty) yy
group by DateRcv,VGCQAL,CAPOS
order by capos) group by DateRcv
)
,
moves AS (
SELECT "Date", 
sum(CASE WHEN MOVE = 'RECVGOH' THEN Units ELSE 0 END) AS MOVE_GOH, 
sum(CASE WHEN MOVE in( 'LANE001','RECVA','LANE003','QC  IN') THEN Cartons ELSE 0 END) AS MOVE_A,
sum(CASE WHEN MOVE in( 'RECVMUL','RECVRET','RECVHV') THEN Cartons ELSE 0 END) AS MOVE_C
FROM (
SELECT 
( digits(dasie) || digits(daann) || '-' || digits(damois) || '-' || digits(dajour)) as "Date",
MVC1EM || MVC3EM MOVE,
sum(GEQGEI) as Units,
count(distinct mvnsup) as Cartons
from infsql.nddatep
inner join GUEPRDDB.HLMVAEP on MVSVAM = dasie and MVAVAM = daann and MVMVAM = damois and MVJVAM = dajour
LEFT  join hlgeinp on gensup = mvnsup AND gecqal <>'STD'
inner join hlemplp on MVNEPO = EMNEMP 
where MVCDPO = '001' and MVJVAM <> 0 and MVCTMP = '010' and ( digits(dasie) || digits(daann) || '-' || digits(damois) || '-' || digits(dajour)) = (current_date - 1 day) AND  MVC1EM || MVC3EM <> 'CONSIN'
group by ( digits(dasie) || digits(daann) || '-' || digits(damois) || '-' || digits(dajour)), MVC1EM || MVC3EM) GROUP BY "Date"
)
,
pick_pack AS (
SELECT
TheDate as "Date",
sum(CASE WHEN B.A2CFAR = 'CAPO_A' THEN PickM_1M ELSE 0 END) as PICKING_MANUAL_PIECES_GOH,
sum(CASE WHEN B.A2CFAR <> 'CAPO_A' THEN PickM_1M ELSE 0 END) as PICKING_MANUAL_PIECES_flat,
sum(PickS_1) as PICKING_SORTER_PIECES,
sum(PickMP_1)+ sum(PickPS_1) + sum(Packing_CSB_1) as OPT_PICKING,
sum(CASE WHEN B.A2CFAR <> 'CAPO_A' THEN Pack ELSE 0 END) + sum(CASE WHEN B.A2CFAR = 'CAPO_V' THEN Packing_CS_1 ELSE 0 END) as PACK_MANAUL,
sum(CASE WHEN B.A2CFAR = 'CAPO_A' THEN Pack ELSE 0 END) AS PACK_GOH,
sum(PACK_SOR) + sum(CASE WHEN B.A2CFAR = 'CAPO_S' THEN Packing_CS_1 ELSE 0 END) as PACK_SORTER,
sum(CASE WHEN B.A2CFAR = 'CAPO_S' THEN Packing_CS_1 ELSE 0 END) AS PACK_400_S,
sum(CASE WHEN B.A2CFAR = 'CAPO_V' THEN Packing_CS_1 ELSE 0 END) AS PACK_400_V
FROM
(
SELECT
(current_date - 1 day) as TheDate,
PVCART as Item,
sum(case when  (PVCT03 = '004') and (pvcatl in ( '100', '105')) then PVQPRB else 0 end ) as PickS_1,
sum(case when  (PVCT03 = '004') and (pvcatl in ( '300', '305')) then PVQPRB else 0 end ) as PickM_1M,
count( distinct case when  (PVCT03 = '002') and (pvcatl in ( '300', '000')) then PVNSUP else NULL end ) as PickMP_1,
count( distinct case when  (PVCT03 = '002' and pvcatl = '400') then PVNSUP else NULL end ) as Packing_CSB_1,
count( distinct case when  (PVCT03 = '002') and (pvcatl in ( '100', '105')) then PVNSUP else NULL end ) as PickPS_1,
sum(case when  ((PVCT03 = '003') and (pvcatl =  '400')) then PVQPRB else 0 end ) as Packing_CS_1,
0 AS PACK,
sum(case when PVCT03 = '003' and pvcatl = '100'  then PVQPRB else 0 end ) as PACK_SOR
FROM infsql.nddatep
inner join GUEPRDDB.hlprelp on PVSVPR = dasie and PVAVPR = daann and PVMVPR = damois and PVJVPR = dajour
WHERE PVTVPR = '1' and PVTNPR = '0' and PVCACT= 'GUE' and PVCT03 in ('002', '004', '003') and PVCDPO = '001' 
and ( digits(dasie) || digits(daann) || '-' || digits(damois) || '-' || digits(dajour)) = (current_date - 1 day)                            
GROUP BY PVCART
UNION
Select
(current_date - 1 day) as TheDate,
VGCART as Item,
0 as PickS_1,
0 as PickM_1M,
0 as PickMP_1,
0 as Packing_CSB_1,
0 as PickPS_1,
0 as Packing_CS_1, 
sum( case when vgsmvg = '+' then VGQMVG else VGQMVG * (-1) end) as PACK,
0 as PACK_SOR
FROM infsql.nddatep
inner join GUEPRDDB.hlmvtgp ON VGSCRE = dasie and VGACRE = daann and VGMCRE = damois and VGJCRE = dajour
exception join GUEPRD_DAT.GNANCEP on AECACT = vgcact and AECDPO = vgcdpo and AENCOL = vgncol
WHERE VGCTST= '200' and VGCDPO = '001' and VGCACT = 'GUE' and vgncol = vgnsup and VGCPCR = 'HLLG65'    
 and VGCTVG in ('330', '340') and  
( digits(dasie) || digits(daann) || '-' || digits(damois) || '-' || digits(dajour)) = (current_date - 1 day)                                
GROUP BY VGCART
HAVING sum( case when vgsmvg = '+' then VGQMVG else VGQMVG * (-1) end) <> 0
) xx
left outer join GUEPRDDB.hlcdfap B ON (ITEM = B.A2CART) AND 'GUE' = B.A2CACT AND B.A2CFAN = 'CAPO'
Group by TheDate
)
,
Shipped as ( 
SELECT
( digits(dasie) || digits(daann) || '-' || digits(damois) || '-' || digits(dajour)) "DATE"
,count(distinct GSNCOL) Shipped_boxes FROM infsql.nddatep
INNER JOIN GUEPRDDB.hlgesop ON GSSCRE = dasie and GSACRE = daann and GSMCRE = damois and GSJCRE = dajour
inner join GUEPRDDB.hlcosop on O9NCOL=GSNCOL
inner join GUEPRD_DAT.GNORDOP on gordor = GSRODP
WHERE ( digits(dasie) || digits(daann) || '-' || digits(damois) || '-' || digits(dajour)) = (current_date - 1 day)
GROUP BY ( digits(dasie) || digits(daann) || '-' || digits(damois) || '-' || digits(dajour))
)
,
MZN AS (
SELECT ( digits(dasie) || digits(daann) || '-' || digits(damois) || '-' || digits(dajour)) "DATE", count(DISTINCT MVNSUP) MZN_BOXES FROM infsql.nddatep
inner join GUEPRDDB.HLMVAEP on MVSVAM = dasie and MVAVAM = daann and MVMVAM = damois and MVJVAM = dajour
INNER JOIN HLEMPLP C ON C.EMNEMP = MVNEMF AND SUBSTR(C.EMC1EM, 2, 1) = 'C'
WHERE ( digits(dasie) || digits(daann) || '-' || digits(damois) || '-' || digits(dajour)) = (current_date - 1 day) AND MVC1EM = 'MZN' AND MVTPEC = '1' 
GROUP BY ( digits(dasie) || digits(daann) || '-' || digits(damois) || '-' || digits(dajour))
)
--
SELECT 
DADATE "DATE"
,IFNULL(receiving.UNLOADING_GOH, 0) "Unloading GoH"
,IFNULL(moves.MOVE_GOH, 0) "Put away GoH"
,IFNULL(receiving.FLAT_CARTONS, 0) "Unloading & palletizing Flat"
,IFNULL(moves.MOVE_A, 0) + IFNULL(pick_pack.OPT_PICKING, 0) "OPT Put away & Picking"
,IFNULL(receiving.SAMPLES_UNITS, 0)  "Samples inbound"
,'' "Samples outbound"
,IFNULL(pick_pack.PICKING_MANUAL_PIECES_GOH, 0)  "Picking GoH"
,IFNULL(pick_pack.PICKING_MANUAL_PIECES_flat, 0)  "Piece pick manual"
,IFNULL(pick_pack.PICKING_SORTER_PIECES, 0)  "Piece pick sorter"
,IFNULL(pick_pack.PACK_SORTER, 0)  "Sorter"
,IFNULL(MZN.MZN_BOXES, 0)  + IFNULL(moves.MOVE_C, 0)  "MZN"
,IFNULL(pick_pack.PACK_MANAUL, 0)  "Packing manual"
,IFNULL(pick_pack.PACK_GOH, 0)  "Packing GoH"
,IFNULL(Shipped.Shipped_boxes, 0)  "Shipping"
,IFNULL(pick_pack.PACK_400_S, 0)  "Packing 400 S"
,IFNULL(pick_pack.PACK_400_V, 0)  "Packing 400 V"
FROM infsql.nddatep 
left JOIN receiving ON DADATE=receiving.DateRcv
left JOIN moves ON DADATE = moves."Date"
left JOIN pick_pack ON DADATE = pick_pack."Date"
left JOIN Shipped ON DADATE = Shipped."DATE"
left JOIN MZN ON DADATE = MZN."DATE"
WHERE DADATE = (current_date - 1 day)
