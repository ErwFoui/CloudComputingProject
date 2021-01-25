-- Hive optimizations
SET hive.cli.print.header=true;
SET hive.exec.parallel=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.fetch.column.stats=true;
SET hive.stats.fetch.partition.stats=true;
SET hive.vectorized.execution.enabled=true;
SET hive.auto.convert.join=true;

-- Compute basic statistics on hive table
ANALYZE TABLE flights PARTITION (Yeard, Monthd) COMPUTE STATISTICS;
ANALYZE TABLE flights PARTITION (Yeard, Monthd) COMPUTE STATISTICS FOR COLUMNS;

-- Question 1.1 - Query
SELECT 
  orig.ORIGIN,
  orig.all_departures + destination.all_arrivals as all_flights
FROM
  (SELECT ORIGIN, count(OP_CARRIER_FL_NUM) AS all_departures FROM flights GROUP BY ORIGIN) orig
  JOIN
  (SELECT DEST, count(OP_CARRIER_FL_NUM) AS all_arrivals FROM flights GROUP BY DEST) destination
  ON orig.ORIGIN = destination.DEST
ORDER BY all_flights DESC
LIMIT 10;
-- Question 1.1 - Result
-- orig.origin	all_flights
-- "ORD"	13285286
-- "ATL"	12231578
-- "DFW"	11501498
-- "LAX"	8201586
-- "PHX"	7011933
-- "DEN"	6682815
-- "DTW"	5998100
-- "IAH"	5791682
-- "MSP"	5540681
-- "SFO"	5478875
-- Time taken: 277.546 seconds --> Time taken: 89.121 seconds with hive.vectorized.execution.enabled=true and hive.auto.convert.join=true


-- Question 1.2 - Query
-- "On-time arrival performance" = Ascending order of arrival delay
SELECT
  OP_UNIQUE_CARRIER as airline,
  ROUND(AVG(ARR_DELAY), 3) as avg_arr_delay
FROM flights
WHERE CANCELLED = 0
GROUP BY OP_UNIQUE_CARRIER
ORDER BY avg_arr_delay ASC
LIMIT 10;
  
-- Question 1.2 - Result
-- airline	avg_arr_delay
-- "HA"	-0.775
-- "KH"	1.157
-- "ML (1)"	4.748
-- "PA (1)"	5.434
-- "NW"	5.491
-- "WN"	5.515
-- "F9"	5.693
-- "OO"	5.877
-- "9E"	6.108
-- "TZ"	6.129
-- Time taken: 139.859 seconds --> Time taken: 80.228 seconds with hive.vectorized.execution.enabled=true and hive.auto.convert.join=true

-- Question 1.3 - Query
SELECT
  DAY_OF_WEEK as dow,
  ROUND(AVG(ARR_DELAY), 3) as avg_arr_delay
FROM flights
WHERE CANCELLED = 0
GROUP BY DAY_OF_WEEK
ORDER BY avg_arr_delay ASC;

-- Question 1.3 - Result
-- dow	avg_arr_delay
-- 6	4.191
-- 2	5.966
-- 7	6.529
-- 1	6.684
-- 3	7.111
-- 4	8.951
-- 5	9.611
-- Time taken: 134.044 seconds --> Time taken: 78.216 seconds with hive.vectorized.execution.enabled=true and hive.auto.convert.join=true

-- Question 2.1 - Table
-- First create new table
DROP TABLE flights_2_1;
CREATE EXTERNAL TABLE flights_2_1 (
  orig STRING,
  carrier STRING,
  avg_delay DOUBLE,
  rank BIGINT
);
STORED AS SEQUENCEFILE;

-- Then write to table
INSERT OVERWRITE TABLE flights_2_1
SELECT *
FROM (
  SELECT 
    *, 
    rank() OVER (PARTITION BY orig ORDER BY avg_delay, carrier) rk
  FROM (
    SELECT 
      ORIGIN AS orig,
      OP_UNIQUE_CARRIER as carrier,
      round(avg(DEP_DELAY), 3) AS avg_delay
    FROM flights
    WHERE CANCELLED = 0
    GROUP BY ORIGIN, OP_UNIQUE_CARRIER
  ) unranked
) ranked
WHERE (ranked.rk <= 10);

-- Question 2.1 - Query
SELECT * FROM flights_2_1 WHERE orig LIKE '%CMI%'
OR orig LIKE '%BWI%'
OR orig LIKE '%MIA%'
OR orig LIKE '%LAX%'
OR orig LIKE '%IAH%'
OR orig LIKE '%SFO%';

-- Question 2.1 - Result
-- "BWI"	"F9"	0.756	1
-- "BWI"	"PA (1)"	4.762	2
-- "BWI"	"CO"	5.178	3
-- "BWI"	"YV"	5.355	4
-- "BWI"	"NW"	5.578	5
-- "BWI"	"AA"	5.922	6
-- "BWI"	"9E"	6.747	7
-- "BWI"	"US"	7.494	8
-- "BWI"	"UA"	7.605	9
-- "BWI"	"FL"	7.695	10
-- "CMI"	"OH"	0.612	1
-- "CMI"	"US"	2.033	2
-- "CMI"	"TW"	3.848	3
-- "CMI"	"PI"	4.599	4
-- "CMI"	"DH"	6.028	5
-- "CMI"	"EV"	6.665	6
-- "CMI"	"MQ"	7.849	7
-- "IAH"	"NW"	3.485	1
-- "IAH"	"PI"	3.825	2
-- "IAH"	"PA (1)"	3.985	3
-- "IAH"	"US"	4.912	4
-- "IAH"	"AA"	5.548	5
-- "IAH"	"F9"	5.598	6
-- "IAH"	"TW"	5.979	7
-- "IAH"	"EA"	6.16	8
-- "IAH"	"WN"	6.269	9
-- "IAH"	"MQ"	6.713	10
-- "LAX"	"MQ"	2.356	1
-- "LAX"	"OO"	4.182	2
-- "LAX"	"TZ"	4.764	3
-- "LAX"	"FL"	4.872	4
-- "LAX"	"NW"	5.101	5
-- "LAX"	"YV"	5.89	6
-- "LAX"	"F9"	5.987	7
-- "LAX"	"HA"	6.116	8
-- "LAX"	"US"	6.681	9
-- "LAX"	"AA"	6.964	10
-- "MIA"	"EV"	1.203	1
-- "MIA"	"TZ"	1.782	2
-- "MIA"	"XE"	1.919	3
-- "MIA"	"PA (1)"	4.19	4
-- "MIA"	"NW"	4.443	5
-- "MIA"	"US"	6.044	6
-- "MIA"	"UA"	6.662	7
-- "MIA"	"ML (1)"	7.505	8
-- "MIA"	"PI"	8.064	9
-- "MIA"	"MQ"	8.547	10
-- "SFO"	"TZ"	3.952	1
-- "SFO"	"MQ"	4.975	2
-- "SFO"	"PA (1)"	5.278	3
-- "SFO"	"F9"	5.447	4
-- "SFO"	"NW"	5.592	5
-- "SFO"	"DL"	6.474	6
-- "SFO"	"CO"	7.106	7
-- "SFO"	"US"	7.434	8
-- "SFO"	"TW"	7.743	9
-- "SFO"	"AA"	7.836	10
-- Time taken: 0.073 seconds, Fetched: 57 row(s)

-- Question 2.2 - Table
DROP TABLE flights_2_2;
CREATE EXTERNAL TABLE flights_2_2 (
  orig STRING,
  destination STRING,
  avg_delay DOUBLE,
  rank BIGINT
)
STORED AS SEQUENCEFILE;

INSERT OVERWRITE TABLE flights_2_2
SELECT * 
FROM (
  SELECT
    *,
    rank() OVER (PARTITION BY orig ORDER BY avg_delay, destination) rk
  FROM (
    SELECT 
      ORIGIN AS orig,
      DEST AS destination,
      round(avg(DEP_DELAY), 3) AS avg_delay
    FROM flights
    WHERE CANCELLED = 0
    GROUP BY ORIGIN, DEST
  ) unranked
) ranked
WHERE ranked.rk <= 10;

-- Question 2.2 - Query
SELECT * FROM flights_2_2 WHERE orig LIKE '%CMI%'
OR orig LIKE '%BWI%'
OR orig LIKE '%MIA%'
OR orig LIKE '%LAX%'
OR orig LIKE '%IAH%'
OR orig LIKE '%SFO%';

-- Question 2.2 - Result
-- "BWI"	"SAV"	-7.0	1
-- "BWI"	"MLB"	1.155	2
-- "BWI"	"DAB"	1.47	3
-- "BWI"	"SRQ"	1.558	4
-- "BWI"	"IAD"	1.597	5
-- "BWI"	"UCA"	3.654	6
-- "BWI"	"CHO"	4.012	7
-- "BWI"	"GSP"	4.198	8
-- "BWI"	"MDT"	4.421	9
-- "BWI"	"OAJ"	4.494	10
-- "CMI"	"ABI"	-7.0	1
-- "CMI"	"PIT"	1.102	2
-- "CMI"	"CVG"	1.895	3
-- "CMI"	"DAY"	3.256	4
-- "CMI"	"STL"	3.727	5
-- "CMI"	"PIA"	4.163	6
-- "CMI"	"DFW"	6.334	7
-- "CMI"	"ATL"	6.665	8
-- "CMI"	"ORD"	7.939	9
-- "CMI"	"SPI"	587.0	10
-- "IAH"	"MSN"	-2.0	1
-- "IAH"	"AGS"	-0.619	2
-- "IAH"	"MLI"	-0.5	3
-- "IAH"	"EFD"	1.888	4
-- "IAH"	"HOU"	2.091	5
-- "IAH"	"JAC"	2.388	6
-- "IAH"	"SBA"	3.0	7
-- "IAH"	"RNO"	3.161	8
-- "IAH"	"MTJ"	3.175	9
-- "IAH"	"BPT"	3.6	10
-- "LAX"	"SDF"	-16.0	1
-- "LAX"	"IDA"	-7.0	2
-- "LAX"	"DRO"	-6.0	3
-- "LAX"	"RSW"	-3.0	4
-- "LAX"	"LAX"	-2.0	5
-- "LAX"	"BZN"	-0.231	6
-- "LAX"	"MAF"	0.0	7
-- "LAX"	"PIH"	0.0	8
-- "LAX"	"IYK"	1.169	9
-- "LAX"	"MFE"	1.376	10
-- "MIA"	"SHV"	0.0	1
-- "MIA"	"BUF"	1.0	2
-- "MIA"	"SAN"	1.71	3
-- "MIA"	"SLC"	2.537	4
-- "MIA"	"HOU"	3.145	5
-- "MIA"	"ISP"	3.647	6
-- "MIA"	"MEM"	3.659	7
-- "MIA"	"PSE"	3.711	8
-- "MIA"	"TLH"	4.235	9
-- "MIA"	"GNV"	4.827	10
-- "SFO"	"SDF"	-7.5	1
-- "SFO"	"MSO"	-4.0	2
-- "SFO"	"PIH"	-3.0	3
-- "SFO"	"LGA"	-1.758	4
-- "SFO"	"PIE"	-1.341	5
-- "SFO"	"OAK"	-0.409	6
-- "SFO"	"FAR"	0.0	7
-- "SFO"	"BNA"	2.426	8
-- "SFO"	"MEM"	3.184	9
-- "SFO"	"SJC"	4.307	10
-- Time taken: 0.076 seconds, Fetched: 60 row(s)

-- Question 2.3 - Table
DROP TABLE flights_2_3;
CREATE EXTERNAL TABLE flights_2_3 (
  orig STRING,
  destination STRING,
  carrier STRING,
  avg_delay DOUBLE,
  rank BIGINT
)
STORED AS SEQUENCEFILE;

-- This time, select "ARR_DELAY" instead of "DEP_DELAY"
INSERT OVERWRITE TABLE flights_2_3
SELECT * 
FROM (
  SELECT
    *,
    rank() OVER (PARTITION BY orig, destination ORDER BY avg_delay, carrier) rk
  FROM (
    SELECT 
      ORIGIN AS orig,
      DEST AS destination,
      OP_UNIQUE_CARRIER AS carrier,
      round(avg(ARR_DELAY), 3) AS avg_delay
    FROM flights
    WHERE CANCELLED = 0
    GROUP BY ORIGIN, DEST, OP_UNIQUE_CARRIER
  ) unranked
) ranked
WHERE ranked.rk <= 10;

-- Question 2.3 - Query
SELECT * FROM flights_2_3 WHERE (orig LIKE '%CMI%' AND destination LIKE '%ORD%')
OR (orig LIKE '%IND%' AND destination LIKE '%CMH%')
OR (orig LIKE '%DFW%' AND destination LIKE '%IAH%')
OR (orig LIKE '%LAX%' AND destination LIKE '%SFO%')
OR (orig LIKE '%JFK%' AND destination LIKE '%LAX%')
OR (orig LIKE '%ATL%' AND destination LIKE '%PHX%');

-- Question 2.3 - Result
-- "ATL"	"PHX"	"FL"	5.023	1
-- "ATL"	"PHX"	"US"	6.207	2
-- "ATL"	"PHX"	"HP"	8.071	3
-- "ATL"	"PHX"	"DL"	9.926	4
-- "ATL"	"PHX"	"EA"	10.335	5
-- "CMI"	"ORD"	"MQ"	9.707	1
-- "DFW"	"IAH"	"PA (1)"	-1.596	1
-- "DFW"	"IAH"	"UA"	3.502	2
-- "DFW"	"IAH"	"EV"	5.093	3
-- "DFW"	"IAH"	"CO"	6.52	4
-- "DFW"	"IAH"	"OO"	7.564	5
-- "DFW"	"IAH"	"XE"	8.066	6
-- "DFW"	"IAH"	"AA"	8.12	7
-- "DFW"	"IAH"	"DL"	8.669	8
-- "DFW"	"IAH"	"MQ"	9.103	9
-- "IND"	"CMH"	"CO"	-2.549	1
-- "IND"	"CMH"	"HP"	5.316	2
-- "IND"	"CMH"	"AA"	5.5	3
-- "IND"	"CMH"	"NW"	5.762	4
-- "IND"	"CMH"	"US"	7.136	5
-- "IND"	"CMH"	"DL"	10.688	6
-- "IND"	"CMH"	"EA"	12.407	7
-- "JFK"	"LAX"	"B6"	NULL	1
-- "JFK"	"LAX"	"UA"	2.984	2
-- "JFK"	"LAX"	"AA"	6.648	3
-- "JFK"	"LAX"	"HP"	6.681	4
-- "JFK"	"LAX"	"DL"	7.385	5
-- "JFK"	"LAX"	"PA (1)"	11.294	6
-- "JFK"	"LAX"	"TW"	11.697	7
-- "LAX"	"SFO"	"TZ"	-7.619	1
-- "LAX"	"SFO"	"F9"	-2.029	2
-- "LAX"	"SFO"	"EV"	6.965	3
-- "LAX"	"SFO"	"AA"	7.553	4
-- "LAX"	"SFO"	"US"	7.727	5
-- "LAX"	"SFO"	"MQ"	7.808	6
-- "LAX"	"SFO"	"CO"	9.343	7
-- "LAX"	"SFO"	"NW"	9.849	8
-- "LAX"	"SFO"	"UA"	9.958	9
-- "LAX"	"SFO"	"WN"	10.367	10
-- Time taken: 0.081 seconds, Fetched: 39 row(s)

-- Question 2.4 - Table
DROP TABLE flights_2_4;
CREATE EXTERNAL TABLE flights_2_4 (
  orig STRING,
  destination STRING,
  avg_delay DOUBLE
)
STORED AS SEQUENCEFILE;

INSERT OVERWRITE TABLE flights_2_4
SELECT 
  ORIGIN as orig,
  DEST as destination,
  round(avg(ARR_DELAY), 3) avg_delay
FROM flights
WHERE CANCELLED = 0
GROUP BY ORIGIN, DEST;

-- Question 2.4 - Query
SELECT * FROM flights_2_4 WHERE (orig LIKE '%CMI%' AND destination LIKE '%ORD%')
OR (orig LIKE '%IND%' AND destination LIKE '%CMH%')
OR (orig LIKE '%DFW%' AND destination LIKE '%IAH%')
OR (orig LIKE '%LAX%' AND destination LIKE '%SFO%')
OR (orig LIKE '%JFK%' AND destination LIKE '%LAX%')
OR (orig LIKE '%ATL%' AND destination LIKE '%PHX%') ORDER BY orig ASC;

-- Question 2.4 - Results
-- "ATL"	"PHX"	9.047
-- "CMI"	"ORD"	9.707
-- "DFW"	"IAH"	7.563
-- "IND"	"CMH"	3.142
-- "JFK"	"LAX"	6.339
-- "LAX"	"SFO"	9.621
-- Time taken: 1.247 seconds, Fetched: 6 row(s)

-- Question 3.1 - Query
SELECT 
  orig.ORIGIN,
  orig.all_departures + destination.all_arrivals as all_flights
FROM
  (SELECT ORIGIN, count(OP_CARRIER_FL_NUM) AS all_departures FROM flights GROUP BY ORIGIN) orig
  JOIN
  (SELECT DEST, count(OP_CARRIER_FL_NUM) AS all_arrivals FROM flights GROUP BY DEST) destination
  ON orig.ORIGIN = destination.DEST
ORDER BY all_flights DESC;

-- Question 3.1 - Results
-- "ORD"	13285286
-- "ATL"	12231578
-- "DFW"	11501498
-- "LAX"	8201586
-- "PHX"	7011933
-- "DEN"	6682815
-- "DTW"	5998100
-- "IAH"	5791682
-- "MSP"	5540681
-- "SFO"	5478875
-- "STL"	5451328
-- "EWR"	5434929
-- "LAS"	5275103
-- "CLT"	5119065
-- "LGA"	4607538
-- "BOS"	4594259
-- "PHL"	4344770
-- "PIT"	4172457
-- "SLC"	4019357
-- "SEA"	3978881
-- "MCO"	3947788
-- "CVG"	3858683
-- "DCA"	3675792
-- "BWI"	3442122
-- "SAN"	3098310
-- "MIA"	2910138
-- "CLE"	2841369
-- "IAD"	2679450
-- "JFK"	2666109
-- "TPA"	2650000
-- "MEM"	2427520
-- "HOU"	2426955
-- "BNA"	2361980
-- "MCI"	2354521
-- "MDW"	2346182
-- "OAK"	2327645
-- "SJC"	2219606
-- "PDX"	2113330
-- "RDU"	2086246
-- "FLL"	2030044
-- "DAL"	1915760
-- "MSY"	1914079
-- "IND"	1653061
-- "SNA"	1647580
-- "SMF"	1631140
-- "SAT"	1621292
-- "AUS"	1609021
-- "ONT"	1552104
-- "ABQ"	1523814
-- "CMH"	1522696
-- "BDL"	1303140
-- "BUR"	1164639
-- "PBI"	1067226
-- "JAX"	1041992
-- "ELP"	1029968
-- "HNL"	1022329
-- "RNO"	1021668
-- "BUF"	981399
-- "OKC"	968173
-- "TUL"	944228
-- "SDF"	924014
-- "SJU"	923219
-- "MKE"	894300
-- "PVD"	885199
-- "ORF"	868039
-- "TUS"	828937
-- "OMA"	815023
-- "BHM"	814416
-- "RSW"	798731
-- "ANC"	792585
-- "GSO"	775789
-- "DAY"	762631
-- "ROC"	737345
-- "RIC"	719045
-- "SYR"	675488
-- "LIT"	661162
-- "ALB"	586319
-- "COS"	537667
-- "GEG"	526827
-- "GRR"	484508
-- "MHT"	480889
-- "BOI"	475479
-- "DSM"	447833
-- "CHS"	445665
-- "ICT"	421095
-- "GSP"	400564
-- "TYS"	400473
-- "JAN"	380920
-- "SAV"	372025
-- "LBB"	347079
-- "CAE"	336074
-- "SRQ"	335463
-- "MDT"	335067
-- "OGG"	329300
-- "PWM"	323725
-- "HSV"	317965
-- "ISP"	317243
-- "MAF"	314720
-- "PNS"	312947
-- "LGB"	312671
-- "BTR"	299481
-- "MSN"	297903
-- "PSP"	293491
-- "FAT"	274088
-- "AMA"	265029
-- "LEX"	262239
-- "CID"	259541
-- "SHV"	259313
-- "HPN"	251466
-- "CRP"	246143
-- "ABE"	246029
-- "MOB"	240184
-- "SBA"	240081
-- "HRL"	232133
-- "BTV"	221538
-- "FSD"	206228
-- "TLH"	205498
-- "FAI"	199630
-- "BIL"	185625
-- "JNU"	178753
-- "XNA"	177765
-- "GRB"	173969
-- "DAB"	173918
-- "SGF"	167985
-- "MYR"	165786
-- "CAK"	161478
-- "FWA"	158342
-- "KOA"	154947
-- "MLI"	154201
-- "MBS"	150240
-- "LIH"	148328
-- "EUG"	146396
-- "CHA"	142779
-- "BZN"	142596
-- "ROA"	139431
-- "MFE"	137592
-- "MRY"	135863
-- "LAN"	135562
-- "MLB"	134108
-- "FAR"	131785
-- "SBN"	131119
-- "AZO"	129675
-- "LNK"	128146
-- "GTF"	122434
-- "CRW"	122033
-- "STT"	120565
-- "RST"	120426
-- "TOL"	117664
-- "ILM"	115985
-- "VPS"	115170
-- "SWF"	114792
-- "AVL"	114440
-- "MSO"	112540
-- "FNT"	112171
-- "MFR"	110877
-- "BGR"	110490
-- "RAP"	107688
-- "AGS"	106940
-- "MGM"	106584
-- "KTN"	103639
-- "MLU"	101283
-- "GPT"	98069
-- "BIS"	97780
-- "EVV"	97475
-- "TRI"	97174
-- "AVP"	94412
-- "JAC"	91078
-- "PIA"	89652
-- "FAY"	89421
-- "PHF"	87801
-- "SBP"	83153
-- "PSC"	76872
-- "LFT"	76178
-- "BFL"	73438
-- "GNV"	72202
-- "IDA"	71433
-- "ITO"	71033
-- "TVC"	70068
-- "GJT"	69936
-- "DLH"	67680
-- "FCA"	66646
-- "LSE"	58805
-- "ERI"	58010
-- "SIT"	57522
-- "CMI"	56808
-- "MOT"	55322
-- "STX"	54994
-- "HLN"	53415
-- "DET"	53239
-- "BGM"	53052
-- "CPR"	52904
-- "PFN"	52032
-- "ATW"	50059
-- "GFK"	48361
-- "ELM"	48326
-- "EGE"	48093
-- "ACV"	47369
-- "BMI"	46278
-- "YUM"	45760
-- "GRK"	43841
-- "ABI"	43419
-- "OME"	42918
-- "ACT"	42140
-- "OTZ"	41735
-- "TYR"	41444
-- "RDM"	40815
-- "CLL"	40126
-- "BET"	39686
-- "DRO"	39228
-- "CHO"	38609
-- "SGU"	38561
-- "FSM"	38544
-- "HDN"	38184
-- "CSG"	37748
-- "ITH"	36860
-- "SUN"	36853
-- "SUX"	36069
-- "LAW"	36008
-- "LRD"	35980
-- "HTS"	35500
-- "SPS"	35121
-- "FLG"	34838
-- "EYW"	34825
-- "AEX"	34489
-- "MTJ"	33521
-- "SJT"	33151
-- "GUM"	31921
-- "CDV"	30319
-- "WRG"	30174
-- "PSG"	30143
-- "YAK"	30078
-- "BRO"	29533
-- "ORH"	28671
-- "ASE"	28554
-- "CLD"	28460
-- "ILE"	27592
-- "BRW"	26140
-- "EKO"	25511
-- "TWF"	25016
-- "RDD"	24273
-- "MOD"	23870
-- "TXK"	23470
-- "SCC"	23163
-- "SMX"	23156
-- "OAJ"	22920
-- "BTM"	22284
-- "PIH"	22051
-- "DHN"	21156
-- "ADQ"	20647
-- "DBQ"	20408
-- "GGG"	19549
-- "SPN"	18957
-- "OXR"	18942
-- "LYH"	18942
-- "ACY"	17633
-- "BQN"	17483
-- "BPT"	16911
-- "LCH"	16053
-- "ABY"	16004
-- "CWA"	15691
-- "GUC"	15334
-- "GTR"	15176
-- "HVN"	14774
-- "VLD"	14670
-- "CIC"	14559
-- "MCN"	14403
-- "BLI"	14163
-- "BQK"	14000
-- "PUB"	13546
-- "COD"	13529
-- "SCE"	13150
-- "PIE"	12934
-- "CEC"	12694
-- "DUT"	12670
-- "MEI"	12603
-- "IYK"	12179
-- "GCN"	12051
-- "AKN"	12030
-- "SPI"	11967
-- "ISO"	11851
-- "MQT"	11533
-- "IPL"	11309
-- "FLO"	10073
-- "DLG"	9888
-- "UCA"	9328
-- "CCR"	8886
-- "APF"	8027
-- "CDC"	7734
-- "SCK"	7381
-- "EFD"	7279
-- "PMD"	6722
-- "TVL"	6620
-- "PSE"	5873
-- "LWS"	5505
-- "VCT"	4879
-- "ROR"	4670
-- "EAU"	4280
-- "VIS"	3975
-- "TUP"	3954
-- "ROP"	3877
-- "GST"	3767
-- "YKM"	3709
-- "HHH"	3666
-- "ACK"	3637
-- "LWB"	3374
-- "TTN"	3358
-- "WYS"	3249
-- "RFD"	3142
-- "ALO"	3132
-- "EWN"	2784
-- "ROW"	2395
-- "GCC"	2092
-- "RKS"	1907
-- "SLE"	1764
-- "ILG"	1622
-- "PLN"	1551
-- "YAP"	1401
-- "TEX"	1366
-- "HKY"	1344
-- "CMX"	1255
-- "ADK"	1178
-- "LMT"	1136
-- "OTH"	1032
-- "ANI"	947
-- "KSM"	931
-- "RHI"	888
-- "MKG"	788
-- "SOP"	636
-- "INL"	580
-- "LNY"	579
-- "MKK"	577
-- "BJI"	404
-- "MTH"	255
-- "MAZ"	239
-- "MIB"	178
-- "FOE"	114
-- "RDR"	92
-- "OGD"	25
-- "PIR"	19
-- "CKB"	14
-- "CYS"	14
-- "PVU"	13
-- "FMN"	8
-- "GLH"	4
-- "MKC"	3
-- "BFF"	3
-- "BFI"	2
-- Time taken: 271.387 seconds --> Time taken: 89.742 seconds with hive.vectorized.execution.enabled=true and hive.auto.convert.join=true

-- Question 3.2 - Table
DROP TABLE flights_3_2;
CREATE EXTERNAL TABLE flights_3_2 (
  x STRING,
  y STRING,
  z STRING,
  xy_FL_DATE STRING,
  xy_depTime STRING,
  carrier_xy STRING,
  flight_xy STRING,
  yz_FL_DATE STRING,
  yz_depTime STRING,
  carrier_yz STRING,
  flight_yz STRING,
  total_delay DOUBLE
)
STORED AS SEQUENCEFILE;

INSERT OVERWRITE TABLE flights_3_2
SELECT 
  xy.ORIGIN AS x,
  xy.DEST AS y,
  yz.DEST AS z,
  xy.FL_DATE AS xy_FL_DATE,
  xy.CRS_DEP_TIME AS xy_depTime,
  xy.OP_UNIQUE_CARRIER AS carrier_xy,
  xy.OP_CARRIER_FL_NUM AS flight_xy,
  yz.FL_DATE AS yz_FL_DATE,
  yz.CRS_DEP_TIME AS yz_depTime,
  yz.OP_UNIQUE_CARRIER AS carrier_yz,
  yz.OP_CARRIER_FL_NUM AS flight_yz,
  xy.ARR_DELAY + yz.ARR_DELAY AS total_delay
FROM (
  SELECT
    ORIGIN,
    FL_DATE,
    CRS_DEP_TIME,
    OP_UNIQUE_CARRIER,
    OP_CARRIER_FL_NUM,
    DEST,
    ARR_DELAY,
    rank() OVER (PARTITION BY ORIGIN, DEST, yeard, monthd, ORDER BY ARR_DELAY ASC) rk
  FROM flights
  WHERE substring(CRS_DEP_TIME,2,4) < 1200 AND yeard = 2008 AND CANCELLED = 0
) xy
JOIN (
  SELECT
    ORIGIN,
    FL_DATE,
    CRS_DEP_TIME,
    OP_UNIQUE_CARRIER,
    OP_CARRIER_FL_NUM,
    DEST,
    ARR_DELAY,
    rank() OVER (PARTITION BY ORIGIN, DEST, yeard, monthd, ORDER BY ARR_DELAY ASC) rk
  FROM flights
  WHERE substring(CRS_DEP_TIME,2,4) > 1200 AND yeard = 2008 AND CANCELLED = 0
) yz
ON xy.rk = 1 AND yz.rk = 1 AND xy.DEST = yz.ORIGIN AND date_add(xy.FL_DATE, 2) = yz.FL_DATE;

-- Question 3.2 - Query
SELECT *
FROM flights_3_2
WHERE (x LIKE "%CMI%" AND y LIKE "%ORD%" AND z LIKE "%LAX%" AND cast(xy_FL_DATE AS DATE) = "2008-03-04")
  OR (x LIKE "%JAX%" AND y LIKE "%DFW%" AND z LIKE "%CRP%" AND cast(xy_FL_DATE AS DATE) = "2008-09-09")
  OR (x LIKE "%SLC%" AND y LIKE "%BFL%" AND z LIKE "%LAX%" AND cast(xy_FL_DATE AS DATE) = "2008-04-01")
  OR (x LIKE "%LAX%" AND y LIKE "%SFO%" AND z LIKE "%PHX%" AND cast(xy_FL_DATE AS DATE) = "2008-07-12")
  OR (x LIKE "%DFW%" AND y LIKE "%ORD%" AND z LIKE "%DFW%" AND cast(xy_FL_DATE AS DATE) = "2008-06-10")
  OR (x LIKE "%LAX%" AND y LIKE "%ORD%" AND z LIKE "%JFK%" AND cast(xy_FL_DATE AS DATE) = "2008-01-01")
ORDER BY x;

-- Question 3.2 - Results
"CMI"	"ORD"	"LAX"	2008-03-04	"0710"	"MQ"	"4278"	2008-03-06	"1950"	"AA"	"607"	-38.0
"DFW"	"ORD"	"DFW"	2008-06-10	"0700"	"UA"	"1104"	2008-06-12	"1645"	"AA"	"2341"	-31.0
"JAX"	"DFW"	"CRP"	2008-09-09	"0725"	"AA"	"845"	2008-09-11	"1645"	"MQ"	"3627"	-6.0
"LAX"	"SFO"	"PHX"	2008-07-12	"0650"	"WN"	"3534"	2008-07-14	"1925"	"US"	"412"	-32.0
"LAX"	"ORD"	"JFK"	2008-01-01	"0705"	"UA"	"944"	2008-01-03	"1900"	"B6"	"918"	-6.0
"SLC"	"BFL"	"LAX"	2008-04-01	"1100"	"OO"	"3755"	2008-04-03	"1455"	"OO"	"5429"	18.0
Time taken: 175.682 seconds --> with hive.vectorized.execution.enabled=true and hive.auto.convert.join=true
