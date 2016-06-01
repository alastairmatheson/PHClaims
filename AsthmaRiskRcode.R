##########################################################
# Generate data for U01 prediction model
# This code identifies children with an asmtha-related claim in 2014.
# It then brings in 2015 claims to look at risk factors for asmtha-related hospital or ED visit

# APDE, PHSKC
# SQL code by Lin Song, edited by Alastair Matheson to work in R and include medication
# 2016-05-26
##########################################################

options(max.print = 600)

library(RODBC) # used to connect to SQL server
library(dplyr) # used to manipulate data
library(reshape2) # used to reshape data


# DATA SETUP --------------------------------------------------------------

### Connect to the server
db.claims <- odbcConnect("PHClaims")


##### Bring in all the relevant eligibility data #####

# Bring in 2014 data for children aged 3-17 (born between 1997-2011) in 2014
ptm01 <- proc.time() # Times how long this query takes (~21 secs)
elig <-
  sqlQuery(
    db.claims,
    "SELECT CAL_YEAR AS 'Year', MEDICAID_RECIPIENT_ID AS 'ID2014', SOCIAL_SECURITY_NMBR AS 'SSN',
    GENDER AS 'Gender', RACE1  AS 'Race1', RACE2 AS 'Race2', HISPANIC_ORIGIN_NAME AS 'Hispanic',
    BIRTH_DATE AS 'DOB', CTZNSHP_STATUS_NAME AS 'Citizenship', INS_STATUS_NAME AS 'Immigration',
    SPOKEN_LNG_NAME AS 'Lang', FPL_PRCNTG AS 'FPL', RAC_CODE AS 'RACcode', RAC_NAME AS 'RACname',
    FROM_DATE AS 'FromDate', TO_DATE AS 'ToDate',
    covtime = DATEDIFF(dd,FROM_DATE, CASE WHEN TO_DATE > GETDATE() THEN GETDATE() ELSE TO_DATE END),
    END_REASON AS 'EndReason', COVERAGE_TYPE_IND AS 'Coverage', POSTAL_CODE AS 'Zip',
    ROW_NUMBER() OVER(PARTITION BY MEDICAID_RECIPIENT_ID ORDER BY MEDICAID_RECIPIENT_ID, FROM_DATE DESC, TO_DATE DESC) AS 'Row'
    FROM dbo.vEligibility
    WHERE CAL_YEAR=2014 AND BIRTH_DATE BETWEEN '1997-01-01' AND '2011-12-31'
    ORDER BY MEDICAID_RECIPIENT_ID, FROM_DATE DESC, TO_DATE DESC"
  )
proc.time() - ptm01


# Keep the last row from 2014 for each child
elig2014 <- elig %>%
  group_by(ID2014) %>%
  filter(row_number() == n())


# Select children in the following year to be matched with baseline
elig2015 <-
  sqlQuery(
    db.claims,
    "SELECT DISTINCT MEDICAID_RECIPIENT_ID AS 'ID2015'
    FROM dbo.vEligibility
    WHERE CAL_YEAR = 2015 AND BIRTH_DATE BETWEEN '1997-01-01' AND '2011-12-31'
    GROUP BY MEDICAID_RECIPIENT_ID"
  )


# Match baseline with the following year (only include children present in both years)
eligall <- merge(elig2014, elig2015, by.x = "ID2014", by.y = "ID2015")


##### Bring in all the relevant claims data ####

# Baseline hospitalizations and ED visits
hospED <-
  sqlQuery(
    db.claims,
    "SELECT DISTINCT MEDICAID_RECIPIENT_ID AS 'ID2014',
    SUM(CASE WHEN CAL_YEAR = 2014 AND CLM_TYPE_CID = 31 THEN 1 ELSE 0 END) AS 'Hosp',
    SUM(CASE WHEN CAL_YEAR=2014 AND REVENUE_CODE IN ('0450','0456','0459','0981') THEN 1 ELSE 0 END) AS 'ED'
    FROM dbo.vClaims
    GROUP BY MEDICAID_RECIPIENT_ID"
  )


# Baseline claims for patients with asthma
ptm02 <- proc.time() # Times how long this query takes (~65 secs)
asthma <-
  sqlQuery(
    db.claims,
    "SELECT MEDICAID_RECIPIENT_ID AS 'ID2014', *
    FROM dbo.vClaims
    WHERE CAL_YEAR = 2014
    AND (PRIMARY_DIAGNOSIS_CODE LIKE '493%' OR PRIMARY_DIAGNOSIS_CODE LIKE 'J45%'
    OR DIAGNOSIS_CODE_2 LIKE '493%' OR DIAGNOSIS_CODE_2 LIKE 'J45%'
    OR DIAGNOSIS_CODE_3 LIKE '493%' OR DIAGNOSIS_CODE_3 LIKE 'J45%'
    OR DIAGNOSIS_CODE_4 LIKE '493%' OR DIAGNOSIS_CODE_4 LIKE 'J45%'
    OR DIAGNOSIS_CODE_5 LIKE '493%' OR DIAGNOSIS_CODE_5 LIKE 'J45%')"
  )
proc.time() - ptm02


##### Merge all eligible children with asthma claims
asthmachild <- merge(eligall, asthma, by = "ID2014")

# Count up number of predictors for each child
asthmarisk <- asthmachild %>%
  group_by(ID2014) %>%
  mutate(hospcount = summarise(ifelse(CAL_YEAR=2014 & CLM_TYPE_CID=31,1,0)))
           
           
           
           
           
           ifelse(substr(PRIMARY_DIAGNOSIS_CODE, 1, 3) == "493" | 
                              substr(PRIMARY_DIAGNOSIS_CODE, 1, 3) == "J45" |
                              substr(DIAGNOSIS_CODE_2, 1, 3) == "493" |
                              substr(DIAGNOSIS_CODE_2, 1, 3) == "J45" |
                              substr(DIAGNOSIS_CODE_3, 1, 3) == "493" |
                              substr(DIAGNOSIS_CODE_3, 1, 3) == "J45" |
                              substr(DIAGNOSIS_CODE_4, 1, 3) == "493" |
                              substr(DIAGNOSIS_CODE_4, 1, 3) == "J45" |
                              substr(DIAGNOSIS_CODE_5, 1, 3) == "493" |
                              substr(DIAGNOSIS_CODE_5, 1, 3) == "J45" |
                              , 1, 0))


--#B. SELECT 2014 BASELINE CLAIMS DATA FOR PATIENTS WITH ASTHMA
  
  
  --Generate predictors from #B, such as hospitalization, ED visit, urgent care visit, other visit type? comorbidity?
--#B1=number of baseline hospitalizations for asthma, any diagnosis
  SELECT DISTINCT ID2014, COUNT(DISTINCT FROM_SRVC_DATE) AS H1 INTO #B1
FROM #B
WHERE CAL_YEAR=2014 AND CLM_TYPE_CID=31
AND (PRIMARY_DIAGNOSIS_CODE LIKE '493%' OR PRIMARY_DIAGNOSIS_CODE LIKE 'J45%'
     OR DIAGNOSIS_CODE_2 LIKE '493%' OR DIAGNOSIS_CODE_2 LIKE 'J45%'
     OR DIAGNOSIS_CODE_3 LIKE '493%' OR DIAGNOSIS_CODE_3 LIKE 'J45%'
     OR DIAGNOSIS_CODE_4 LIKE '493%' OR DIAGNOSIS_CODE_4 LIKE 'J45%'
     OR DIAGNOSIS_CODE_5 LIKE '493%' OR DIAGNOSIS_CODE_5 LIKE 'J45%')
GROUP BY ID2014;

--#B2=number of baseline hospitalizations for asthma, primary diagnosis
  DROP TABLE #B2
SELECT DISTINCT ID2014, COUNT(DISTINCT FROM_SRVC_DATE) AS H2 INTO #B2
FROM #B
WHERE CAL_YEAR=2014 AND CLM_TYPE_CID=31
AND (PRIMARY_DIAGNOSIS_CODE LIKE '493%' OR PRIMARY_DIAGNOSIS_CODE LIKE 'J45%')
GROUP BY ID2014;

--#B3=number of ER visit for asthma, any diagnosis
  SELECT DISTINCT ID2014, COUNT(DISTINCT FROM_SRVC_DATE) AS E1 INTO #B3	
FROM #B
WHERE CAL_YEAR=2014 AND REVENUE_CODE IN ('0450','0456','0459','0981')
AND (PRIMARY_DIAGNOSIS_CODE LIKE '493%' OR PRIMARY_DIAGNOSIS_CODE LIKE 'J45%'
     OR DIAGNOSIS_CODE_2 LIKE '493%' OR DIAGNOSIS_CODE_2 LIKE 'J45%'
     OR DIAGNOSIS_CODE_3 LIKE '493%' OR DIAGNOSIS_CODE_3 LIKE 'J45%'
     OR DIAGNOSIS_CODE_4 LIKE '493%' OR DIAGNOSIS_CODE_4 LIKE 'J45%'
     OR DIAGNOSIS_CODE_5 LIKE '493%' OR DIAGNOSIS_CODE_5 LIKE 'J45%')
GROUP BY ID2014;

DROP TABLE #B4
--#B4=number of ER visit for asthma, primary diagnosis
  SELECT DISTINCT ID2014, COUNT(DISTINCT FROM_SRVC_DATE) AS E2 INTO #B4	
FROM #B
WHERE CAL_YEAR=2014 AND REVENUE_CODE IN ('0450','0456','0459','0981')
AND (PRIMARY_DIAGNOSIS_CODE LIKE '493%' OR PRIMARY_DIAGNOSIS_CODE LIKE 'J45%')
GROUP BY ID2014;


DROP TABLE #B5
--#B5=well child check up visit for asthma, any diagnosis
  SELECT DISTINCT ID2014, COUNT(DISTINCT FROM_SRVC_DATE) AS C1 INTO #B5	
FROM #B
WHERE CAL_YEAR=2014 AND CLM_TYPE_CID=27
AND (PRIMARY_DIAGNOSIS_CODE LIKE '493%' OR PRIMARY_DIAGNOSIS_CODE LIKE 'J45%'
     OR DIAGNOSIS_CODE_2 LIKE '493%' OR DIAGNOSIS_CODE_2 LIKE 'J45%'
     OR DIAGNOSIS_CODE_3 LIKE '493%' OR DIAGNOSIS_CODE_3 LIKE 'J45%'
     OR DIAGNOSIS_CODE_4 LIKE '493%' OR DIAGNOSIS_CODE_4 LIKE 'J45%'
     OR DIAGNOSIS_CODE_5 LIKE '493%' OR DIAGNOSIS_CODE_5 LIKE 'J45%')
GROUP BY ID2014;

--#B6=number of well child checkup visit for asthma, primary diagnosis
  DROP TABLE #B6
SELECT DISTINCT ID2014, COUNT(DISTINCT FROM_SRVC_DATE) AS C2 INTO #B6	
FROM #B
WHERE CAL_YEAR=2014 AND CLM_TYPE_CID=27
AND (PRIMARY_DIAGNOSIS_CODE LIKE '493%' OR PRIMARY_DIAGNOSIS_CODE LIKE 'J45%')
GROUP BY ID2014;

--#B7=number of asthma-related claims, any diagnosis
  SELECT DISTINCT ID2014, COUNT(DISTINCT TCN) AS A1 INTO #B7	
FROM #B
WHERE CAL_YEAR=2014
AND (PRIMARY_DIAGNOSIS_CODE LIKE '493%' OR PRIMARY_DIAGNOSIS_CODE LIKE 'J45%'
     OR DIAGNOSIS_CODE_2 LIKE '493%' OR DIAGNOSIS_CODE_2 LIKE 'J45%'
     OR DIAGNOSIS_CODE_3 LIKE '493%' OR DIAGNOSIS_CODE_3 LIKE 'J45%'
     OR DIAGNOSIS_CODE_4 LIKE '493%' OR DIAGNOSIS_CODE_4 LIKE 'J45%'
     OR DIAGNOSIS_CODE_5 LIKE '493%' OR DIAGNOSIS_CODE_5 LIKE 'J45%')
GROUP BY ID2014;

--#B8=number of asthma-related claims, primary diagnosis
  DROP TABLE #B8
SELECT DISTINCT ID2014, COUNT(DISTINCT TCN) AS A2 INTO #B8	
FROM #B
WHERE CAL_YEAR=2014
AND (PRIMARY_DIAGNOSIS_CODE LIKE '493%' OR PRIMARY_DIAGNOSIS_CODE LIKE 'J45%')
GROUP BY ID2014;

---------------------------
  --#C. SELECT 2015 (SUBSEQUENT YEAR) CLAIMS DATA FOR PATIENTS WITH ASTHMA
  DROP TABLE #C
SELECT * INTO #C
FROM PHClaims.dbo.vClaims
INNER JOIN #A5
ON PHClaims.dbo.vClaims.MEDICAID_RECIPIENT_ID=#A5.ID2014
  WHERE CAL_YEAR=2015
AND (PRIMARY_DIAGNOSIS_CODE LIKE '493%' OR PRIMARY_DIAGNOSIS_CODE LIKE 'J45%'
     OR DIAGNOSIS_CODE_2 LIKE '493%' OR DIAGNOSIS_CODE_2 LIKE 'J45%'
     OR DIAGNOSIS_CODE_3 LIKE '493%' OR DIAGNOSIS_CODE_3 LIKE 'J45%'
     OR DIAGNOSIS_CODE_4 LIKE '493%' OR DIAGNOSIS_CODE_4 LIKE 'J45%'
     OR DIAGNOSIS_CODE_5 LIKE '493%' OR DIAGNOSIS_CODE_5 LIKE 'J45%');

--Generate outcome variables from #C, such as hospitalization, ED visit, 
--#C1=number of hospitalizations for asthma, any diagnosis, OUTCOME VARIABLE
  DROP TABLE #C1
SELECT DISTINCT ID2014, COUNT(DISTINCT FROM_SRVC_DATE) AS EX_H1 INTO #C1
FROM #C
WHERE CAL_YEAR=2015 AND CLM_TYPE_CID=31
AND (PRIMARY_DIAGNOSIS_CODE LIKE '493%' OR PRIMARY_DIAGNOSIS_CODE LIKE 'J45%'
     OR DIAGNOSIS_CODE_2 LIKE '493%' OR DIAGNOSIS_CODE_2 LIKE 'J45%'
     OR DIAGNOSIS_CODE_3 LIKE '493%' OR DIAGNOSIS_CODE_3 LIKE 'J45%'
     OR DIAGNOSIS_CODE_4 LIKE '493%' OR DIAGNOSIS_CODE_4 LIKE 'J45%'
     OR DIAGNOSIS_CODE_5 LIKE '493%' OR DIAGNOSIS_CODE_5 LIKE 'J45%')
GROUP BY ID2014;

--#C2=number of EXIT hospitalizations for asthma, primary diagnosis
  DROP TABLE #C2
SELECT DISTINCT ID2014, COUNT(DISTINCT FROM_SRVC_DATE) AS EX_H2 INTO #C2
FROM #C
WHERE CAL_YEAR=2015 AND CLM_TYPE_CID=31
AND (PRIMARY_DIAGNOSIS_CODE LIKE '493%' OR PRIMARY_DIAGNOSIS_CODE LIKE 'J45%')
GROUP BY ID2014;

--#C3=number of EXIT ER visit for asthma, any diagnosis
  DROP TABLE #C3
SELECT DISTINCT ID2014, COUNT(DISTINCT FROM_SRVC_DATE) AS EX_E1 INTO #C3	
FROM #C
WHERE CAL_YEAR=2015 AND REVENUE_CODE IN ('0450','0456','0459','0981')
AND (PRIMARY_DIAGNOSIS_CODE LIKE '493%' OR PRIMARY_DIAGNOSIS_CODE LIKE 'J45%'
     OR DIAGNOSIS_CODE_2 LIKE '493%' OR DIAGNOSIS_CODE_2 LIKE 'J45%'
     OR DIAGNOSIS_CODE_3 LIKE '493%' OR DIAGNOSIS_CODE_3 LIKE 'J45%'
     OR DIAGNOSIS_CODE_4 LIKE '493%' OR DIAGNOSIS_CODE_4 LIKE 'J45%'
     OR DIAGNOSIS_CODE_5 LIKE '493%' OR DIAGNOSIS_CODE_5 LIKE 'J45%')
GROUP BY ID2014;

DROP TABLE #C4
--#C4=number of EXIT ER visit for asthma, primary diagnosis
  SELECT DISTINCT ID2014, COUNT(DISTINCT FROM_SRVC_DATE) AS EX_E2 INTO #C4	
FROM #C
WHERE CAL_YEAR=2015 AND REVENUE_CODE IN ('0450','0456','0459','0981')
AND (PRIMARY_DIAGNOSIS_CODE LIKE '493%' OR PRIMARY_DIAGNOSIS_CODE LIKE 'J45%')
GROUP BY ID2014;

SELECT * FROM #B1

----xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
/*Join files */
  SELECT #A5.ID2014,
#T1.T1,
#T2.T2,
#B7.A1,
#B8.A2,
#B1.H1,
#B2.H2,
#B3.E1,
#B4.E2,
#B5.C1,
#B6.C2,
#C1.EX_H1,
#C2.EX_H2,
#C3.EX_E1,
#C4.EX_E2
INTO #D1
FROM #A5
INNER JOIN #B7
ON #A5.ID2014=#B7.ID2014
LEFT JOIN #B8
ON #A5.ID2014=#B8.ID2014
LEFT JOIN #B1
ON #A5.ID2014=#B1.ID2014
LEFT JOIN #B2
ON #A5.ID2014=#B2.ID2014
LEFT JOIN #B3
ON #A5.ID2014=#B3.ID2014
LEFT JOIN #B4
ON #A5.ID2014=#B4.ID2014
LEFT JOIN #B5
ON #A5.ID2014=#B5.ID2014
LEFT JOIN #B6
ON #A5.ID2014=#B6.ID2014
LEFT JOIN #C1
ON #A5.ID2014=#C1.ID2014
LEFT JOIN #C2
ON #A5.ID2014=#C2.ID2014
LEFT JOIN #C3
ON #A5.ID2014=#C3.ID2014
LEFT JOIN #C4
ON #A5.ID2014=#C4.ID2014
LEFT JOIN #T1
ON #A5.ID2014=#T1.ID2014
LEFT JOIN #T2
ON #A5.ID2014=#T2.ID2014;



SELECT * FROM #D1

SELECT * FROM #A4
INNER JOIN #D1
ON #A4.ID2014=#D1.ID2014;