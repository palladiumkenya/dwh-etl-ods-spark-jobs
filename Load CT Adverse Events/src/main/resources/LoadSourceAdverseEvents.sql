SELECT DISTINCT P.[patientcccnumber] AS PatientID,
                P.[patientpid]       AS PatientPK,
                F.NAME               AS FacilityName,
                F.code               AS SiteCode,
    [adverseevent],
    [adverseeventstartdate],
    [adverseeventenddate],
    CASE [severity]
    WHEN '1' THEN 'Mild'
    WHEN '2' THEN 'Moderate'
    WHEN '3' THEN 'Severe'
    ELSE [severity]
END                  AS [Severity],
                          [visitdate],
                          PA.[emr],
                          PA.[project],
                          [adverseeventcause],
                          [adverseeventregimen],
                          [adverseeventactiontaken],
                          [adverseeventclinicaloutcome],
                          [adverseeventispregnant],
                          PA.id,
                          PA.[date_created],
                          PA.[date_last_modified],
                          PA.recorduuid,
                          PA.voided
          FROM   [DWAPICentral].[dbo].[patientextract](nolock) P
                 INNER JOIN
                 [DWAPICentral].[dbo].patientadverseeventextract(nolock)
                 PA
                         ON PA.[patientid] = P.id
                 INNER JOIN [DWAPICentral].[dbo].[facility](nolock) F
                         ON P.[facilityid] = F.id
                            AND F.voided = 0
                            AND F.code > 0