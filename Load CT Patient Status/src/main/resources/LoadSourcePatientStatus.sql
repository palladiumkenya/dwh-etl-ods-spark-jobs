SELECT
    DISTINCT P.[patientcccnumber] AS PatientID,
             P.[patientpid] AS PatientPK,
             F.NAME AS FacilityName,
             F.code AS SiteCode,
             PS.[exitdescription] ExitDescription,
             PS.[exitdate] ExitDate,
             PS.[exitreason] ExitReason,
             P.[emr] Emr,
             CASE P.[project] WHEN 'I-TECH' THEN 'Kenya HMIS II' WHEN 'HMIS' THEN 'Kenya HMIS II' ELSE P.[project] END AS [Project],
    PS.[voided] Voided,
    PS.[processed] Processed,
    PS.[created] Created,
    [reasonfordeath],
    [specificdeathreason],
    Cast([deathdate] AS DATE) [DeathDate],
    effectivediscontinuationdate,
    PS.toverified TOVerified,
    PS.toverifieddate TOVerifiedDate,
    PS.reenrollmentdate ReEnrollmentDate,
    PS.[date_created],
    PS.[date_last_modified],
    PS.[recorduuid]
FROM
    [DWAPICentral].[dbo].[patientextract] P WITH (nolock)
    INNER JOIN [DWAPICentral].[dbo].[patientstatusextract]PS WITH (nolock) ON PS.[patientid] = P.id
    INNER JOIN [DWAPICentral].[dbo].[facility] F (nolock) ON P.[facilityid] = F.id
    AND F.voided = 0
    INNER JOIN (
    SELECT
    P.patientpid,
    F.code,
    exitdate,
    ps.voided,
    max(PS.ID) As Max_ID,
    Max(
    Cast(Ps.created AS DATE)
    ) MaxCreated
    FROM
    [DWAPICentral].[dbo].[patientextract] P WITH (nolock)
    INNER JOIN [DWAPICentral].[dbo].[patientstatusextract]PS WITH (nolock) ON PS.[patientid] = P.id
    INNER JOIN [DWAPICentral].[dbo].[facility] F (nolock) ON P.[facilityid] = F.id
    AND F.voided = 0
    GROUP BY
    P.patientpid,
    F.code,
    exitdate,
    ps.voided
    ) tn ON P.patientpid = tn.patientpid
    AND f.code = tn.code
    AND PS.exitdate = tn.exitdate
    AND Cast(PS.created AS DATE) = tn.maxcreated
    and PS.ID = tn.Max_ID
WHERE
    p.gender != 'Unknown'
  AND F.code > 0