SELECT DISTINCT  P.PatientCccNumber,p.PatientPID As PatientPK,F.Code As SiteCode,F.[Name],IIT.[Emr],IIT.[Project],IIT.[Voided],IIT.[Processed],IIT.[Id],[FacilityName],[PatientId],[SourceSysUUID]
        ,[RiskScore],[RiskFactors],[RiskDescription],[RiskEvaluationDate],IIT.[Created],IIT.[Date_Created]
        ,IIT.[Date_Last_Modified]
FROM [DWAPICentral].[dbo].[IITRiskScoresExtract] IIT
    INNER JOIN [DWAPICentral].[dbo].[PatientExtract] P
ON IIT.[PatientId]= P.ID
    INNER JOIN [DWAPICentral].[dbo].[Facility] F ON P.[FacilityId] = F.Id AND F.Voided=0
WHERE p.gender!='Unknown' AND F.code >0