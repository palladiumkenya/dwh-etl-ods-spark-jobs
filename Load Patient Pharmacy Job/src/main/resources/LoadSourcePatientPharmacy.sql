SELECT
    P.[PatientCccNumber] AS PatientID, P.[PatientPID] AS PatientPK,F.[Name] AS FacilityName, F.Code AS SiteCode,PP.[VisitID] VisitID,PP.[Drug] Drug
     ,PP.[DispenseDate] DispenseDate,PP.[Duration] Duration,PP.[ExpectedReturn] ExpectedReturn,PP.[TreatmentType] TreatmentType
     ,PP.[PeriodTaken] PeriodTaken,PP.[ProphylaxisType] ProphylaxisType,P.[Emr] Emr
     ,CASE P.[Project]
          WHEN 'I-TECH' THEN 'Kenya HMIS II'
          WHEN 'HMIS' THEN 'Kenya HMIS II'
          ELSE P.[Project]
    END AS [Project]
					  ,PP.[Voided] Voided
					  ,PP.[Processed] Processed
					  ,PP.[Provider] [Provider]
					  ,PP.[RegimenLine] RegimenLine
					  ,PP.[Created] Created
					  ,PP.RegimenChangedSwitched RegimenChangedSwitched
					  ,PP.RegimenChangeSwitchReason RegimenChangeSwitchReason
					  ,PP.StopRegimenReason StopRegimenReason
					  ,PP.StopRegimenDate StopRegimenDate
					  ,LTRIM(RTRIM(STR(F.Code)))+'-'+LTRIM(RTRIM(P.[PatientCccNumber])) +'-'+LTRIM(RTRIM(STR(P.[PatientPID]))) AS CKV,
					  0 AS IsRegimenFlag,
					  0 AS KnockOutDrug
					  ,P.ID as PatientUnique_ID
					  ,PP.ID as PatientPharmacyUnique_ID
FROM [DWAPICentral].[dbo].[PatientExtract] P
    INNER JOIN [DWAPICentral].[dbo].[PatientArtExtract] PA ON PA.[PatientId]= P.ID
    INNER JOIN [DWAPICentral].[dbo].[PatientPharmacyExtract] PP ON PP.[PatientId]= P.ID AND PP.Voided=0
    INNER JOIN [DWAPICentral].[dbo].[Facility] F ON P.[FacilityId] = F.Id AND F.Voided=0
WHERE p.gender!='Unknown'