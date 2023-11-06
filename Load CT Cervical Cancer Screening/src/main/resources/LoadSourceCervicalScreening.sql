SELECT DISTINCT
    f.code AS SiteCode,p.PatientPID AS PatientPK,p.PatientCccNumber AS PatientID,ccs.[Emr],ccs.[Project],ccs.[Voided],ccs.[Processed]
              ,ccs.[Id],[FacilityName],[VisitID],[VisitDate],[VisitType],[ScreeningMethod],[TreatmentToday]
              ,[ReferredOut],[NextAppointmentDate],[ScreeningType],[ScreeningResult],[PostTreatmentComplicationCause]
              ,[OtherPostTreatmentComplication],[ReferralReason],ccs.[Created],ccs.[Date_Created],ccs.[Date_Last_Modified]
FROM [DWAPICentral].[dbo].[CervicalCancerScreeningExtract] ccs
    INNER JOIN [DWAPICentral].[dbo].[PatientExtract] P
ON ccs.[PatientId]= P.ID AND ccs.Voided=0
    INNER JOIN [DWAPICentral].[dbo].[Facility] F ON P.[FacilityId] = F.Id AND F.Voided=0
WHERE p.gender!='Unknown'