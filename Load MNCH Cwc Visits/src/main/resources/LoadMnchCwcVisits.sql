SELECT Distinct p.[PatientMnchID],p.[PatientPk],P.[SiteCode],p.[FacilityName],P.EMR,p.[Project],cast(p.[DateExtracted] as date)[DateExtracted]
              ,cast(p.[VisitDate] as date)[VisitDate],[VisitID],[Height],[Weight],[Temp],[PulseRate],[RespiratoryRate],[OxygenSaturation]
              ,[MUAC],[WeightCategory],[Stunted],[InfantFeeding],[MedicationGiven],[TBAssessment],[MNPsSupplementation],[Immunization]
              ,[DangerSigns],[Milestones],[VitaminA],[Disability],[ReceivedMosquitoNet],[Dewormed],[ReferredFrom],[ReferredTo],[ReferralReasons]
              ,[FollowUP],cast([NextAppointment] as date)[NextAppointment],p.[Date_Last_Modified]
              ,ZScore,ZScoreAbsolute
              ,HeightLength,Refferred,RevisitThisYear
FROM [MNCHCentral].[dbo].[CwcVisits] P (Nolock)
    inner join (select tn.PatientPK,tn.SiteCode,tn.[VisitDate],max(tn.DateExtracted)MaxDateExtracted FROM [MNCHCentral].[dbo].[CwcVisits] (NoLock)tn
    group by tn.PatientPK,tn.SiteCode,tn.[VisitDate])tm
on P.PatientPk = tm.PatientPk and p.SiteCode = tm.SiteCode and p.DateExtracted = tm.MaxDateExtracted
    -- INNER JOIN  [MNCHCentral].[dbo].[MnchPatients] MnchP(Nolock)
    --on P.patientPK = MnchP.patientPK and P.Sitecode = MnchP.Sitecode
    INNER JOIN [MNCHCentral].[dbo].[Facilities] F ON P.[FacilityId] = F.Id