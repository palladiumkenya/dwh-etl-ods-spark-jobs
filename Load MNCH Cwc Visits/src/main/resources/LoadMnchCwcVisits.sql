SELECT Distinct p.[PatientMnchID],p.[PatientPk],P.[SiteCode],p.[FacilityName],P.EMR,p.[Project],cast(p.[DateExtracted] as date)[DateExtracted]
              ,cast(p.[VisitDate] as date)[VisitDate],[VisitID],[Height],[Weight],[Temp],[PulseRate],[RespiratoryRate],[OxygenSaturation]
              ,[MUAC],[WeightCategory],[Stunted],[InfantFeeding],[MedicationGiven],[TBAssessment],[MNPsSupplementation],[Immunization]
              ,[DangerSigns],[Milestones],[VitaminA],[Disability],[ReceivedMosquitoNet],[Dewormed],[ReferredFrom],[ReferredTo],[ReferralReasons]
              ,[FollowUP],cast([NextAppointment] as date)[NextAppointment],p.[Date_Last_Modified]
              ,ZScore,ZScoreAbsolute
              ,HeightLength,Refferred,RevisitThisYear,RecordUUID
FROM [MNCHCentral].[dbo].[CwcVisits] P (Nolock)
    inner join (select tn.PatientPK,tn.SiteCode,tn.[VisitDate],Max(ID) As MaxID,max(cast(tn.DateExtracted as date))MaxDateExtracted FROM [MNCHCentral].[dbo].[CwcVisits] (NoLock)tn
    group by tn.PatientPK,tn.SiteCode,tn.[VisitDate])tm
on P.PatientPk = tm.PatientPk and p.SiteCode = tm.SiteCode and cast(p.DateExtracted as date) = tm.MaxDateExtracted and p.ID = tm.MaxID
    INNER JOIN [MNCHCentral].[dbo].[Facilities] F ON P.[FacilityId] = F.Id