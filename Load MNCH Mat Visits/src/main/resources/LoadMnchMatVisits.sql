SELECT distinct  P.[PatientPk],P.[SiteCode],P.[Emr], P.[Project], P.[Processed], P.[QueueId], P.[Status], P.[StatusDate], P.[DateExtracted]
              , P.[FacilityId], P.[PatientMnchID], P.[FacilityName],[VisitID],[VisitDate],[AdmissionNumber],[ANCVisits],[DateOfDelivery]
              ,[DurationOfDelivery],[GestationAtBirth],[ModeOfDelivery],[PlacentaComplete],[UterotonicGiven],[VaginalExamination]
              ,[BloodLoss],[BloodLossVisual],[ConditonAfterDelivery],[MaternalDeath],[DeliveryComplications],[NoBabiesDelivered]
              ,[BabyBirthNumber],[SexBaby],[BirthWeight],[BirthOutcome],[BirthWithDeformity],[TetracyclineGiven],[InitiatedBF],[ApgarScore1]
              ,[ApgarScore5],[ApgarScore10],[KangarooCare],[ChlorhexidineApplied],[VitaminKGiven],[StatusBabyDischarge],[MotherDischargeDate]
              ,[SyphilisTestResults],[HIVStatusLastANC],[HIVTestingDone],[HIVTest1],[HIV1Results],[HIVTest2],[HIV2Results],[HIVTestFinalResult]
              ,[OnARTANC],[BabyGivenProphylaxis],[MotherGivenCTX],[PartnerHIVTestingMAT],[PartnerHIVStatusMAT],[CounselledOn],[ReferredFrom]
              ,[ReferredTo],[ClinicalNotes]
              ,[EDD]
              ,[LMP]
              ,[MaternalDeathAudited]
              ,[OnARTMat]
              ,[ReferralReason],RecordUUID
FROM [MNCHCentral].[dbo].[MatVisits] P(Nolock)
    inner join (select tn.PatientPK,tn.SiteCode,max(ID) As MaxID,max(cast(tn.DateExtracted as date))MaxDateExtracted FROM [MNCHCentral].[dbo].[MatVisits] (NoLock)tn
    group by tn.PatientPK,tn.SiteCode)tm
on P.PatientPk = tm.PatientPk and p.SiteCode = tm.SiteCode and cast(p.DateExtracted as date) = tm.MaxDateExtracted and p.ID = tm.MaxID
    INNER JOIN [MNCHCentral].[dbo].[Facilities] F ON P.[FacilityId] = F.Id