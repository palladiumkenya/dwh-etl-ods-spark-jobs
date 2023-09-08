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
              ,[ReferralReason]
FROM [MNCHCentral].[dbo].[MatVisits] P(Nolock)
    inner join (select tn.PatientPK,tn.SiteCode,max(tn.DateExtracted)MaxDateExtracted FROM [MNCHCentral].[dbo].[MatVisits] (NoLock)tn
    group by tn.PatientPK,tn.SiteCode)tm
on P.PatientPk = tm.PatientPk and p.SiteCode = tm.SiteCode and p.DateExtracted = tm.MaxDateExtracted
    -- 	INNER JOIN  [MNCHCentral].[dbo].[MnchPatients] MnchP(Nolock) -- to be reviwed later
    --on P.patientPK = MnchP.patientPK and P.Sitecode = MnchP.Sitecode
    INNER JOIN [MNCHCentral].[dbo].[Facilities] F ON P.[FacilityId] = F.Id