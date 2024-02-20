SELECT  DISTINCT F.Code AS SiteCode
               ,P.patientPID AS PatientPK
               ,P.PatientcccNumber AS PatientID
               ,CSE.Emr
               ,CSE.Project
               ,CSE.Voided
               ,CSE.Id
               ,F.[name] AS FacilityName
               ,VisitType
               ,VisitID
               ,VisitDate
               ,SmokesCigarette
               ,NumberYearsSmoked
               ,NumberCigarettesPerDay
               ,OtherFormTobacco
               ,TakesAlcohol
               ,HIVStatus
               ,FamilyHistoryOfCa
               ,PreviousCaTreatment
               ,SymptomsCa
               ,CancerType
               ,FecalOccultBloodTest
               ,TreatmentOccultBlood
               ,Colonoscopy
               ,TreatmentColonoscopy
               ,EUA
               ,TreatmentRetinoblastoma
               ,RetinoblastomaGene
               ,TreatmentEUA
               ,DRE
               ,TreatmentDRE
               ,PSA
               ,TreatmentPSA
               ,VisualExamination
               ,TreatmentVE
               ,Cytology
               ,TreatmentCytology
               ,Imaging
               ,TreatmentImaging
               ,Biopsy
               ,TreatmentBiopsy
               ,PostTreatmentComplicationCause
               ,OtherPostTreatmentComplication
               ,ReferralReason
               ,ScreeningMethod
               ,TreatmentToday
               ,ReferredOut
               ,NextAppointmentDate
               ,ScreeningType
               ,HPVScreeningResult
               ,TreatmentHPV
               ,VIAScreeningResult
               ,VIAVILIScreeningResult
               ,VIATreatmentOptions
               ,PAPSmearScreeningResult
               ,TreatmentPapSmear
               ,ReferalOrdered
               ,Colposcopy
               ,TreatmentColposcopy
               ,BiopsyCINIIandAbove
               ,BiopsyCINIIandBelow
               ,BiopsyNotAvailable
               ,CBE
               ,TreatmentCBE
               ,Ultrasound
               ,TreatmentUltraSound
               ,IfTissueDiagnosis
               ,DateTissueDiagnosis
               ,ReasonNotDone
               ,FollowUpDate
               ,Referred
               ,ReasonForReferral
               ,CSE.RecordUUID
               ,CSE.Date_Created
               ,CSE.Date_Last_Modified
               ,CSE.Created
FROM [DWAPICentral].[dbo].[CancerScreeningExtract] CSE
    INNER JOIN [DWAPICentral].[dbo].[PatientExtract] P
ON CSE.[PatientId] = P.ID
    INNER JOIN [DWAPICentral].[dbo].[Facility] F
    ON P.[FacilityId] = F.Id
WHERE p.gender != 'Unknown'