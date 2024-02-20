SELECT distinct   P.[PatientCccNumber] AS PatientID, P.[PatientPID] AS PatientPK,F.[Name] AS FacilityName, F.Code AS SiteCode,PV.[VisitId] VisitID,PV.[VisitDate] VisitDate
              ,PV.[Service] [SERVICE],PV.[VisitType] VisitType,PV.[WHOStage] WHOStage,PV.[WABStage] WABStage,PV.[Pregnant] Pregnant,PV.[LMP] LMP,PV.[EDD] EDD,PV.[Height] [Height],PV.[Weight] [Weight],PV.[BP] [BP],PV.[OI] [OI],PV.[OIDate] [OIDate]
              ,PV.[SubstitutionFirstlineRegimenDate] SubstitutionFirstlineRegimenDate,PV.[SubstitutionFirstlineRegimenReason] SubstitutionFirstlineRegimenReason,PV.[SubstitutionSecondlineRegimenDate] SubstitutionSecondlineRegimenDate,PV.[SubstitutionSecondlineRegimenReason] SubstitutionSecondlineRegimenReason
              ,PV.[SecondlineRegimenChangeDate] SecondlineRegimenChangeDate,PV.[SecondlineRegimenChangeReason] SecondlineRegimenChangeReason,PV.[Adherence] Adherence,PV.[AdherenceCategory] AdherenceCategory,PV.[FamilyPlanningMethod] FamilyPlanningMethod
              ,PV.[PwP] PwP,PV.[GestationAge] GestationAge,PV.[NextAppointmentDate] NextAppointmentDate,P.[Emr] Emr
              ,CASE P.[Project]
                   WHEN 'I-TECH' THEN 'Kenya HMIS II'
                   WHEN 'HMIS' THEN 'Kenya HMIS II'
                   ELSE P.[Project]
    END AS [Project]
						  ,PV.[Voided] Voided,pv.[StabilityAssessment] StabilityAssessment,pv.[DifferentiatedCare] DifferentiatedCare,pv.[PopulationType] PopulationType,pv.[KeyPopulationType] KeyPopulationType,PV.[Processed] Processed
						  ,PV.[Created] Created
						 ,[GeneralExamination],[SystemExamination],[Skin],[Eyes],[ENT],[Chest],[CVS],[Abdomen],[CNS],[Genitourinary]
							-----Missing columns Added later by Dennis
						  ,PV.VisitBy VisitBy,PV.Temp Temp,PV.PulseRate PulseRate,PV.RespiratoryRate RespiratoryRate,PV.OxygenSaturation OxygenSaturation,PV.Muac Muac,PV.NutritionalStatus NutritionalStatus,PV.EverHadMenses EverHadMenses,PV.Menopausal Menopausal
						  ,PV.Breastfeeding Breastfeeding,PV.NoFPReason NoFPReason,PV.ProphylaxisUsed ProphylaxisUsed,PV.CTXAdherence CTXAdherence,PV.CurrentRegimen CurrentRegimen,PV.HCWConcern HCWConcern,PV.TCAReason TCAReason,PV.ClinicalNotes ClinicalNotes
						  ,P.ID as PatientUnique_ID
						  ,PV.PatientId as UniquePatientVisitId
						  ,PV.ID as PatientVisitUnique_ID
						  ,[ZScore]
							,[ZScoreAbsolute]
							,RefillDate
							,PaedsDisclosure,PV.[Date_Created],PV.[Date_Last_Modified]
							,PV.RecordUUID
FROM [DWAPICentral].[dbo].[PatientExtract] P WITH (NoLock)
    INNER JOIN [DWAPICentral].[dbo].[PatientVisitExtract] PV WITH(NoLock)  ON PV.[PatientId]= P.ID
    INNER JOIN [DWAPICentral].[dbo].[Facility] F WITH(NoLock)  ON P.[FacilityId] = F.Id AND F.Voided=0
    INNER JOIN (
    SELECT F.code as SiteCode,p.[PatientPID] as PatientPK,[VisitId],visitDate,InnerPV.voided,max(InnerPV.ID) maxID, MAX(InnerPV.created) AS Maxdatecreated
    FROM [DWAPICentral].[dbo].[PatientExtract] P WITH (NoLock)
    INNER JOIN [DWAPICentral].[dbo].[PatientVisitExtract] InnerPV WITH(NoLock)  ON InnerPV.[PatientId]= P.ID
    INNER JOIN [DWAPICentral].[dbo].[Facility] F WITH(NoLock)  ON P.[FacilityId] = F.Id AND F.Voided=0
    GROUP BY F.code,p.[PatientPID],[VisitId],visitDate,InnerPV.voided
    ) tm
    ON f.code = tm.[SiteCode] and p.PatientPID=tm.PatientPK and
    pv.[VisitId] = tm.[VisitId] and pv.visitDate = tm.visitDate and pv.voided = tm.voided and
    pv.created = tm.Maxdatecreated and
    PV.ID =tm. maxID
WHERE p.gender!='Unknown' AND F.code >0