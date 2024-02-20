SELECT DISTINCT  a.ID,a.[FacilityName],a.[SiteCode],a.[PatientPk],a.[HtsNumber],a.[Emr],a.[Project],a.[Processed],a.[QueueId],a.[Status]
              ,a.[StatusDate],a.[EncounterId],a.[VisitID],a.[VisitDate],a.[PopulationType],[KeyPopulation],[PriorityPopulation],[Department]
              ,[PatientType],[IsHealthWorker],[RelationshipWithContact],[TestedHIVBefore],[WhoPerformedTest],[ResultOfHIV],[DateTestedSelf]
              ,[StartedOnART],[CCCNumber],[EverHadSex],[SexuallyActive],[NewPartner],[PartnerHIVStatus],a.[CoupleDiscordant],[MultiplePartners]
              ,[NumberOfPartners],[AlcoholSex],[MoneySex],[CondomBurst],[UnknownStatusPartner],[KnownStatusPartner],[Pregnant],[BreastfeedingMother]
              ,[ExperiencedGBV],[ContactWithTBCase],[Lethargy],[EverOnPrep],[CurrentlyOnPrep],[EverOnPep],[CurrentlyOnPep],[EverHadSTI],[CurrentlyHasSTI]
              ,[EverHadTB],[SharedNeedle],[NeedleStickInjuries],[TraditionalProcedures],[ChildReasonsForIneligibility],[EligibleForTest]
              ,[ReasonsForIneligibility],[SpecificReasonForIneligibility],a.[FacilityId],[Cough],[DateTestedProvider],[Fever],[MothersStatus]
              ,[NightSweats],[ReferredForTesting],[ResultOfHIVSelf],[ScreenedTB],[TBStatus],[WeightLoss],[AssessmentOutcome],[ForcedSex]
              ,[ReceivedServices],[TypeGBV]
              ,Disability
              ,a.DisabilityType
              ,HTSStrategy
              ,HTSEntryPoint
              ,HIVRiskCategory
              ,ReasonRefferredForTesting
              ,ReasonNotReffered
              ,[HtsRiskScore]

FROM [HTSCentral].[dbo].[HtsEligibilityExtract] (NoLock)a
    Inner join ( select ct.sitecode,ct.patientPK,ct.encounterID,ct.visitID,max(DateCreated)MaxDateCreated  from [HTSCentral].[dbo].[HtsEligibilityExtract] ct
    group by ct.sitecode,ct.patientPK,ct.encounterID,ct.visitID)tn
on a.sitecode = tn.sitecode and a.patientPK = tn.patientPK
    and a.DateCreated = tn.MaxDateCreated
    and a.encounterID = tn.encounterID
    and a.visitID = tn.visitID

    Inner join ( select ct1.sitecode,ct1.patientPK,ct1.encounterID,ct1.visitID,max(cast(ct1.DateExtracted as date))MaxDateExtracted  from [HTSCentral].[dbo].[HtsEligibilityExtract] ct1
    group by ct1.sitecode,ct1.patientPK,ct1.encounterID,ct1.visitID)tn1
    on a.sitecode = tn1.sitecode and a.patientPK = tn1.patientPK
    and cast(a.DateExtracted as date) = tn1.MaxDateExtracted
    and a.encounterID = tn1.encounterID
    and a.visitID = tn1.visitID

    INNER JOIN [HTSCentral].[dbo].Clients (NoLock) Cl
    on a.PatientPk = Cl.PatientPk and a.SiteCode = Cl.SiteCode