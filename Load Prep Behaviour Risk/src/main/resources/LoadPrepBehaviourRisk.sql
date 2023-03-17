SELECT distinct
    A.ID
              ,a.[RefId]
              ,a.[Created]
              ,a.[PatientPk]
              ,a.[SiteCode]
              ,a.[Emr]
              ,a.[Project]
              ,a.[Processed]
              ,a.[QueueId]
              ,a.[Status]
              ,a.[StatusDate]
              ,a.[DateExtracted]
              ,a.[FacilityId]
              ,a.[FacilityName]
              ,a.[PrepNumber]
              ,a.[HtsNumber]
              ,[VisitDate]
              ,[VisitID]
              ,[SexPartnerHIVStatus]
              ,[IsHIVPositivePartnerCurrentonART]
              ,[IsPartnerHighrisk]
              ,[PartnerARTRisk]
              ,[ClientAssessments]
              ,[ClientRisk]
              ,[ClientWillingToTakePrep]
              ,[PrEPDeclineReason]
              ,[RiskReductionEducationOffered]
              ,[ReferralToOtherPrevServices]
              ,[FirstEstablishPartnerStatus]
              ,[PartnerEnrolledtoCCC]
              ,[HIVPartnerCCCnumber]
              ,[HIVPartnerARTStartDate]
              ,[MonthsknownHIVSerodiscordant]
              ,[SexWithoutCondom]
              ,[NumberofchildrenWithPartner]
              ,a.[Date_Created]
              ,a.[Date_Last_Modified]

FROM [PREPCentral].[dbo].[PrepBehaviourRisks](NoLock)a
    inner join    [PREPCentral].[dbo].[PrepPatients](NoLock) b

on a.SiteCode = b.SiteCode and a.PatientPk =  b.PatientPk

    INNER JOIN (SELECT PatientPk, SiteCode, max(Created) AS maxCreated from [PREPCentral].[dbo].[PrepBehaviourRisks]
    group by PatientPk,SiteCode) tn
    ON a.PatientPk = tn.PatientPk and a.SiteCode = tn.SiteCode and a.Created = tn.maxCreated

    INNER JOIN (SELECT PatientPk, SiteCode, max(DateExtracted) AS maxDateExtracted from [PREPCentral].[dbo].[PrepBehaviourRisks]
    group by PatientPk,SiteCode) tm
    ON a.PatientPk = tm.PatientPk and a.SiteCode = tm.SiteCode and a.DateExtracted = tm.maxDateExtracted