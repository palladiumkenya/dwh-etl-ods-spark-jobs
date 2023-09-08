SELECT DISTINCT a.ID,a.[FacilityName]
              ,a.[SiteCode]
              ,a.[PatientPk]
              ,a.[HtsNumber]
              ,a.[Emr]
              ,a.[Project]
              ,[PartnerPatientPk]
              ,a.[KnowledgeOfHivStatus]
              ,[PartnerPersonID]
              ,[CccNumber]
              ,[IpvScreeningOutcome]
              ,[ScreenedForIpv]
              ,[PnsConsent]
              ,a.[RelationsipToIndexClient]
              ,[LinkedToCare]
              ,a.[MaritalStatus]
              ,[PnsApproach]
              ,[FacilityLinkedTo]
              ,LEFT([Sex], 1) AS Gender
              ,[CurrentlyLivingWithIndexClient]
              ,[Age]
              ,[DateElicited]
              ,a.[Dob]
              ,[LinkDateLinkedToCare]
              ,a.Dateextracted
FROM [HTSCentral].[dbo].[HtsPartnerNotificationServices](NoLock) a
    --inner join ( select n.SiteCode,n.PatientPk,n.HtsNumber,n.KnowledgeOfHivStatus,n.RelationsipToIndexClient,Max(n.DateExtracted)MaxDateExtracted FROM [HTSCentral].[dbo].[HtsPartnerNotificationServices](NoLock)n
    --		group by n.SiteCode,n.PatientPk,n.HtsNumber,n.KnowledgeOfHivStatus,n.RelationsipToIndexClient)tn
    --on a.siteCode = tn.SiteCode and a.patientPK =tn.patientPK and  a.KnowledgeOfHivStatus=tn.KnowledgeOfHivStatus and a.RelationsipToIndexClient = tn.RelationsipToIndexClient and a.DateExtracted=tn.MaxDateExtracted
    INNER JOIN [HTSCentral].[dbo].Clients (NoLock) Cl
on a.PatientPk = Cl.PatientPk and a.SiteCode = Cl.SiteCode