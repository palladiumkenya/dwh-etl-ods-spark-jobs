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
              ,a.RecordUUID
FROM [HTSCentral].[dbo].[HtsPartnerNotificationServices](NoLock) a
    INNER JOIN [HTSCentral].[dbo].Clients (NoLock) Cl
on a.PatientPk = Cl.PatientPk and a.SiteCode = Cl.SiteCode