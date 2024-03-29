SELECT  ID
     ,[RefId]
     ,[Created]
     ,c.[PatientPk]
     ,c.[SiteCode]
     ,[Emr]
     ,[Project]
     ,[Processed]
     ,[QueueId]
     ,[Status]
     ,[StatusDate]
     ,[DateExtracted]
     ,[FacilityId]
     ,[FacilityName]
     ,[PrepNumber]
     ,[HtsNumber]
     ,[PrepEnrollmentDate]
     ,[Sex]
     ,[DateofBirth]
     ,[CountyofBirth]
     ,[County]
     ,[SubCounty]
     ,[Location]
     ,[LandMark]
     ,[Ward]
     ,[ClientType]
     ,[ReferralPoint]
     ,[MaritalStatus]
     ,[Inschool]
     ,null [PopulationType]
     ,null [KeyPopulationType]
     ,[Refferedfrom]
     ,[TransferIn]
     ,[TransferInDate]
     ,[TransferFromFacility]
     ,[DatefirstinitiatedinPrepCare]
     ,[DateStartedPrEPattransferringfacility]
     ,[ClientPreviouslyonPrep]
     ,[PrevPrepReg]
     ,[DateLastUsedPrev]
     ,[Date_Created]
     ,[Date_Last_Modified]
     ,RecordUUID
FROM [PREPCentral].[dbo].[PrepPatients](NoLock) c
    INNER JOIN
    (SELECT patientPK,sitecode,Max(ID)As MaxID,max(cast(created as date))as Maxcreated from [PREPCentral].[dbo].[PrepPatients](NoLock) group by patientPK,sitecode)tn
on c.patientPK = tn.patientPK
    and c.sitecode =tn.sitecode and cast(c.created as date) = tn.Maxcreated and C.ID = tn.MaxID