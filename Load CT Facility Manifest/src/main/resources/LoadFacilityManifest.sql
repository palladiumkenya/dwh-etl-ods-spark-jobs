SELECT DISTINCT
    ID,Emr,Project,Voided,Processed,SiteCode,PatientCount,DateRecieved,[Name],EmrName,EmrSetup,UploadMode,[Start],[End],Tag
FROM [DWAPICentral].[dbo].[FacilityManifest](NoLock)