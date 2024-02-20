SELECT  DISTINCT P.ID,P.[PatientCccNumber] as PatientID,P.[PatientPID] as PatientPK,F.Code as SiteCode,F.[Name] as FacilityName,Gender,DOB,RegistrationDate,RegistrationAtCCC
               ,RegistrationAtPMTCT,RegistrationAtTBClinic,PatientSource,Region,District,Village
               ,ContactRelation,LastVisit,MaritalStatus,EducationLevel,DateConfirmedHIVPositive,PreviousARTExposure,PreviousARTStartDate,P.Emr,P.Project,Orphan,Inschool,PatientType,null PopulationType,null KeyPopulationType,PatientResidentCounty,
    PatientResidentSubCounty,PatientResidentLocation,PatientResidentSubLocation,PatientResidentWard,PatientResidentVillage,TransferInDate,Occupation,NUPI
               ,Pkv,P.[Date_Created],P.[Date_Last_Modified]
               ,P.RecordUUID,P.voided
FROM [DWAPICentral].[dbo].[PatientExtract]  P  with (NoLock)
    INNER JOIN [DWAPICentral].[dbo].[Facility] F with (NoLock)
ON P.[FacilityId]  = F.Id  AND F.Voided=0
    INNER JOIN (SELECT P.PatientPID,F.code,max(p.ID) As Max_ID,Max(cast(P.created as date))MaxCreated FROM [DWAPICentral].[dbo].[PatientExtract]  P  with (NoLock)
    INNER JOIN [DWAPICentral].[dbo].[Facility] F with (NoLock)
    ON P.[FacilityId]  = F.Id
    GROUP BY  P.PatientPID,F.code)tn
    on P.PatientPID = tn.PatientPID and
    F.code = tn.code and
    cast(P.Created as date) = tn.MaxCreated
    and P.ID = tn.Max_ID
WHERE  P.[Gender] is NOT NULL and p.gender!='Unknown' AND F.code >0