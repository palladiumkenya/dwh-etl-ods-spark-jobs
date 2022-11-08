SELECT
    P.[Id],P.[PatientCccNumber] AS PatientID,P.[PatientPID] AS PatientPK,F.[Code] AS SiteCode,F.[Name] AS FacilityName,P.[Gender],P.[DOB],P.[RegistrationDate],P.[RegistrationAtCCC],P.[RegistrationATPMTCT]
     ,P.[RegistrationAtTBClinic],P.[PatientSource],P.[Region],P.[District],ISNULL(P.[Village],'') AS [Village],P.[ContactRelation],P.[LastVisit],P.[MaritalStatus]
						  ,P.[EducationLevel],P.[DateConfirmedHIVPositive],P.[PreviousARTExposure],P.[PreviousARTStartDate],P.[Emr]
						  ,CASE P.[Project]
								WHEN 'I-TECH' THEN 'Kenya HMIS II'
								WHEN 'HMIS' THEN 'Kenya HMIS II'
						   ELSE P.[Project]
END AS [Project]
						   ,GETDATE() AS DateImported
						   ,LTRIM(RTRIM(STR(F.Code)))+'-'+LTRIM(RTRIM(P.[PatientCccNumber])) +'-'+LTRIM(RTRIM(STR(P.[PatientPID]))) AS CKV
						  ,P.[Processed],P.[StatusAtCCC],P.[StatusAtPMTCT],P.[StatusAtTBClinic],P.[Created],
						P.Orphan,P.Inschool,P.PatientType,P.PopulationType,P.KeyPopulationType,P.PatientResidentCounty,P.PatientResidentSubCounty,
						P.PatientResidentLocation,P.PatientResidentSubLocation,P.PatientResidentWard,P.PatientResidentVillage,P.TransferInDate,
						P.Occupation,P.NUPI
					  FROM [DWAPICentral].[dbo].[PatientExtract](NoLock) P
					INNER JOIN [DWAPICentral].[dbo].[Facility](NoLock) F ON P.[FacilityId] = F.Id AND F.Voided=0
					WHERE P.Voided=0 and P.[Gender] is NOT NULL and p.gender!='Unknown'