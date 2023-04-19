SELECT 	DISTINCT a.[FacilityName]
                 ,a.[SiteCode]
                 ,a.[PatientPk]
                 ,a.[HtsNumber]
                 ,a.[Emr]
                 ,a.DateExtracted
                 ,a.[Project]
                 ,[EnrolledFacilityName]
                 ,CAST (MAX([ReferralDate]) AS DATE) AS [ReferralDate]
                 ,CAST([DateEnrolled] AS DATE) AS [DateEnrolled]
                 ,CAST(MAX([DatePrefferedToBeEnrolled]) AS DATE ) AS [DatePrefferedToBeEnrolled]
                 ,CASE WHEN [FacilityReferredTo]='Other Facility' THEN NULL ELSE [FacilityReferredTo] END AS [FacilityReferredTo]
							  ,[HandedOverTo]
							  ,[HandedOverToCadre]
							  ,[ReportedCCCNumber]
							  ,CASE WHEN CAST([ReportedStartARTDate] AS DATE) = '0001-01-01' THEN NULL ELSE CAST([ReportedStartARTDate] AS DATE) END AS [ReportedStartARTDate]

						FROM [HTSCentral].[dbo].[ClientLinkages](NoLock) a--
						INNER JOIN (
								SELECT distinct SiteCode,PatientPK, MAX(DateExtracted) AS MaxDateExtracted
								FROM  [HTSCentral].[dbo].[ClientLinkages](NoLock)
								GROUP BY SiteCode,PatientPK
							) tm
				     ON a.[SiteCode] = tm.[SiteCode] and a.PatientPK=tm.PatientPK and a.DateExtracted = tm.MaxDateExtracted
						INNER JOIN [HTSCentral].[dbo].Clients (NoLock) Cl
						on a.PatientPk = Cl.PatientPk and a.SiteCode = Cl.SiteCode
						WHERE a.DateExtracted > '2019-09-08'
						GROUP BY a.[FacilityName]
							,a.[SiteCode]
							,a.[PatientPk]
							,a.[HtsNumber]
							,a.[Emr]
							,a.[Project]
							,[EnrolledFacilityName]
							,CAST([DateEnrolled] AS DATE)
							,[FacilityReferredTo]
							,[HandedOverTo]
							,[HandedOverToCadre]
							,a.DateExtracted
							,[ReportedCCCNumber]
							,CAST([ReportedStartARTDate] AS DATE)