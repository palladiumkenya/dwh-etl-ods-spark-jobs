SELECT distinct P.[PatientMnchID],P.[PatientPk],[PNCRegisterNumber],P.[SiteCode],P.[EMR],F.Name FacilityName,P.[Project]
        ,cast(P.[DateExtracted] as date)[DateExtracted],[VisitID],cast([VisitDate] as date)[VisitDate] ,[PNCVisitNo]
        ,cast([DeliveryDate] as date)[DeliveryDate],[ModeOfDelivery],[PlaceOfDelivery],[Height],[Weight],[Temp]
        ,[PulseRate],[RespiratoryRate],[OxygenSaturation],[MUAC],[BP],[BreastExam],[GeneralCondition],[HasPallor]
        ,[Pallor],[Breast],[PPH],[CSScar],[UterusInvolution],[Episiotomy],[Lochia],[Fistula],[MaternalComplications]
        ,[TBScreening],[ClientScreenedCACx],[CACxScreenMethod],[CACxScreenResults],[PriorHIVStatus],[HIVTestingDone]
        ,[HIVTest1],[HIVTest1Result],[HIVTest2],[HIVTest2Result],[HIVTestFinalResult],[InfantProphylaxisGiven],[MotherProphylaxisGiven]
        ,[CoupleCounselled],[PartnerHIVTestingPNC],[PartnerHIVResultPNC],[CounselledOnFP],[ReceivedFP],[HaematinicsGiven]
        ,[DeliveryOutcome],[BabyConditon],[BabyFeeding],[UmbilicalCord],[Immunization],[InfantFeeding],[PreventiveServices]
        ,[ReferredFrom],[ReferredTo],cast([NextAppointmentPNC] as date)[NextAppointmentPNC],[ClinicalNotes]
        ,P.[Date_Last_Modified]

FROM [MNCHCentral].[dbo].[PncVisits] P (nolock)
    inner join (select tn.PatientPK,tn.SiteCode,max(tn.DateExtracted)MaxDateExtracted FROM [MNCHCentral].[dbo].[PncVisits] (NoLock)tn
    group by tn.PatientPK,tn.SiteCode)tm
on P.PatientPk = tm.PatientPk and p.SiteCode = tm.SiteCode and p.DateExtracted = tm.MaxDateExtracted
    INNER JOIN  [MNCHCentral].[dbo].[MnchPatients] MnchP(Nolock)
    on P.patientPK = MnchP.patientPK and P.Sitecode = MnchP.Sitecode
    INNER JOIN [MNCHCentral].[dbo].[Facilities] F ON P.[FacilityId] = F.Id