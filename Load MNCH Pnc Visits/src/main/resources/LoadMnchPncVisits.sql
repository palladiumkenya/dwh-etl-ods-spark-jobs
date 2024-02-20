SELECT distinct P.[PatientMnchID],P.[PatientPk],[PNCRegisterNumber],P.[SiteCode],P.[EMR],F.Name FacilityName,P.[Project]
        ,cast(P.[DateExtracted] as date)[DateExtracted],p.[VisitID],cast(p.[VisitDate] as date)[VisitDate] ,[PNCVisitNo]
        ,cast([DeliveryDate] as date)[DeliveryDate],[ModeOfDelivery],[PlaceOfDelivery],[Height],[Weight],[Temp]
        ,[PulseRate],[RespiratoryRate],[OxygenSaturation],[MUAC],[BP],[BreastExam],[GeneralCondition],[HasPallor]
        ,[Pallor],[Breast],[PPH],[CSScar],[UterusInvolution],[Episiotomy],[Lochia],[Fistula],[MaternalComplications]
        ,[TBScreening],[ClientScreenedCACx],[CACxScreenMethod],[CACxScreenResults],[PriorHIVStatus],[HIVTestingDone]
        ,[HIVTest1],[HIVTest1Result],[HIVTest2],[HIVTest2Result],[HIVTestFinalResult],[InfantProphylaxisGiven],[MotherProphylaxisGiven]
        ,[CoupleCounselled],[PartnerHIVTestingPNC],[PartnerHIVResultPNC],[CounselledOnFP],[ReceivedFP],[HaematinicsGiven]
        ,[DeliveryOutcome],[BabyConditon],[BabyFeeding],[UmbilicalCord],[Immunization],[InfantFeeding],[PreventiveServices]
        ,[ReferredFrom],[ReferredTo],cast([NextAppointmentPNC] as date)[NextAppointmentPNC],[ClinicalNotes]
        ,P.[Date_Last_Modified]
        ,[InfactCameForHAART]
        ,[MotherCameForHIVTest]
        ,[MotherGivenHAART]
        ,[VisitTimingBaby]
        ,[VisitTimingMother]
        ,RecordUUID
FROM [MNCHCentral].[dbo].[PncVisits] P (nolock)
    inner join (select tn.SiteCode,tn.PatientPK,tn.VisitDate,tn.visitID,Max(ID) As MaxID,max(cast(tn.DateExtracted as date))MaxDateExtracted
    FROM [MNCHCentral].[dbo].[PncVisits] (NoLock)tn
    group by tn.SiteCode,tn.PatientPK,tn.VisitDate,tn.visitID)tm
on  p.SiteCode = tm.SiteCode and P.PatientPk = tm.PatientPk and p.VisitDate = tm.VisitDate and p.VisitID = tm.VisitID and   cast(p.DateExtracted as Date) = tm.MaxDateExtracted
    and p.ID = tm.MaxID
    INNER JOIN [MNCHCentral].[dbo].[Facilities] F ON P.[FacilityId] = F.Id