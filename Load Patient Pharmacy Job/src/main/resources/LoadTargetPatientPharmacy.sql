
SELECT
    PatientID,
    PatientPK,
    FacilityName,
    SiteCode,
    VisitID,
    Drug,
    DispenseDate,
    Duration,
    ExpectedReturn,
    TreatmentType,
    PeriodTaken,
    ProphylaxisType,
    Emr,
    Project,
--     DateImported,
    RegimenLine,
    RegimenChangedSwitched,
    RegimenChangeSwitchReason,
    StopRegimenReason,
    StopRegimenDate,
    CKV,
    PatientUnique_ID,
    PatientPharmacyUnique_ID
FROM
    [ODS].[dbo].CT_PatientPharmacy
