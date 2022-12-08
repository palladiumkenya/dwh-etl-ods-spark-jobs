truncate table ODS.dbo.HistoricalARTOutcomesBaseTable;

declare
@start_date date = '2017-01-31',
@end_date date = '2022-09-30';

with dates as (
    select datefromparts(year(@start_date), month(@start_date), 1) as dte
    union all
    select dateadd(month, 1, dte)
    from dates
    where dateadd(month, 1, dte) <= @end_date
)
select
    eomonth(dte) as end_date
into #months
from dates
    option (maxrecursion 0);




--declare as of date
declare @as_of_date as date;

--declare cursor
declare cursor_AsOfDates cursor for
select * from #months

    open cursor_AsOfDates

fetch next from cursor_AsOfDates into @as_of_date
    while @@FETCH_STATUS = 0

begin

with clinical_visits_as_of_date as (
    /* get visits as of date */
    select
        PatientPK,
        PatientID,
        SiteCode,
        VisitDate,
        NextAppointmentDate,
        PopulationType,
        KeyPopulationType,
        CurrentRegimen,
        Emr
    from  ODS.dbo.CT_PatientVisits
    where SiteCode > 0 and VisitDate <= @as_of_date
),
     pharmacy_visits_as_of_date as (
         /* get pharmacy dispensations as of date */
         select
             PatientPK,
             PatientID,
             SiteCode,
             DispenseDate,
             ExpectedReturn,
             Emr
         from ODS.dbo.CT_PatientPharmacy
         where SiteCode > 0 and DispenseDate <= @as_of_date
     ),
     patient_art_and_enrollment_info as (
         /* get patients' ART start date */
         select
             distinct CT_ARTPatients.PatientID,
                      CT_ARTPatients.PatientPK,
                      CT_ARTPatients.SiteCode,
                      CT_ARTPatients.StartARTDate,
                      CT_ARTPatients.StartRegimen,
                      CT_ARTPatients.StartRegimenLine,
                      CT_Patient.RegistrationAtCCC as EnrollmentDate,
                      CT_Patient.DOB,
                      CT_Patient.Gender,
                      CT_Patient.DateConfirmedHIVPositive,
                      datediff(yy, CT_Patient.DOB, CT_Patient.RegistrationAtCCC) as AgeEnrollment
         from ODS.dbo.CT_ARTPatients
                  left join ODS.dbo.CT_Patient  on CT_Patient.PatientID = CT_ARTPatients.PatientID
             and CT_Patient.PatientPK = CT_ARTPatients.PatientPK
             and CT_Patient.SiteCode = CT_ARTPatients.SiteCode
     ),
     visit_encounter_as_of_date_ordering as (
         /* order visits as of date by the VisitDate */
         select
             clinical_visits_as_of_date.*,
             row_number() over (partition by PatientPK, PatientID, SiteCode order by VisitDate desc) as rank
         from clinical_visits_as_of_date
     ),
     pharmacy_dispense_as_of_date_ordering as (
         /* order pharmacy dispensations as of date by the VisitDate */
         select
             pharmacy_visits_as_of_date.*,
             row_number() over (partition by PatientPK, PatientID, SiteCode order by DispenseDate desc) as rank
         from pharmacy_visits_as_of_date
     ),
     last_visit_encounter_as_of_date as (
         /*get the latest visit record for patients for as of date */
         select
             *
         from visit_encounter_as_of_date_ordering
         where rank = 1
     ),
     last_pharmacy_dispense_as_of_date as (
         /*get the latest pharmacy dispensations record for patients for as of date */
         select
             *
         from pharmacy_dispense_as_of_date_ordering
         where rank = 1
     ),
     effective_discontinuation_ordering as (
         /*order the effective discontinuation by the EffectiveDiscontinuationDate*/
         select
             PatientID,
             PatientPK,
             SiteCode,
             EffectiveDiscontinuationDate,
             ExitDate,
             ExitReason,
             row_number() over (partition by PatientPK, PatientID, SiteCode order by EffectiveDiscontinuationDate desc) as rank
         from ODS.dbo.CT_PatientStatus
         where ExitDate is not null and EffectiveDiscontinuationDate is not null
     ),
     latest_effective_discontinuation as (
         /*get the latest discontinuation record*/
         select
             *
         from effective_discontinuation_ordering
         where rank = 1
     ),
     exits_as_of_date as (
         /* get exits as of date */
         select
             PatientID,
             PatientPK,
             SiteCode,
             ExitDate,
             ExitReason
         from ODS.dbo.CT_PatientStatus
         where ExitDate <= @as_of_date
     ),
     exits_as_of_date_ordering as (
         /* order the exits by the ExitDate*/
         select
             PatientID,
             PatientPK,
             SiteCode,
             ExitDate,
             ExitReason,
             row_number() over (partition by PatientPK, PatientID, SiteCode order by ExitDate desc) as rank
         from exits_as_of_date
     ),
     last_exit_as_of_date as (
         /* get latest exit_date as of date */
         select
             *
         from exits_as_of_date_ordering
         where rank = 1
     ),
     visits_and_dispense_encounters_combined_tbl as (
         /* combine latest visits and latest pharmacy dispensation records as of date - 'borrowed logic from the view vw_PatientLastEnconter*/
         /* we don't include the stg_ARTPatients table logic because this table has only the latest records of the patients (no history) */
         select  distinct coalesce (last_visit.PatientID, last_dispense.PatientID) as PatientID,
                          coalesce(last_visit.SiteCode, last_dispense.SiteCode) as SiteCode,
                          coalesce(last_visit.PatientPK, last_dispense.PatientPK) as PatientPK ,
                          coalesce(last_visit.Emr, last_dispense.Emr) as Emr,
                          case
                              when last_visit.VisitDate >= last_dispense.DispenseDate then last_visit.VisitDate
                              else isnull(last_dispense.DispenseDate, last_visit.VisitDate)
                              end as LastEncounterDate,
                          case
                              when last_visit.NextAppointmentDate >= last_dispense.ExpectedReturn then last_visit.NextAppointmentDate
                              else isnull(last_dispense.ExpectedReturn, last_visit.NextAppointmentDate)
                              end as NextAppointmentDate
         from last_visit_encounter_as_of_date as last_visit
                  full join last_pharmacy_dispense_as_of_date as last_dispense on last_visit.PatientID = last_dispense.PatientID
             and last_visit.SiteCode = last_dispense.SiteCode
             and last_visit.PatientPK = last_dispense.PatientPK
         where
             case
                 when last_visit.VisitDate >= last_dispense.DispenseDate then last_visit.VisitDate
                 else isnull(last_dispense.DispenseDate, last_visit.VisitDate)
                 end is not null
     ),
     last_encounter as (
         /* preparing the latest encounter records as of date */
         select
             visits_and_dispense_encounters_combined_tbl.PatientID,
             visits_and_dispense_encounters_combined_tbl.SiteCode,
             visits_and_dispense_encounters_combined_tbl.PatientPK,
             visits_and_dispense_encounters_combined_tbl.Emr,
             visits_and_dispense_encounters_combined_tbl.LastEncounterDate,
             case
                 when datediff(dd, @as_of_date, visits_and_dispense_encounters_combined_tbl.NextAppointmentDate) >= 365 then dateadd(day, 30, LastEncounterDate)
                 else visits_and_dispense_encounters_combined_tbl.NextAppointmentDate
                 end As NextAppointmentDate
         from visits_and_dispense_encounters_combined_tbl
     ),
     ARTOutcomesCompuation as (
         /* computing the ART_Outcome as of date - 'borrowed logic from the view vw_ARTOutcomeX'*/
         select
             last_encounter.*,
             patient_art_and_enrollment_info.startARTDate,
             last_exit_as_of_date.ExitDate,
             patient_art_and_enrollment_info.EnrollmentDate,
             patient_art_and_enrollment_info.AgeEnrollment,
             patient_art_and_enrollment_info.StartRegimen,
             patient_art_and_enrollment_info.StartRegimenLine,
             patient_art_and_enrollment_info.DateConfirmedHIVPositive,
             patient_art_and_enrollment_info.Gender,
             datediff(year, patient_art_and_enrollment_info.DOB, last_encounter.LastEncounterDate) as AgeLastVisit,
             case
                 when latest_effective_discontinuation.ExitDate is not null
                     and latest_effective_discontinuation.ExitReason <> 'DIED'
                     and latest_effective_discontinuation.EffectiveDiscontinuationDate > eomonth(@as_of_date) then 'V'
                 when patient_art_and_enrollment_info.startARTDate > dateadd(s,-1,dateadd(mm, datediff(m,0, @as_of_date) + 1 ,0)) then 'NP'
                 when last_exit_as_of_date.ExitDate is not null then substring(last_exit_as_of_date.ExitReason, 1, 1)
                 when eomonth(@as_of_date) < last_encounter.NextAppointmentDate
                     or datediff(dd, last_encounter.NextAppointmentDate, eomonth(@as_of_date)) <= 30 then 'V'
                 when datediff(dd, last_encounter.NextAppointmentDate, eomonth(@as_of_date)) > 30 then 'uL'
                 when NextAppointmentDate is null then 'NV'
                 end as ARTOutcome,
             @as_of_date as AsOfDate
         from last_encounter
                  left join latest_effective_discontinuation on latest_effective_discontinuation.PatientID = last_encounter.PatientID
             and latest_effective_discontinuation.PatientPK = last_encounter.PatientPK
             and latest_effective_discontinuation.SiteCode = last_encounter.SiteCode
                  left join last_exit_as_of_date on last_exit_as_of_date.PatientID = last_encounter.PatientID
             and last_exit_as_of_date.PatientPK = last_encounter.PatientPK
             and last_exit_as_of_date.SiteCode = last_encounter.SiteCode
                  left join patient_art_and_enrollment_info on patient_art_and_enrollment_info.PatientID = last_encounter.PatientID
             and patient_art_and_enrollment_info.PatientPK = last_encounter.PatientPK
             and patient_art_and_enrollment_info.SiteCode = last_encounter.SiteCode
     )
insert into dbo.HistoricalARTOutcomesBaseTable
select
    ARTOutcomesCompuation.PatientID as PatientID,
    ARTOutcomesCompuation.PatientPK,
    ARTOutcomesCompuation.SiteCode as MFLCode,
    ARTOutcomesCompuation.ARTOutcome,
    ARTOutcomesCompuation.AsOfDate
from ARTOutcomesCompuation

    fetch next from cursor_AsOfDates into @as_of_date
end


--free up objects
drop table #months
    close cursor_AsOfDates
    deallocate cursor_AsOfDates