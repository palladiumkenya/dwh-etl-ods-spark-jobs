with cleaned_up_dates as (
    select
        TestKitExpiry1,
        try_cast(TestKitExpiry1 as datetime) as TestKitExpiryCleanedVersion1
    from ODS.dbo.HTS_TestKits
),
dd_mm_yyyy_data as (
    select
        TestKitExpiry1,
        convert(datetime, TestKitExpiry1, 103) as TestKitExpiryCleanedVersion2
    from cleaned_up_dates
    where TestKitExpiryCleanedVersion1 is null
        and TestKitExpiry1 is not null
        and len(substring(TestKitExpiry1, 7, 4)) = 4
),
combined_dates as (
    select TestKitExpiry1, TestKitExpiryCleanedVersion1 as TestKitExpiry1Cleaned from cleaned_up_dates
    union
    select TestKitExpiry1, TestKitExpiryCleanedVersion2 as TestKitExpiry1Cleaned from dd_mm_yyyy_data
)
update ODS.dbo.HTS_TestKits
set TestKitExpiry1 =  combined_dates.TestKitExpiry1Cleaned
    from ODS.dbo.HTS_TestKits as kits
inner join combined_dates on combined_dates.TestKitExpiry1 = kits.TestKitExpiry1;

-- clean TestKitExpiry2
with cleaned_up_dates as (
    select
        TestKitExpiry2,
        try_cast(TestKitExpiry2 as datetime) as TestKitExpiryCleanedVersion1
    from ODS.dbo.HTS_TestKits
),
     dd_mm_yyyy_data as (
         select
             TestKitExpiry2,
             convert(datetime, TestKitExpiry2, 103) as TestKitExpiryCleanedVersion2
         from cleaned_up_dates
         where TestKitExpiryCleanedVersion1 is null
           and TestKitExpiry2 is not null
           and len(substring(TestKitExpiry2, 7, 4)) = 4
     ),
     combined_dates as (
         select TestKitExpiry2, TestKitExpiryCleanedVersion1 as TestKitExpiry2Cleaned from cleaned_up_dates
         union
         select TestKitExpiry2, TestKitExpiryCleanedVersion2 as TestKitExpiry2Cleaned from dd_mm_yyyy_data
     )
update ODS.dbo.HTS_TestKits
set TestKitExpiry2 =  combined_dates.TestKitExpiry2Cleaned
    from ODS.dbo.HTS_TestKits as kits
inner join combined_dates on combined_dates.TestKitExpiry2 = kits.TestKitExpiry2