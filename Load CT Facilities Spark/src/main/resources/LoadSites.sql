SELECT DISTINCT MFL_Code, Facility_Name,County,SubCounty,[Owner],Latitude,Longitude,SDP,
    SDP_Agency,Implementation,EMR, EMR_Status,HTS_Use,
    HTS_Deployment, HTS_Status,null as [IL_Status], null as Registration_IE,
    null as Phamarmacy_IE,mlab,Ushauri,Nishauri,null as Appointment_Management_IE,
    OVC,OTZ,PrEP,null as [_3PM],AIR,KP,MCH,null as TB, Lab_Manifest,Comments,Project
FROM [HIS_Implementation].[dbo].[All_EMRSites] WHERE MFL_Code !=''