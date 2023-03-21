SELECT DISTINCT MFL_Code,[Facility Name] as Facility_Name,County,SubCounty,[Owner],Latitude,Longitude,SDP,
  [SDP Agency] as SDP_Agency,Implementation,EMR,[EMR Status] as EMR_Status,[HTS Use] as HTS_Use,
  [HTS Deployment] as HTS_Deployment,[HTS Status] as HTS_Status,[IL Status] as IL_Status,[Registration IE] as Registration_IE,
  [Phamarmacy IE] as Phamarmacy_IE,mlab,Ushauri,Nishauri,[Appointment Management IE] as Appointment_Management_IE,
  OVC,OTZ,PrEP,[3PM],AIR,KP,MCH,TB,[Lab Manifest] as Lab_Manifest,Comments,Project
FROM [HIS_Implementation].[dbo].[All_EMRSites] WHERE MFL_Code !=''