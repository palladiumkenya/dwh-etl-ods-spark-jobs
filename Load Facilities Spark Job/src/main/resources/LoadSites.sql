SELECT mfl_interface_db.facilities_facility_info.mfl_code                              as `MFL_Code`,
       mfl_interface_db.facilities_facility_info.name                                  As `Facility Name`,
--     mfl_interface_db.facilities_facility_info.date_modified                         as `DateModified`,
       mfl_interface_db.facilities_facility_info.date_added                            as `DateCreated`,
       mfl_interface_db.facilities_counties.name                                       As County,
       mfl_interface_db.facilities_sub_counties.name                                   As SubCounty,
       facilities_owner.name                                                           As Owner,
       lat                                                                             as Latitude,
       lon                                                                             as Longitude,
       mfl_interface_db.facilities_partners.name                                       As SDP,
       mfl_interface_db.facilities_sdp_agencies.name                                   As `SDP Agency`,
       (case
            when mfl_interface_db.facilities_implementation_type.ct = 1 then 'CT'
            when mfl_interface_db.facilities_implementation_type.ct = 0 then '' end) AS Implementation,
       mfl_interface_db.facilities_emr_type.type                                       As EMR,
       facilities_emr_info.status                                                      As `EMR Status`,
       facilities_hts_use_type.hts_use_name                                            As `HTS Use`,
       facilities_hts_deployment_type.deployment                                       As `HTS Deployment`,
       facilities_hts_info.status                                                      As `HTS Status`,
       facilities_il_info.status as 'IL Status',
       facilities_il_info.webADT_registration as 'Registration IE',
       facilities_il_info.webADT_pharmacy as 'Pharmacy IE',
       facilities_il_info.Mlab                                                         as `mlab`,
       facilities_il_info.Ushauri,
       facilities_mhealth_info.Nishauri,
       ''                                                                            AS `Appointment Management IE`,
       facilities_emr_info.ovc                                                         as `OVC`,
       facilities_emr_info.otz                                                         as `OTZ`,
       facilities_emr_info.prep                                                        as `PrEP`,
       facilities_il_info.three_PM                                                     as `3PM`,
       facilities_il_info.air                                                          as `AIR`,
       facilities_implementation_type.kp                                               as `KP`,
       facilities_emr_info.mnch                                                        as `MCH`,
       facilities_emr_info.tb                                                          as `TB`,
       facilities_emr_info.lab_manifest                                                as `Lab Manifest`,
       ''                                                                            as Comments,
       ''                                                                            as Project
FROM mfl_interface_db.facilities_facility_info
         LEFT OUTER JOIN mfl_interface_db.facilities_owner
                         ON mfl_interface_db.facilities_owner.id = mfl_interface_db.facilities_facility_info.owner_id
         LEFT OUTER JOIN mfl_interface_db.facilities_counties
                         ON mfl_interface_db.facilities_counties.id = facilities_facility_info.county_id
         LEFT OUTER JOIN mfl_interface_db.facilities_sub_counties
                         ON mfl_interface_db.facilities_sub_counties.id = facilities_facility_info.sub_county_id
         LEFT OUTER JOIN mfl_interface_db.facilities_partners
                         ON mfl_interface_db.facilities_partners.id = facilities_facility_info.partner_id
         LEFT OUTER JOIN mfl_interface_db.facilities_emr_info
                         ON mfl_interface_db.facilities_facility_info.id = facilities_emr_info.id
         LEFT OUTER JOIN mfl_interface_db.facilities_emr_type
                         ON mfl_interface_db.facilities_emr_info.type_id = facilities_emr_type.id
         LEFT OUTER JOIN mfl_interface_db.facilities_hts_info
                         ON mfl_interface_db.facilities_facility_info.id = facilities_hts_info.facility_info_id
         LEFT OUTER JOIN mfl_interface_db.facilities_hts_use_type
                         ON mfl_interface_db.facilities_hts_info.hts_use_name_id = facilities_hts_use_type.id
         LEFT OUTER JOIN mfl_interface_db.facilities_hts_deployment_type
                         ON mfl_interface_db.facilities_hts_info.deployment_id = facilities_hts_deployment_type.id
         LEFT OUTER JOIN mfl_interface_db.facilities_il_info
                         ON mfl_interface_db.facilities_il_info.facility_info_id = facilities_facility_info.id
         LEFT OUTER JOIN mfl_interface_db.facilities_mhealth_info
                         ON mfl_interface_db.facilities_mhealth_info.facility_info_id = facilities_facility_info.id
         LEFT OUTER JOIN mfl_interface_db.facilities_implementation_type
                         ON mfl_interface_db.facilities_implementation_type.facility_info_id =
                            facilities_facility_info.id
         LEFT OUTER JOIN mfl_interface_db.facilities_sdp_agencies
                         ON mfl_interface_db.facilities_sdp_agencies.id = facilities_partners.agency_id
where facilities_facility_info.approved = True