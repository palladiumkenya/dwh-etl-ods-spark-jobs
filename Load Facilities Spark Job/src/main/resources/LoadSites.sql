select
    `mfl_interface_db`.`facilities_facility_info`.`mfl_code` AS `MFL_Code`,
    `mfl_interface_db`.`facilities_facility_info`.`name` AS `Facility Name`,
    `mfl_interface_db`.`facilities_counties`.`name` AS `County`,
    `mfl_interface_db`.`facilities_sub_counties`.`name` AS `SubCounty`,
    `mfl_interface_db`.`facilities_owner`.`name` AS `Owner`,
    `mfl_interface_db`.`facilities_facility_info`.`lat` AS `Latitude`,
    `mfl_interface_db`.`facilities_facility_info`.`lon` AS `Longitude`,
    `mfl_interface_db`.`facilities_partners`.`name` AS `SDP`,
    `mfl_interface_db`.`facilities_emr_type`.`type` AS `EMR`,
    `mfl_interface_db`.`facilities_emr_info`.`status` AS `EMR Status`,
    `mfl_interface_db`.`facilities_hts_use_type`.`hts_use_name` AS `HTS Use`,
    `mfl_interface_db`.`facilities_hts_deployment_type`.`deployment` AS `HTS Deployment`,
    `mfl_interface_db`.`facilities_hts_info`.`status` AS `HTS Status`,
    `mfl_interface_db`.`facilities_il_info`.`status` AS `IL Status`,
    `mfl_interface_db`.`facilities_il_info`.`webADT_registration` AS `Registration IE`,
    `mfl_interface_db`.`facilities_il_info`.`webADT_pharmacy` AS `Pharmacy IE`,
    `mfl_interface_db`.`facilities_il_info`.`Mlab` AS `mlab`,
    `mfl_interface_db`.`facilities_il_info`.`Ushauri` AS `Ushauri`,
    `mfl_interface_db`.`facilities_mhealth_info`.`Nishauri` AS `Nishauri`,
    `mfl_interface_db`.`facilities_emr_info`.`ovc` AS `OVC`,
    `mfl_interface_db`.`facilities_emr_info`.`otz` AS `OTZ`,
    `mfl_interface_db`.`facilities_emr_info`.`prep` AS `PrEP`,
    `mfl_interface_db`.`facilities_il_info`.`three_PM` AS `3PM`,
    `mfl_interface_db`.`facilities_il_info`.`air` AS `AIR`,
    `mfl_interface_db`.`facilities_implementation_type`.`KP` AS `KP`,
    `mfl_interface_db`.`facilities_emr_info`.`mnch` AS `MCH`,
    `mfl_interface_db`.`facilities_emr_info`.`tb` AS `TB`,
    `mfl_interface_db`.`facilities_emr_info`.`lab_manifest` AS `Lab Manifest`,
    `mfl_interface_db`.`facilities_facility_info`.`date_added` AS `Date Added`
from
    ((((((((((((`mfl_interface_db`.`facilities_facility_info`
        left join `mfl_interface_db`.`facilities_owner` on
            ((`mfl_interface_db`.`facilities_owner`.`id` = `mfl_interface_db`.`facilities_facility_info`.`owner_id`)))
        left join `mfl_interface_db`.`facilities_counties` on
            ((`mfl_interface_db`.`facilities_counties`.`id` = `mfl_interface_db`.`facilities_facility_info`.`county_id`)))
        left join `mfl_interface_db`.`facilities_sub_counties` on
            ((`mfl_interface_db`.`facilities_sub_counties`.`id` = `mfl_interface_db`.`facilities_facility_info`.`sub_county_id`)))
        left join `mfl_interface_db`.`facilities_partners` on
            ((`mfl_interface_db`.`facilities_partners`.`id` = `mfl_interface_db`.`facilities_facility_info`.`partner_id`)))
        left join `mfl_interface_db`.`facilities_emr_info` on
            ((`mfl_interface_db`.`facilities_facility_info`.`id` = `mfl_interface_db`.`facilities_emr_info`.`id`)))
        left join `mfl_interface_db`.`facilities_emr_type` on
            ((`mfl_interface_db`.`facilities_emr_info`.`type_id` = `mfl_interface_db`.`facilities_emr_type`.`id`)))
        left join `mfl_interface_db`.`facilities_hts_info` on
            ((`mfl_interface_db`.`facilities_facility_info`.`id` = `mfl_interface_db`.`facilities_hts_info`.`facility_info_id`)))
        left join `mfl_interface_db`.`facilities_hts_use_type` on
            ((`mfl_interface_db`.`facilities_hts_info`.`hts_use_name_id` = `mfl_interface_db`.`facilities_hts_use_type`.`id`)))
        left join `mfl_interface_db`.`facilities_hts_deployment_type` on
            ((`mfl_interface_db`.`facilities_hts_info`.`deployment_id` = `mfl_interface_db`.`facilities_hts_deployment_type`.`id`)))
        left join `mfl_interface_db`.`facilities_il_info` on
            ((`mfl_interface_db`.`facilities_il_info`.`facility_info_id` = `mfl_interface_db`.`facilities_facility_info`.`id`)))
        left join `mfl_interface_db`.`facilities_mhealth_info` on
            ((`mfl_interface_db`.`facilities_mhealth_info`.`facility_info_id` = `mfl_interface_db`.`facilities_facility_info`.`id`)))
        left join `mfl_interface_db`.`facilities_implementation_type` on
        ((`mfl_interface_db`.`facilities_implementation_type`.`facility_info_id` = `mfl_interface_db`.`facilities_facility_info`.`id`)))
where
    (`mfl_interface_db`.`facilities_facility_info`.`approved` = true)