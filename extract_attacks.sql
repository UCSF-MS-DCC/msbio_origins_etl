select "patient_external_identifier","external_identifier","year","month","day","optic_neuritis","optic_neuritis_side","cerebellar","long_tract_motor","cognitive","brainstem","spinal_cord","long_tract_sensory",
"unknown_system_affected","steroids_used","fully_recovered","patient_source_id","source_id"
union
select
right(sa.alias_term1, 4) as "patient_external_identifier",
ifnull(oa.attackid,'') as "external_identifier",
ifnull(oa.attackyr,'') as "year",
ifnull(oa.attackmo,'') as "month",
ifnull(oa.attackdy,'') as "day",
ifnull(oa.on,'') as "optic_neuritis",
ifnull(oa.onside,'') as "optic_neuritis_side",
ifnull(oa.cer,'') as "cerebellar",
ifnull(oa.ltm,'') as "long_tract_motor",
ifnull(oa.cog,'') as "cognitive",
ifnull(oa.bs,'') as "brainstem",
ifnull(oa.sc,'') as "spinal_cord",
ifnull(oa.lts,'') as "long_tract_sensory",
ifnull(oa.unk,'') as "unknown_system_affected",
ifnull(oa.steroids,'') as "steroids_used",
ifnull(oa.fullyrecovered,'') as "fully_recovered",
2 as patient_source_id,
2 as source_id
from
origins.subject_origins oso
join
origins.attack oa
on
oso.subjectid = oa.subjectid
left join
subjects.aliases sa
on
oso.subjectid = sa.subjectid and sa.alias_type = "EPIC"
into outfile '/var/lib/mysql-files/origins_msbio_etl_attacks.csv'
fields terminated by ','
enclosed by '"'
lines terminated by '\n';
