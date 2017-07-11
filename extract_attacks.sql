select "patient_external_identifier","external_identifier","year","month","day","optic_neuritis","optic_neuritis_side","cerebellar","long_tract_motor","cognitive","brainstem","spinal_cord","long_tract_sensory",
"unknown_system_affected","steroids_used","fully_recovered","patient_source_id","source_id"
union
select
right(sa.alias_term1, 4) as "patient_external_identifier",
oa.attackid as "external_identifier",
oa.attackyr as "year",
oa.attackmo as "month",
oa.attackdy as "day",
oa.on as "optic_neuritis",
oa.onside as "optic_neuritis_side",
oa.cer as "cerebellar",
oa.ltm as "long_tract_motor",
oa.cog as "cognitive",
oa.bs as "brainstem",
oa.sc as "spinal_cord",
oa.lts as "long_tract_sensory",
oa.unk as "unknown_system_affected",
oa.steroids as "steroids_used",
oa.fullyrecovered as "fully_recovered",
8 as patient_source_id,
8 as source_id
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
into outfile '/var/lib/mysql-files/msb_origins_attacks.csv'
fields terminated by ','
enclosed by '"'
lines terminated by '\n';
