select "external_identifier","end_date","name","start_date","category","dosage","patient_source_id","patient_external_identifier","source_id","treatment_stopped_due_to_efficacy"
union
select
ot.treatmentid as "external_identifier",
ot.enddate as "end_date",
rt.treatmenttype as "name",
ot.startdate as "start_date",
rt.class as "category",
ot.dosage as "dosage",
8 as "patient_source_id",
right(sa.alias_term1, 4) as "patient_external_identifier",
8 as "source_id",
ot.stoppedefficacy as "treatment_stopped_due_to_efficacy"
from
origins.treatment ot
left join
reference.treatmenttypes rt
on
ot.treatmenttypeid = rt.treatmenttypeid
left join
subjects.aliases sa
on
ot.subjectid = sa.subjectid and sa.alias_type = "EPIC"
into outfile '/var/lib/mysql-files/msb_origins_treatments.csv'
fields terminated by ','
enclosed by '"'
lines terminated by '\n';
