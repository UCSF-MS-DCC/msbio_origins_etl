select "external_identifier","end_date","name","start_date","category","dosage","patient_source_id","patient_external_identifier","source_id","treatment_stopped_due_to_efficacy"
union
select
ifnull(ot.treatmentid,'') as "external_identifier",
ifnull(ot.enddate,'') as "end_date",
ifnull(rt.treatmenttype,'') as "name",
ifnull(ot.startdate,'') as "start_date",
ifnull(rt.class,'') as "category",
ifnull(ot.dosage,'') as "dosage",
2 as "patient_source_id",
right(sa.alias_term1, 4) as "patient_external_identifier",
2 as "source_id",
ifnull(ot.stoppedefficacy,'') as "treatment_stopped_due_to_efficacy"
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
into outfile '/var/lib/mysql-files/origins_msbio_etl_treatments.csv'
fields terminated by ','
enclosed by '"'
lines terminated by '\n';
