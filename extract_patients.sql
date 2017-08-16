select
"external_identifier", "first_name", "last_name", "middle_name", "suffix", "alt_last_name", "dob", "mri_ms_id", "source_id",
"alcohol_weekly_frequency", "smoker_status", "cigarettes_per_day", "handedness", "caffeine_daily_frequency", "overweight",
"overweight_as_child", "overweight_as_teen", "overweight_as_young_adult",
"overweight_as_adult", "overweight_as_older_adult", "years_of_education", "year_converted_rr_to_sp",
"age_of_onset", "year_of_onset", "relapse_count_onset_to_year_5",
"african_american", "caucasian", "hispanic", "asian_pacific", "special_population", "other_ethnicity",
"birth_country", "maternal_birth_country", "paternal_birth_country", "smoking_start_age", "smoking_quit_age", "family_history", "gender", 'patient_identifier'
union
select
right(sa.alias_term1, 4) as "external_identifier",
ifnull(sp.firstname, '') as "first_name",
ifnull(sp.lastname, '') as "last_name",
ifnull(sp.middlename, '') as "middle_name",
ifnull(sp.suffix, '') as "suffix",
ifnull(sp.altlastname,'') as "alt_last_name",
ifnull(sp.dob,'') as "dob",
ifnull(samri.alias_term1,'') as "mri_ms_id",
8 as "source_id",
ifnull(os.drinkfreq,'') as "alcohol_weekly_frequency",
ifnull(os.smokerstatus,'') as "smoker_status",
ifnull(os.numcigs,'') as "cigarettes_per_day",
ifnull(sd.handedness,'') as "handedness",
ifnull(oe.caffeinefreq,'') as "caffeine_daily_frequency",
ifnull(oe.overweight,'') as "overweight",
ifnull(oe.owtchild,'') as "overweight_as_child",
ifnull(oe.owtteen,'') as "overweight_as_teen",
ifnull(oe.owtyoungadult,'') as "overweight_as_young_adult",
ifnull(oe.owtadult,'') as "overweight_as_adult",
ifnull(oe.owtolder,'') as "overweight_as_older_adult",
ifnull(oe.yrseducation,'') as "years_of_education",
ifnull(om.yrrrsp,'') as "year_converted_rr_to_sp",
ifnull(om.ageofonset,'') as "age_of_onset",
ifnull(om.onsetyear,'') as "year_of_onset",
count(oa.attackid) as "relapse_count_onset_to_year_5",
ifnull(sd.aa_flag,'') as "african_american",
ifnull(sd.caucasian_flag,'') as "caucasian",
ifnull(sd.hispanic_flag,'') as "hispanic",
ifnull(sd.asianpacific_flag,'') as "asian_pacific",
ifnull(sd.specialpops_flag,'') as "special_population",
ifnull(sd.other_flag,'') as "other_ethnicity",
ifnull(sp.birthcountry,'') as "birth_country",
ifnull(sp.motherbirthcountry,'') as "maternal_birth_country",
ifnull(sp.fatherbirthcountry,'') as "paternal_birth_country",
'' as "smoking_start_age",
'' as "smoking_quit_age",
ifnull(sd.familyhxofms,'') as "family_history",
ifnull(sd.gender,'') as "gender",
ifnull(sam.alias_term1, '') as "patient_identifier"
from
origins.subject_origins oso
left join
subjects.aliases sa
on
oso.subjectid = sa.subjectid and sa.alias_type = "EPIC"
left join
subjects.aliases samri
on
oso.subjectid = samri.subjectid and samri.alias_type = "MRI"
left join
subjects.phi sp
on
oso.subjectid = sp.subjectid
left join
origins.smokedrink os
on
oso.subjectid = os.subjectid
left join
subjects.demographics sd
on
oso.subjectid = sd.subjectid
left join
origins.eduweight oe
on
oso.subjectid = oe.subjectid
left join
origins.mshistory om
on
oso.subjectid = om.subjectid
left join
origins.attack oa
on
oso.subjectid = oa.subjectid
left join
subjects.aliases sam
on
sam.subjectid = oso.subjectid and sam.alias_type = "MRN"
group by sa.alias_term1
into outfile '/var/lib/mysql-files/origins_msbio_etl_patients.csv'
fields terminated by ','
enclosed by '"'
lines terminated by '\n';


-- se = subjects.ethniccode
-- sa = subjects.aliases
-- sp = subjects.phi
-- os = origins.smokedrink
-- sd = subjects.demographics
-- oe = origins.eduweight
-- om = origins.mshistory
-- oso = origins.subject_origins
-- samri = subjects.aliases - for ms_mri_id field
