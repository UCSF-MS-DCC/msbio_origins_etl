select
"external_identifier", "first_name", "last_name", "middle_name", "suffix", "alt_last_name", "dob", "mri_ms_id", "source_id",
"alcohol_weekly_frequency", "smoker_status", "cigarettes_per_day", "handedness", "caffeine_daily_frequency", "overweight",
"overweight_as_child", "overweight_as_teen", "overweight_as_young_adult",
"overweight_as_adult", "overweight_as_older_adult", "years_of_education", "year_converted_rr_to_sp",
"age_of_onset", "year_of_onset", "relapse_count_onset_to_year_5",
"african_american", "caucasian", "hispanic", "asian_pacific", "special_population", "other_ethnicity",
"birth_country", "maternal_birth_country", "paternal_birth_country", "smoking_start_age", "smoking_quit_age", "family_history", "gender"
union
select
right(sa.alias_term1, 4) as "external_identifier",
sp.firstname as "first_name",
sp.lastname as "last_name",
sp.middlename as "middle_name",
sp.suffix as "suffix",
sp.altlastname as "alt_last_name",
sp.dob as "dob",
samri.alias_term1 as "mri_ms_id",
8 as "source_id",
os.drinkfreq as "alcohol_weekly_frequency",
os.smokerstatus as "smoker_status",
os.numcigs as "cigarettes_per_day",
sd.handedness as "handedness",
oe.caffeinefreq as "caffeine_daily_frequency",
oe.overweight as "overweight",
oe.owtchild as "overweight_as_child",
oe.owtteen as "overweight_as_teen",
oe.owtyoungadult as "overweight_as_young_adult",
oe.owtadult as "overweight_as_adult",
oe.owtolder as "overweight_as_older_adult",
oe.yrseducation as "years_of_education",
om.yrrrsp as "year_converted_rr_to_sp",
om.ageofonset as "age_of_onset",
om.onsetyear as "year_of_onset",
count(oa.attackid) as "relapse_count_onset_to_year_5",
sd.aa_flag as "african_american",
sd.caucasian_flag as "caucasian",
sd.hispanic_flag as "hispanic",
sd.asianpacific_flag as "asian_pacific",
sd.specialpops_flag as "special_population",
sd.other_flag as "other_ethnicity",
sp.birthcountry as "birth_country",
sp.motherbirthcountry as "maternal_birth_country",
sp.fatherbirthcountry as "paternal_birth_country",
null as "smoking_start_age",
null as "smoking_quit_age",
sd.familyhxofms as "family_history",
sd.gender as "gender"
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
group by sa.alias_term1
into outfile '/var/lib/mysql-files/msb_origins_patients.csv'
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
