select "patient_external_identifier", "external_identifier","date","age_at_visit","disease_duration","disease_course","msss","edss","fssc_visual","fssc_brainstem","fssc_pyramidal","fssc_cerebellar","fssc_sensory",
"fssc_bowel","fssc_mental","pasat_score","nhpt_dominant_time","nhpt_nondominant_time","nhpt_nondominant_incomplete",
"timed_walk_trial1_ankle_foot_orthotic_used","timed_walk_trial1_assistance","timed_walk_trial1_incomplete",
"timed_walk_trial2_ankle_foot_orthotic_used","timed_walk_trial2_assistance","timed_walk_trial2_incomplete","pasat_incomplete",
"sdmt_score","sdmt_incomplete","timed_walk_trial1_time","timed_walk_trial2_time","acute_transverse_myelitis","optic_neuritis",
"internuclear_ophthalmoplegia","motor_weakness","sensory_disturbance","ataxia","bladder_disturbance","bowel_disturbance","myelopathy",
"cognitive_disturbance","visual_loss","serum_b12","vitamin_d_level","hltv_1_or_hiv_1_titers",
"sed_rate","rheumatoid_factor","antinuclear_antibodies","antidna_antibodies","sjogrens_antibodies","serum_vdrl","angiotensin_converting_enzyme",
"chest_x_ray","borrelia_serology","vlcfa","lactate_pyruvate","urine_porphyine_screen","complete_blood_count","patient_source_id","source_id"
union
select
right(sa.alias_term1, 4) as "patient_external_identifier",
ifnull(ov.visitid,'') as "external_identifier",
ifnull(ov.examdate,'') as "date",
ifnull(ov.ageatexam,'') as "age_at_visit",
ifnull(ov.diseaseduration,'') as "disease_duration",
ifnull(ov.diseasecourse,'') as "disease_course",
ifnull(ov.msss,'') as "msss",
ifnull(of.actualedss,'') as "edss",
ifnull(of.visual,'') as "fssc_visual",
ifnull(of.brainstem,'') as "fssc_brainstem",
ifnull(of.pyramidal,'') as "fssc_pyramidal",
ifnull(of.cerebellar,'') as "fssc_cerebellar",
ifnull(of.sensory,'') as "fssc_sensory",
ifnull(of.bowel,'') as "fssc_bowel",
ifnull(of.mental,'') as "fssc_mental",
ifnull(op.totalscore,'') as "pasat_score",
ifnull(pd.trial1,'') as "nhpt_dominant_time",
ifnull(pn.trial1,'') as "nhpt_nondominant_time",
case when pn.trial1incompletereason is not null or pn.trial2incompletereason is not null then true else false end as "nhpt_nondominant_incomplete",
ifnull(ow.trial1afo,'') as "timed_walk_trial1_ankle_foot_orthotic_used",
ifnull(ow.trial1assistivedevice,'') as "timed_walk_trial1_assistance",
case when ow.trial1incompletereason is not null then true else false end as "timed_walk_trial1_incomplete",
ifnull(ow.trial1afo,'') as "timed_walk_trial2_ankle_foot_orthotic_used",
ifnull(ow.trial2assistivedevice,'') as "timed_walk_trial2_assistance",
case when ow.trial2incompletereason is not null then true else false end as "timed_walk_trial2_incomplete",
case when op.incompletereason is not null or op.incompletedetails is not null then true else false end as "pasat_incomplete",
ifnull(os.score,'') as "sdmt_score",
case when os.sdmtincompletereason is not null or os.sdmtincompletedetails is not null then true else false end as "sdmt_incomplete",
ifnull(ow.trial1,'') as "timed_walk_trial1_time",
ifnull(ow.trial2,'') as "timed_walk_trial2_time",
ifnull(ov.atm,'') as "acute_transverse_myelitis",
ifnull(ov.opticneuritis,'') as "optic_neuritis",
ifnull(ov.ino,'') as "internuclear_ophthalmoplegia",
ifnull(ov.motorweakness,'') as "motor_weakness",
ifnull(ov.sensorydisturbance,'') as "sensory_disturbance",
ifnull(ov.ataxia,'') as "ataxia",
ifnull(ov.bladderdisturbance,'') as "bladder_disturbance",
ifnull(ov.boweldisturbance,'') as "bowel_disturbance",
ifnull(ov.myelopathy,'') as "myelopathy",
ifnull(ov.cogdisturbance,'') as "cognitive_disturbance",
ifnull(ov.visualloss,'') as "visual_loss",
ifnull(ol.b12,'') as "serum_b12",
ifnull(ol.vitamind,'') as "vitamin_d_level",
ifnull(ol.htlvorhiv,'') as "hltv_1_or_hiv_1_titers",
ifnull(ol.sedrateorcrp,'') as "sed_rate",
ifnull(ol.rf,'') as "rheumatoid_factor",
ifnull(ol.ana,'') as "antinuclear_antibodies",
ifnull(ol.dsdna,'') as "antidna_antibodies",
ifnull(ol.ssaorssb,'') as "sjogrens_antibodies",
ifnull(ol.vdrl,'') as "serum_vdrl",
ifnull(ol.ace,'') as "angiotensin_converting_enzyme",
ifnull(ol.chestxray,'') as "chest_x_ray",
ifnull(ol.borrelia,'') as "borrelia_serology",
ifnull(ol.vlcfa,'') as "vlcfa",
ifnull(ol.lactateorpyruvate,'') as "lactate_pyruvate",
ifnull(ol.upscreen,'') as "urine_porphyine_screen",
ifnull(ol.cbc,'') as "complete_blood_count",
8 as "patient_source_id",
8 as "source_id"
from
origins.subject_origins oso
left join
origins.visit ov
on
oso.subjectid = ov.subjectid
left join
origins.fssc of
on
ov.visitid = of.visitid
left join
origins.pasat op
on
ov.visitid = op.visitid
left join
origins.msfc_peg pd
on
ov.visitid = pd.visitid and pd.handtested = "Dominant"
left join
origins.msfc_peg pn
on
ov.visitid = pn.visitid and pn.handtested = "Non-dominant"
left join
origins.msfc_walk ow
on
ov.visitid = ow.visitid
left join
origins.sdmt os
on
ov.visitid = os.visitid
left join
origins.lab ol
on
ov.visitid = ol.visitid
left join
subjects.aliases sa
on
oso.subjectid = sa.subjectid and sa.alias_type = "EPIC"
order by patient_external_identifier, external_identifier
into outfile '/var/lib/mysql-files/origins_msbio_etl_visits.csv'
fields terminated by ','
enclosed by '"'
lines terminated by '\n';

--pd = origins.msfc_peg (dominant hand)
--pn = origins.msfc_peg (non dominant hand)
--ow = origins.msfc_walk
--ol = origins.lab
--oc = origins.csf
