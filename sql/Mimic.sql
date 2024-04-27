--mimic 共73181
select count(*) from mimiciv_icu.icustays;
select count(distinct(stay_id)) from mimiciv_icu.icustays;

--以icustays为左表关联rheumatic_disease、malignant_cancer、metastatic_solid_tumor、aids
--rheumatic_disease = 0       无风湿病
--malignant_cancer = 0        无恶性肿瘤
--metastatic_solid_tumor = 0  无恶性实体肿瘤
--aids = 0                    无艾滋病
--共73181
DROP MATERIALIZED VIEW IF EXISTS self_collection_all CASCADE;
CREATE MATERIALIZED VIEW self_collection_all AS
SELECT
    icu.subject_id,
    icu.hadm_id,
    icu.stay_id,
    sep.stay_id sepsis3_stay_id,
    sep.sepsis3,
    sub_age.age,
    COALESCE ( sep.sepsis3, FALSE ) sepsis,
    charlson.rheumatic_disease,
    charlson.malignant_cancer,
    charlson.metastatic_solid_tumor,
    charlson.aids
FROM
    mimiciv_icu.icustays icu
    LEFT JOIN mimiciv_derived.sepsis3 sep ON ( sep.subject_id = icu.subject_id AND sep.stay_id = icu.stay_id )
    LEFT JOIN mimiciv_derived.age sub_age ON ( sub_age.subject_id = icu.subject_id AND sub_age.hadm_id = icu.hadm_id )
    LEFT JOIN mimiciv_derived.charlson charlson ON ( charlson.subject_id = icu.subject_id AND charlson.hadm_id = icu.hadm_id );

--本次研究的住ICU数据集合（患者18周岁以上、住ICU时间超过1天，暂不排除有相关基础疾病的）
--select count(*) from public.self_collection;
--共 33125
DROP MATERIALIZED VIEW IF EXISTS self_collection CASCADE;
CREATE MATERIALIZED VIEW self_collection AS
SELECT
    icu.subject_id,
    icu.hadm_id,
    icu.stay_id,
--     sep.stay_id sepsis3_stay_id,
--     sep.sepsis3,
    sub_age.age,
-- 	   charlson.rheumatic_disease,
--     charlson.malignant_cancer,
--     charlson.metastatic_solid_tumor,
--     charlson.aids,
    icu.outtime,
    icu.intime,
    EXTRACT(EPOCH FROM icu.outtime - icu.intime) / 3600 / 24 as stay_day,
    COALESCE ( sep.sepsis3, FALSE ) sepsis
FROM
    mimiciv_icu.icustays icu
    LEFT JOIN mimiciv_derived.sepsis3 sep ON ( sep.subject_id = icu.subject_id AND sep.stay_id = icu.stay_id )
    LEFT JOIN mimiciv_derived.age sub_age ON ( sub_age.subject_id = icu.subject_id AND sub_age.hadm_id = icu.hadm_id )
    LEFT JOIN mimiciv_derived.charlson charlson ON ( charlson.subject_id = icu.subject_id AND charlson.hadm_id = icu.hadm_id )
WHERE
    sub_age.age >= 18 and sub_age.age < 89
--     AND rheumatic_disease = 0       --无风湿病
--     AND malignant_cancer = 0        --无恶性肿瘤
--     AND metastatic_solid_tumor = 0  --无恶性实体肿瘤
--     AND aids = 0                    --无艾滋病
    AND EXTRACT(EPOCH FROM icu.outtime - icu.intime) / 3600 > 48;
------------------------------------------------------------------------------------------------------------------------

-- 创建函数，icustays为左表关联所有的mimiciv_hosp.labevents化验数据提前相关数据，同一次住ICU化验多次的计算最大、最小、平均
-- 计算的时间范围为入ICU前6小-后24小时
CREATE OR REPLACE FUNCTION get_hosp_event(target_name VARCHAR, item_id INT, interval_hour INT) RETURNS
    TABLE (
              subject_id INT,
              hadm_id INT,
              stay_id INT,
              itemid INT,
              min_value Float,
              max_value Float,
              avg_value Float,
              t_name VARCHAR
          )
AS
$BODY$
BEGIN
    RETURN QUERY
        SELECT
            stay.subject_id,
            stay.hadm_id,
            stay.stay_id,
            event.itemid,
            min(event.valuenum) min_value,
            max(event.valuenum) max_value,
            avg(event.valuenum) avg_value,
            target_name e_type
        FROM mimiciv_icu.icustays stay
            LEFT JOIN mimiciv_hosp.labevents event ON ( stay.subject_id = event.subject_id AND stay.hadm_id = event.hadm_id )
        WHERE
            event.charttime >= mimiciv_derived.DATETIME_SUB ( stay.intime, INTERVAL '6' HOUR )
            AND EXTRACT(EPOCH FROM event.charttime - stay.intime) / 3600 <= interval_hour
            AND event.itemid = item_id
        GROUP BY stay.subject_id, stay.hadm_id, stay.stay_id, event.itemid;
END;
$BODY$ LANGUAGE plpgsql VOLATILE;


-- 创建函数，icustays为左表关联所有的mimiciv_hosp.labevents化验数据提前相关数据，同一次住ICU化验多次的计算最大、最小、平均
-- 计算的时间范围为入ICU前6小-出ICU
CREATE OR REPLACE FUNCTION get_hosp_event_in_icu(target_name VARCHAR, item_id INT) RETURNS
    TABLE (
              subject_id INT,
              hadm_id INT,
              stay_id INT,
              itemid INT,
              min_value Float,
              max_value Float,
              avg_value Float,
              t_name VARCHAR
          )
AS
$BODY$
BEGIN
    RETURN QUERY
        SELECT
            stay.subject_id,
            stay.hadm_id,
            stay.stay_id,
            event.itemid,
            min(event.valuenum) min_value,
            max(event.valuenum) max_value,
            avg(event.valuenum) avg_value,
            target_name e_type
        FROM mimiciv_icu.icustays stay
            LEFT JOIN mimiciv_hosp.labevents event ON ( stay.subject_id = event.subject_id AND stay.hadm_id = event.hadm_id )
        WHERE
            event.charttime >= mimiciv_derived.DATETIME_SUB ( stay.intime, INTERVAL '6' HOUR )
            AND event.charttime <= stay.outtime
            AND event.itemid = item_id
        GROUP BY stay.subject_id, stay.hadm_id, stay.stay_id, event.itemid;
END;
$BODY$ LANGUAGE plpgsql VOLATILE;

-- 创建函数，icustays为左表关联所有的mimiciv_icu.chartevents仪器监控数据提前相关数据，同一次类型的监控数据的计算最大、最小、平均
-- 计算的时间范围为入ICU前6小-后24小时
CREATE OR REPLACE FUNCTION get_icu_event(target_name VARCHAR, item_id INT, interval_hour INT) RETURNS
    TABLE (
              subject_id INT,
              hadm_id INT,
              stay_id INT,
              itemid INT,
              min_value Float,
              max_value Float,
              avg_value Float,
              t_name VARCHAR
          )
AS
$BODY$
BEGIN
    RETURN QUERY
        SELECT
            stay.subject_id,
            stay.hadm_id,
            stay.stay_id,
            event.itemid,
            min(event.valuenum) min_value,
            max(event.valuenum) max_value,
            avg(event.valuenum) avg_value,
            target_name e_type
        FROM mimiciv_icu.icustays stay
        LEFT JOIN mimiciv_icu.chartevents event ON ( stay.subject_id = event.subject_id AND stay.hadm_id = event.hadm_id AND stay.stay_id = event.stay_id )
        WHERE
            event.charttime >= mimiciv_derived.DATETIME_SUB ( stay.intime, INTERVAL '6' HOUR )
            AND EXTRACT(EPOCH FROM event.charttime - stay.intime) / 3600 <= interval_hour
            AND event.itemid = item_id
        GROUP BY stay.subject_id, stay.hadm_id, stay.stay_id, event.itemid;
END;
$BODY$ LANGUAGE plpgsql VOLATILE;
------------------------------------------------------------------------------------------------------------------------

-- 从blood_differential表里取Lymphocytes、Monocytes、Neutrophils的绝对值和比例
-- 入ICU的-6-+24小时，共71124条
DROP MATERIALIZED VIEW IF EXISTS self_blood_differential CASCADE;
CREATE MATERIALIZED VIEW self_blood_differential AS
SELECT
    stay.subject_id,
    stay.hadm_id,
    stay.stay_id,
    MAX ( bd.wbc ) wbc_max,
    MIN ( bd.wbc ) wbc_min,
    AVG ( bd.wbc ) wbc_avg,
    MAX ( bd.lymphocytes_abs ) lymphocytes_abs_max,
    MIN ( bd.lymphocytes_abs ) lymphocytes_abs_min,
    AVG ( bd.lymphocytes_abs ) lymphocytes_abs_avg,
    MAX ( bd.monocytes_abs ) monocytes_abs_max,
    MIN ( bd.monocytes_abs ) monocytes_abs_min,
    AVG ( bd.monocytes_abs ) monocytes_abs_avg,
    MAX ( bd.neutrophils_abs ) neutrophils_abs_max,
    MIN ( bd.neutrophils_abs ) neutrophils_abs_min,
    AVG ( bd.neutrophils_abs ) neutrophils_abs_avg,
    MAX ( bd.lymphocytes ) lymphocytes_max,
    MIN ( bd.lymphocytes ) lymphocytes_min,
    AVG ( bd.lymphocytes ) lymphocytes_avg,
    MAX ( bd.monocytes ) monocytes_max,
    MIN ( bd.monocytes ) monocytes_min,
    AVG ( bd.monocytes ) monocytes_avg,
    MAX ( bd.neutrophils ) neutrophils_max,
    MIN ( bd.neutrophils ) neutrophils_min,
    AVG ( bd.neutrophils ) neutrophils_avg
FROM
    mimiciv_icu.icustays stay
    LEFT JOIN mimiciv_derived.blood_differential bd on (bd.subject_id = stay.subject_id AND bd.hadm_id = stay.hadm_id)
WHERE
    bd.charttime >= mimiciv_derived.DATETIME_SUB ( stay.intime, INTERVAL '6' HOUR )
    AND EXTRACT(EPOCH FROM bd.charttime - stay.intime) / 3600 <= 24
GROUP BY
    stay.subject_id, stay.hadm_id, stay.stay_id;

-- 从mimiciv_hosp.labevents中取乳酸信息
-- DROP MATERIALIZED VIEW IF EXISTS self_LactateDehydrogenase_Hosp CASCADE;
-- CREATE MATERIALIZED VIEW self_LactateDehydrogenase_Hosp AS
-- SELECT * from get_hosp_event('Lactate Dehydrogenase (LD)',50954,24);

-- Lactate Dehydrogenase (LD) 从first_day_bg_art 及 labevent 中合并
-- select count(*) from self_LactateDehydrogenase
-- 共 28654
DROP MATERIALIZED VIEW IF EXISTS self_LactateDehydrogenase CASCADE;
CREATE MATERIALIZED VIEW self_LactateDehydrogenase AS
SELECT
    subject_id,
    stay_id,
    MAX ( in_max_value ) max_value,
    MIN ( in_min_value ) min_value
FROM
    ( SELECT subject_id, stay_id, lactate_max in_max_value, lactate_min in_min_value FROM mimiciv_derived.first_day_bg_art) lactate
WHERE
    lactate.in_max_value IS NOT NULL
GROUP BY
    subject_id, stay_id;

DROP MATERIALIZED VIEW IF EXISTS self_Troponin_T CASCADE;
CREATE MATERIALIZED VIEW self_Troponin_T AS
SELECT * from get_hosp_event('Troponin T',51003,24);

DROP MATERIALIZED VIEW IF EXISTS self_CreatineKinase_IsoenzymeMB CASCADE;
CREATE MATERIALIZED VIEW self_CreatineKinase_IsoenzymeMB AS
SELECT * from get_hosp_event('Creatine Kinase,Isoenzyme MB',50911,24)
-- UNION SELECT * from get_hosp_event('Creatine Kinase,Isoenzyme MB',51595,24);  -- 无记录删除
;

DROP MATERIALIZED VIEW IF EXISTS self_Albumin CASCADE;
CREATE MATERIALIZED VIEW self_Albumin AS
SELECT * from get_hosp_event('Albumin',50862,24)
-- UNION SELECT * from get_hosp_event('Albumin',53085,24) -- 无记录删除
;

DROP MATERIALIZED VIEW IF EXISTS self_NTproBNP CASCADE;
CREATE MATERIALIZED VIEW self_NTproBNP AS
SELECT * from get_hosp_event('NTproBNP',50963,24);

DROP MATERIALIZED VIEW IF EXISTS self_AnionGap CASCADE;
CREATE MATERIALIZED VIEW self_AnionGap AS
SELECT * from get_hosp_event('Anion Gap',50868,24)
-- UNION SELECT * from get_hosp_event('Anion Gap',52500,24) -- 无记录删除
;

DROP MATERIALIZED VIEW IF EXISTS self_Bicarbonate CASCADE;
CREATE MATERIALIZED VIEW self_Bicarbonate AS
SELECT * from get_hosp_event('Bicarbonate',50882,24);

DROP MATERIALIZED VIEW IF EXISTS self_UreaNitrogen CASCADE;
CREATE MATERIALIZED VIEW self_UreaNitrogen AS
SELECT * from get_hosp_event('Urea Nitrogen',51006,24)
-- UNION SELECT * from get_hosp_event('Urea Nitrogen',52647,24); -- 无记录删除
;

DROP MATERIALIZED VIEW IF EXISTS self_Creatinine CASCADE;
CREATE MATERIALIZED VIEW self_Creatinine AS
SELECT * from get_hosp_event('Creatinine',50912,24)
-- UNION SELECT * from get_hosp_event('Creatinine',52546,24); -- 无记录删除
;

DROP MATERIALIZED VIEW IF EXISTS self_Chloride CASCADE;
CREATE MATERIALIZED VIEW self_Chloride AS
SELECT * from get_hosp_event('Chloride',50902,24)
-- UNION SELECT * from get_hosp_event('Chloride',52535,24); -- 无记录删除
;

DROP MATERIALIZED VIEW IF EXISTS self_Sodium CASCADE;
CREATE MATERIALIZED VIEW self_Sodium AS
SELECT * from get_hosp_event('Sodium',50983,24)
-- UNION SELECT * from get_hosp_event('Sodium',52623,24); -- 无记录删除
;

DROP MATERIALIZED VIEW IF EXISTS self_Potassium CASCADE;
CREATE MATERIALIZED VIEW self_Potassium AS
SELECT * from get_hosp_event('Potassium',50971,24)
-- UNION SELECT * from get_hosp_event('Potassium',52610,24); -- 无记录删除
;

DROP MATERIALIZED VIEW IF EXISTS self_Glucose CASCADE;
CREATE MATERIALIZED VIEW self_Glucose AS
SELECT subject_id, hadm_id, stay_id, min(min_value) g_min_value,  max(max_value) g_max_value, avg(min_value) g_avg_value from
    (
        SELECT * from get_hosp_event('Glucose',52569,24)
        UNION SELECT * from get_hosp_event('Glucose',50931,24)
        UNION SELECT * from get_hosp_event('Glucose',50809,24)
    ) glucose
GROUP BY subject_id, hadm_id, stay_id;

DROP MATERIALIZED VIEW IF EXISTS self_DDimer CASCADE;
CREATE MATERIALIZED VIEW self_DDimer AS
SELECT subject_id, hadm_id, stay_id, min(min_value) g_min_value,  max(max_value) g_max_value, avg(min_value) g_avg_value from
    (
        SELECT * from get_hosp_event('D-Dimer',50915,24)
        UNION SELECT * from get_hosp_event('D-Dimer',51196,24)
        UNION SELECT * from get_hosp_event('D-Dimer',52551,24)
    ) ddimer
GROUP BY subject_id, hadm_id, stay_id;

DROP MATERIALIZED VIEW IF EXISTS self_Hematocrit CASCADE;
CREATE MATERIALIZED VIEW self_Hematocrit AS
SELECT * from get_hosp_event('Hematocrit',51221,24)
-- UNION SELECT * from get_hosp_event('Hematocrit',51638,24) -- 无记录删除
-- UNION SELECT * from get_hosp_event('Hematocrit',51639,24) -- 无记录删除
-- UNION SELECT * from get_hosp_event('Hematocrit',52028,24); -- 无记录删除
;

DROP MATERIALIZED VIEW IF EXISTS self_Hemoglobin CASCADE;
CREATE MATERIALIZED VIEW self_Hemoglobin AS
SELECT subject_id, hadm_id, stay_id, min(min_value) g_min_value,  max(max_value) g_max_value, avg(min_value) g_avg_value from
    (
        SELECT * from get_hosp_event('Hemoglobin',50811,24)
        UNION SELECT * from get_hosp_event('Hemoglobin',51640,24)
        UNION SELECT * from get_hosp_event('Hemoglobin',51222,24)
    ) hemoglobin
GROUP BY subject_id, hadm_id, stay_id;

DROP MATERIALIZED VIEW IF EXISTS self_PlateletCount CASCADE;
CREATE MATERIALIZED VIEW self_PlateletCount AS
SELECT * from get_hosp_event('Platelet Count',51265,24)
-- UNION SELECT * from get_hosp_event('Platelet Count',51704,24); -- 无记录删除
;

DROP MATERIALIZED VIEW IF EXISTS self_AlanineAminotransferase CASCADE;
CREATE MATERIALIZED VIEW self_AlanineAminotransferase AS
SELECT * from get_hosp_event('Alanine Aminotransferase (ALT)',50861,24);

DROP MATERIALIZED VIEW IF EXISTS self_Amylase CASCADE;
CREATE MATERIALIZED VIEW self_Amylase AS
SELECT * from get_hosp_event('Amylase',50867,24)
-- UNION SELECT * from get_hosp_event('Amylase',53087,24); -- 无记录删除
;

DROP MATERIALIZED VIEW IF EXISTS self_BilirubinTotal CASCADE;
CREATE MATERIALIZED VIEW self_BilirubinTotal AS
SELECT * from get_hosp_event('Bilirubin, Total',50885,24)
-- UNION SELECT * from get_hosp_event('Bilirubin, Total',53089,24);  -- 无记录删除
;

DROP MATERIALIZED VIEW IF EXISTS self_calcium CASCADE;
CREATE MATERIALIZED VIEW self_calcium AS
SELECT
    stay.subject_id,
    stay.hadm_id,
    stay.stay_id,
    min(ch.calcium) min_value,
    max(ch.calcium) max_value,
    avg(ch.calcium) avg_value,
    'calcium' e_type
FROM mimiciv_icu.icustays stay
LEFT JOIN mimiciv_derived.chemistry ch ON ( stay.subject_id = ch.subject_id AND stay.hadm_id = ch.hadm_id )
WHERE
    ch.charttime >= mimiciv_derived.DATETIME_SUB ( stay.intime, INTERVAL '6' HOUR )
    AND EXTRACT(EPOCH FROM ch.charttime - stay.intime) / 3600 <= 24
GROUP BY stay.subject_id, stay.hadm_id, stay.stay_id;

-- 游离钙 50808 51624
DROP MATERIALIZED VIEW IF EXISTS self_calcium_new CASCADE;
CREATE MATERIALIZED VIEW self_calcium_new AS
SELECT * FROM get_hosp_event('Free Calcium',50808,24)
UNION SELECT * FROM get_hosp_event('Free Calcium',51624,24)
;

DROP MATERIALIZED VIEW IF EXISTS self_alp CASCADE;
CREATE MATERIALIZED VIEW self_alp AS
SELECT
    stay.subject_id,
    stay.hadm_id,
    stay.stay_id,
    min(ch.alp) min_value,
    max(ch.alp) max_value,
    avg(ch.alp) avg_value,
    'alp' e_type
FROM mimiciv_icu.icustays stay
LEFT JOIN mimiciv_derived.enzyme ch ON ( stay.subject_id = ch.subject_id AND stay.hadm_id = ch.hadm_id )
WHERE
    ch.charttime >= mimiciv_derived.DATETIME_SUB ( stay.intime, INTERVAL '6' HOUR )
    AND EXTRACT(EPOCH FROM ch.charttime - stay.intime) / 3600 <= 24
GROUP BY stay.subject_id, stay.hadm_id, stay.stay_id;

DROP MATERIALIZED VIEW IF EXISTS self_gcs CASCADE;
CREATE MATERIALIZED VIEW self_gcs AS
SELECT
    stay.subject_id,
    stay.hadm_id,
    stay.stay_id,
    min(ch.gcs) min_value,
    max(ch.gcs) max_value,
    avg(ch.gcs) avg_value,
    'gcs' e_type
FROM mimiciv_icu.icustays stay
LEFT JOIN mimiciv_derived.gcs ch ON ( stay.subject_id = ch.subject_id AND stay.stay_id = ch.stay_id )
WHERE
    ch.charttime >= mimiciv_derived.DATETIME_SUB ( stay.intime, INTERVAL '6' HOUR )
    AND EXTRACT(EPOCH FROM ch.charttime - stay.intime) / 3600 <= 24
GROUP BY stay.subject_id, stay.hadm_id, stay.stay_id;

DROP MATERIALIZED VIEW IF EXISTS self_charlson_comorbidity_index CASCADE;
CREATE MATERIALIZED VIEW self_charlson_comorbidity_index AS
SELECT
    stay.subject_id,
    stay.hadm_id,
    stay.stay_id,
    min(ch.charlson_comorbidity_index) min_value,
    max(ch.charlson_comorbidity_index) max_value,
    avg(ch.charlson_comorbidity_index) avg_value,
    'charlson_comorbidity_index' e_type
FROM mimiciv_icu.icustays stay
LEFT JOIN mimiciv_derived.charlson ch ON ( stay.subject_id = ch.subject_id AND stay.hadm_id = ch.hadm_id )
GROUP BY stay.subject_id, stay.hadm_id, stay.stay_id;

DROP MATERIALIZED VIEW IF EXISTS self_sofa CASCADE;
CREATE MATERIALIZED VIEW self_sofa AS
SELECT
    stay.subject_id,
    stay.hadm_id,
    stay.stay_id,
    min(ch.sofa) min_value,
    max(ch.sofa) max_value,
    avg(ch.sofa) avg_value,
    'sofa' e_type
FROM mimiciv_icu.icustays stay
LEFT JOIN mimiciv_derived.first_day_sofa ch ON ( stay.subject_id = ch.subject_id AND stay.hadm_id = ch.hadm_id )
GROUP BY stay.subject_id, stay.hadm_id, stay.stay_id;

DROP MATERIALIZED VIEW IF EXISTS self_urineoutput CASCADE;
CREATE MATERIALIZED VIEW self_urineoutput AS
SELECT
    stay.subject_id,
    stay.hadm_id,
    stay.stay_id,
    min(ch.urineoutput) min_value,
    max(ch.urineoutput) max_value,
    avg(ch.urineoutput) avg_value,
    'urineoutput' e_type
FROM mimiciv_icu.icustays stay
LEFT JOIN mimiciv_derived.first_day_urine_output ch ON ( stay.subject_id = ch.subject_id AND stay.stay_id = ch.stay_id )
GROUP BY stay.subject_id, stay.hadm_id, stay.stay_id;

DROP MATERIALIZED VIEW IF EXISTS self_weight_admit CASCADE;
CREATE MATERIALIZED VIEW self_weight_admit AS
SELECT
    stay.subject_id,
    stay.hadm_id,
    stay.stay_id,
    min(ch.weight_admit) min_value,
    max(ch.weight_admit) max_value,
    avg(ch.weight_admit) avg_value,
    'weight_admit' e_type
FROM mimiciv_icu.icustays stay
LEFT JOIN mimiciv_derived.first_day_weight ch ON ( stay.subject_id = ch.subject_id AND stay.stay_id = ch.stay_id )
GROUP BY stay.subject_id, stay.hadm_id, stay.stay_id;

DROP MATERIALIZED VIEW IF EXISTS self_hr_mbp_resp_temperature CASCADE;
CREATE MATERIALIZED VIEW self_hr_mbp_resp_temperature AS
SELECT
    stay.subject_id,
    stay.hadm_id,
    stay.stay_id,
    min(ch.heart_rate_min) min_heart_rate,
    max(ch.heart_rate_max) max_heart_rate,
    avg(ch.heart_rate_mean) avg_heart_rate,
    min(ch.mbp_min) min_mbp,
    max(ch.mbp_max) max_mbp,
    avg(ch.mbp_mean) avg_mbp,
    min(ch.resp_rate_min) min_resp_rate,
    max(ch.resp_rate_max) max_resp_rate,
    avg(ch.resp_rate_mean) avg_resp_rate,
    min(ch.temperature_min) min_temperature,
    max(ch.temperature_max) max_temperature,
    avg(ch.temperature_mean) avg_temperature,

    min(ch.sbp_min) min_sbp,
    max(ch.sbp_max) max_sbp,
    avg(ch.sbp_mean) avg_sbp,
    min(ch.dbp_min) min_dbp,
    max(ch.dbp_max) max_dbp,
    avg(ch.dbp_mean) avg_dbp,
    min(ch.spo2_min) min_spo2,
    max(ch.spo2_max) max_spo2,
    avg(ch.spo2_mean) avg_spo2,
    'hr_mbp_resp_temperature' e_type
FROM mimiciv_icu.icustays stay
LEFT JOIN mimiciv_derived.first_day_vitalsign ch ON ( stay.subject_id = ch.subject_id AND stay.stay_id = ch.stay_id )
GROUP BY stay.subject_id, stay.hadm_id, stay.stay_id;

DROP MATERIALIZED VIEW IF EXISTS self_icp CASCADE;
CREATE MATERIALIZED VIEW self_icp AS
SELECT
    stay.subject_id,
    stay.hadm_id,
    stay.stay_id,
    max(ch.icp) max_value,
    min(ch.icp) min_value,
    avg(ch.icp) avg_value,
    'icp' e_type
FROM mimiciv_icu.icustays stay
LEFT JOIN mimiciv_derived.icp ch ON ( stay.subject_id = ch.subject_id AND stay.stay_id = ch.stay_id )
WHERE
    ch.charttime >= mimiciv_derived.DATETIME_SUB ( stay.intime, INTERVAL '6' HOUR )
    AND EXTRACT(EPOCH FROM ch.charttime - stay.intime) / 3600 <= 24
GROUP BY stay.subject_id, stay.hadm_id, stay.stay_id;

DROP MATERIALIZED VIEW IF EXISTS self_crp CASCADE;
CREATE MATERIALIZED VIEW self_crp AS
SELECT
    stay.subject_id,
    stay.hadm_id,
    stay.stay_id,
    min(ch.crp) min_value,
    max(ch.crp) max_value,
    avg(ch.crp) avg_value,
    'crp' e_type
FROM mimiciv_icu.icustays stay
LEFT JOIN mimiciv_derived.inflammation ch ON ( stay.subject_id = ch.subject_id AND stay.hadm_id = ch.hadm_id )
WHERE
    ch.charttime >= mimiciv_derived.DATETIME_SUB ( stay.intime, INTERVAL '6' HOUR )
    AND EXTRACT(EPOCH FROM ch.charttime - stay.intime) / 3600 <= 24
GROUP BY stay.subject_id, stay.hadm_id, stay.stay_id;

DROP MATERIALIZED VIEW IF EXISTS self_sapsii CASCADE;
CREATE MATERIALIZED VIEW self_sapsii AS
SELECT
    stay.subject_id,
    stay.hadm_id,
    stay.stay_id,
    avg(ch.sapsii) sapsii,
    'sapsii' e_type
FROM mimiciv_icu.icustays stay
LEFT JOIN mimiciv_derived.sapsii ch ON ( stay.subject_id = ch.subject_id AND stay.stay_id = ch.stay_id AND stay.hadm_id = ch.hadm_id)
WHERE
    ch.starttime >= mimiciv_derived.DATETIME_SUB ( stay.intime, INTERVAL '6' HOUR )
    AND EXTRACT(EPOCH FROM ch.starttime - stay.intime) / 3600 <= 24
GROUP BY stay.subject_id, stay.hadm_id, stay.stay_id;

DROP MATERIALIZED VIEW IF EXISTS self_pt CASCADE;
CREATE MATERIALIZED VIEW self_pt AS
SELECT
    stay.subject_id,
    stay.hadm_id,
    stay.stay_id,
    min(ch.pt) min_value,
    max(ch.pt) max_value,
    avg(ch.pt) avg_value,
    'pt' e_type
FROM mimiciv_icu.icustays stay
LEFT JOIN mimiciv_derived.coagulation ch ON ( stay.subject_id = ch.subject_id AND stay.hadm_id = ch.hadm_id )
WHERE
    ch.charttime >= mimiciv_derived.DATETIME_SUB ( stay.intime, INTERVAL '6' HOUR )
    AND EXTRACT(EPOCH FROM ch.charttime - stay.intime) / 3600 <= 24
GROUP BY stay.subject_id, stay.hadm_id, stay.stay_id;

DROP MATERIALIZED VIEW IF EXISTS self_inr CASCADE;
CREATE MATERIALIZED VIEW self_inr AS
SELECT
    stay.subject_id,
    stay.hadm_id,
    stay.stay_id,
    min(ch.inr) min_value,
    max(ch.inr) max_value,
    avg(ch.inr) avg_value,
    'inr' e_type
FROM mimiciv_icu.icustays stay
LEFT JOIN mimiciv_derived.coagulation ch ON ( stay.subject_id = ch.subject_id AND stay.hadm_id = ch.hadm_id )
WHERE
    ch.charttime >= mimiciv_derived.DATETIME_SUB ( stay.intime, INTERVAL '6' HOUR )
    AND EXTRACT(EPOCH FROM ch.charttime - stay.intime) / 3600 <= 24
GROUP BY stay.subject_id, stay.hadm_id, stay.stay_id;

DROP MATERIALIZED VIEW IF EXISTS self_death_ratio CASCADE;
CREATE MATERIALIZED VIEW self_death_ratio AS
SELECT
    stay.subject_id,
    stay.hadm_id,
    stay.stay_id,
    stay.dod,
    stay.dischtime,
    stay.admittime,
    EXTRACT(EPOCH FROM stay.dischtime - stay.admittime) / 3600 / 24 los,
    EXTRACT(EPOCH FROM stay.dod - stay.admittime) / 3600 / 24 cl,
    COALESCE( EXTRACT(EPOCH FROM stay.dod - stay.admittime) / 3600 / 24 <=28 , False) death28
FROM mimiciv_derived.icustay_detail stay;


-- 后面全部要-6 -> 出ICU
DROP MATERIALIZED VIEW IF EXISTS self_sepsis_aki CASCADE;
CREATE MATERIALIZED VIEW self_sepsis_aki AS
SELECT
    ks.subject_id,
    ks.hadm_id,
    ks.stay_id,
    max(aki_stage) max_aki_stage,
    min(aki_stage) min_aki_stage,
    max(aki_stage) >= 2 sepsis_aki
FROM
    mimiciv_derived.kdigo_stages ks
    LEFT JOIN mimiciv_derived.charlson cl ON ( ks.subject_id = cl.subject_id AND ks.hadm_id = cl.hadm_id )
WHERE
    cl.renal_disease = 0
GROUP BY ks.subject_id,ks.hadm_id,ks.stay_id;


DROP MATERIALIZED VIEW IF EXISTS self_50861 CASCADE;
CREATE MATERIALIZED VIEW self_50861 AS
SELECT * from get_hosp_event_in_icu('50861',50861);

DROP MATERIALIZED VIEW IF EXISTS self_53084 CASCADE;
CREATE MATERIALIZED VIEW self_53084 AS
SELECT * from get_hosp_event_in_icu('53084',53084);

DROP MATERIALIZED VIEW IF EXISTS self_50878 CASCADE;
CREATE MATERIALIZED VIEW self_50878 AS
SELECT * from get_hosp_event_in_icu('50878',50878);

DROP MATERIALIZED VIEW IF EXISTS self_53088 CASCADE;
CREATE MATERIALIZED VIEW self_53088 AS
SELECT * from get_hosp_event_in_icu('53088',53088);

DROP MATERIALIZED VIEW IF EXISTS self_50885 CASCADE;
CREATE MATERIALIZED VIEW self_50885 AS
SELECT * from get_hosp_event_in_icu('50885',50885);

DROP MATERIALIZED VIEW IF EXISTS self_53089 CASCADE;
CREATE MATERIALIZED VIEW self_53089 AS
SELECT * from get_hosp_event_in_icu('53089',53089);

DROP MATERIALIZED VIEW IF EXISTS self_liver_injury_collection CASCADE;
CREATE MATERIALIZED VIEW self_liver_injury_collection AS
SELECT subject_id, hadm_id, stay_id from (
    SELECT * from self_50861
    UNION SELECT * from self_53084
    UNION SELECT * from self_50878
    UNION SELECT * from self_53088
    UNION SELECT * from self_50885
    UNION SELECT * from self_53089
) collection
group by subject_id, hadm_id, stay_id;

DROP MATERIALIZED VIEW IF EXISTS self_51265 CASCADE;
CREATE MATERIALIZED VIEW self_51265 AS
SELECT * from get_hosp_event_in_icu('51265',51265);

DROP MATERIALIZED VIEW IF EXISTS self_51704 CASCADE;
CREATE MATERIALIZED VIEW self_51704 AS
SELECT * from get_hosp_event_in_icu('51704',51704);

DROP MATERIALIZED VIEW IF EXISTS self_platelet_count_score CASCADE;
CREATE MATERIALIZED VIEW self_platelet_count_score AS
SELECT
    a1.subject_id,
    a1.hadm_id,
    a1.stay_id,
    a1.platelet_count_score_51265,
    a2.platelet_count_score_51704,
    CASE
        WHEN COALESCE(a1.platelet_count_score_51265, 0) > COALESCE(a2.platelet_count_score_51704, 0) THEN COALESCE(a1.platelet_count_score_51265, 0)
        ELSE COALESCE(a2.platelet_count_score_51704, 0)
    END	platelet_count_score
FROM
    (
        SELECT
            subject_id,
            hadm_id,
            stay_id,
            self_51265.min_value,
            CASE
                WHEN self_51265.min_value < 100 THEN 2
                WHEN self_51265.min_value < 150 THEN 1
                ELSE 0
            END platelet_count_score_51265
        FROM
            self_51265
    ) a1
        LEFT JOIN (
        SELECT
            subject_id,
            hadm_id,
            stay_id,
            self_51704.min_value,
            CASE
                WHEN self_51704.min_value < 100 THEN 2
                WHEN self_51704.min_value < 150 THEN 1
                ELSE 0
            END platelet_count_score_51704
        FROM
            self_51704
    ) a2 on (a1.subject_id=a2.subject_id and a1.hadm_id = a2.hadm_id and a1.stay_id = a2.stay_id);

DROP MATERIALIZED VIEW IF EXISTS self_inr CASCADE;
CREATE MATERIALIZED VIEW self_inr AS
SELECT
    stay.subject_id,
    stay.hadm_id,
    stay.stay_id,
    min(ch.inr) min_inr,
    max(ch.inr) max_inr,
    avg(ch.inr) avg_inr,
    'inr' e_type
FROM mimiciv_icu.icustays stay
LEFT JOIN mimiciv_derived.coagulation ch ON ( stay.subject_id = ch.subject_id AND stay.hadm_id = ch.hadm_id )
WHERE
    ch.charttime >= mimiciv_derived.DATETIME_SUB ( stay.intime, INTERVAL '6' HOUR )
    AND EXTRACT(EPOCH FROM ch.charttime - stay.intime) / 3600 <= 24
GROUP BY stay.subject_id, stay.hadm_id, stay.stay_id;


DROP MATERIALIZED VIEW IF EXISTS self_inr_total CASCADE;
CREATE MATERIALIZED VIEW self_inr_total AS
SELECT
    stay.subject_id,
    stay.hadm_id,
    stay.stay_id,
    min(ch.inr) min_inr,
    max(ch.inr) max_inr,
    avg(ch.inr) avg_inr,
    'inr' e_type
FROM mimiciv_icu.icustays stay
LEFT JOIN mimiciv_derived.coagulation ch ON ( stay.subject_id = ch.subject_id AND stay.hadm_id = ch.hadm_id )
WHERE
    ch.charttime >= stay.intime and charttime <= stay.outtime
GROUP BY stay.subject_id, stay.hadm_id, stay.stay_id;


DROP MATERIALIZED VIEW IF EXISTS self_sepsis_li CASCADE;
CREATE MATERIALIZED VIEW self_sepsis_li AS
SELECT
    s.subject_id,
    s.hadm_id,
    s.stay_id,
    cl.mild_liver_disease,
    cl.severe_liver_disease,
    self_50885.max_value max_50885,
    self_53089.max_value max_53089,
    self_inr_total.max_inr max_inr,
    COALESCE((self_50885.max_value > 2 or self_53089.max_value > 2) and self_inr_total.max_inr > 1.5, false) sepsis_li
FROM
    self_liver_injury_collection s
    LEFT JOIN self_inr_total on (s.subject_id = self_inr_total.subject_id and s.hadm_id = self_inr_total.hadm_id and s.stay_id = self_inr_total.stay_id)
    LEFT JOIN self_50885 on (s.subject_id = self_50885.subject_id and s.hadm_id = self_50885.hadm_id and s.stay_id = self_50885.stay_id)
    LEFT JOIN self_53089 on (s.subject_id = self_53089.subject_id and s.hadm_id = self_53089.hadm_id and s.stay_id = self_53089.stay_id)
    LEFT JOIN mimiciv_derived.charlson cl on (s.subject_id = cl.subject_id and s.hadm_id = cl.hadm_id)
where cl.mild_liver_disease = 0 and cl.severe_liver_disease = 0;


DROP MATERIALIZED VIEW IF EXISTS self_ptr_score CASCADE;
CREATE MATERIALIZED VIEW self_ptr_score AS
SELECT subject_id, hadm_id,stay_id, max_value/12 max_pt_12,
       CASE
           WHEN max_value is NULL THEN NULL
           WHEN max_value/12 > 1.4 THEN 2
           WHEN max_value/12 > 1.2 THEN 1
           ELSE 0
           END ptr
from (SELECT * from get_hosp_event_in_icu('51274',51274)) aa;

DROP MATERIALIZED VIEW IF EXISTS self_sofa24_score CASCADE;
CREATE MATERIALIZED VIEW self_sofa24_score AS
select
    stay_id,
    sofa_24hours,
    CASE
        WHEN sofa_24hours >= 2 THEN 2
        WHEN sofa_24hours >= 1 THEN 1
        ELSE 0
    END sofa24_score
from (select stay_id,max(sofa_24hours) sofa_24hours from mimiciv_derived.sofa GROUP BY stay_id) aa;

DROP MATERIALIZED VIEW IF EXISTS self_sepsis_co_match CASCADE;
CREATE MATERIALIZED VIEW self_sepsis_co_match AS
SELECT subject_id, hadm_id, max(match_icd_code_value) match_icd_code FROM (
   select subject_id, hadm_id,
        CASE COALESCE (substr(di.icd_code,1,3) in ('286','287','451','D68','D69','I80','D65') , FALSE )
        WHEN TRUE THEN 1 ELSE 0 END match_icd_code_value
   from mimiciv_hosp.diagnoses_icd di
) aa GROUP BY subject_id, hadm_id;

--56741
DROP MATERIALIZED VIEW IF EXISTS self_sepsis_co CASCADE;
CREATE MATERIALIZED VIEW self_sepsis_co AS
SELECT
    pc.subject_id,
    pc.hadm_id,
    pc.stay_id,
    pc.platelet_count_score,
    ptr.ptr,
    ss.sofa24_score,
    pc.platelet_count_score + ptr.ptr + ss.sofa24_score >=4 sepsis_co
FROM
    self_platelet_count_score pc
    LEFT JOIN self_ptr_score ptr on  ( pc.subject_id = ptr.subject_id AND pc.hadm_id = ptr.hadm_id AND pc.stay_id = ptr.stay_id )
    LEFT JOIN self_sofa24_score ss on  ( pc.stay_id = ss.stay_id )
    LEFT JOIN self_sepsis_co_match di ON (pc.subject_id = di.subject_id AND pc.hadm_id = di.hadm_id)
where di.match_icd_code = 0;

DROP MATERIALIZED VIEW IF EXISTS self_sepsis_ali_collection CASCADE;
CREATE MATERIALIZED VIEW self_sepsis_ali_collection AS
SELECT
    sp.subject_id,
    sp.hadm_id,
    max(COALESCE(sp.stay_id, 0)) stay_id
FROM
    (
        ( SELECT subject_id, stay_id, hadm_id FROM mimiciv_derived.suspicion_of_infection GROUP BY subject_id, stay_id, hadm_id )
    ) sp
        LEFT JOIN ( SELECT stay_id, MAX ( stay_id ) ok_stay_id FROM mimiciv_derived.ventilation WHERE ventilation_status IN ( 'Tracheostomy', 'InvasiveVent', 'HFNC' ) GROUP BY stay_id ) vs ON vs.stay_id = sp.stay_id
        LEFT JOIN (
            SELECT bg.subject_id, bg.hadm_id, MAX ( pao2fio2ratio ) ok_pao2fio2ratio FROM mimiciv_derived.bg bg
            LEFT JOIN mimiciv_icu.icustays stay on bg.hadm_id=stay.hadm_id and bg.subject_id = stay.subject_id
            WHERE (pao2fio2ratio < 200
                AND charttime >= mimiciv_derived.DATETIME_SUB ( stay.intime, INTERVAL '6' HOUR )
                AND charttime <= stay.outtime) GROUP BY bg.subject_id, bg.hadm_id
        ) bg ON bg.subject_id = sp.subject_id AND bg.hadm_id = sp.hadm_id
WHERE
    vs.ok_stay_id IS NOT NULL
   OR ok_pao2fio2ratio IS NOT NULL
GROUP BY sp.subject_id, sp.hadm_id;

DROP MATERIALIZED VIEW IF EXISTS self_sepsis_ali CASCADE;
CREATE MATERIALIZED VIEW self_sepsis_ali AS
SELECT
    sc.subject_id,
    sc.hadm_id,
    sc.stay_id,
    CASE WHEN cl.chronic_pulmonary_disease = 0 AND cl.congestive_heart_failure = 0 THEN ssa.subject_id IS NOT NULL END sepsis_ali
FROM
    self_collection sc
    LEFT JOIN self_sepsis_ali_collection ssa ON ( ( ssa.stay_id = sc.stay_id OR ssa.stay_id = 0 ) AND ssa.hadm_id = sc.hadm_id AND ssa.subject_id = sc.subject_id)
    LEFT JOIN mimiciv_derived.charlson cl ON ( cl.hadm_id = sc.hadm_id AND cl.subject_id = sc.subject_id );


-- #####################################################################################################################
-- 以self_collection我们的研究对象为左表关联mimic_derived.first_day_bg_art、mimic_derived.blood_differential补齐数据
-- select count(*) from self_sepsis_derived;
-- 共33125
DROP MATERIALIZED VIEW IF EXISTS self_sepsis_20230822 CASCADE;
CREATE MATERIALIZED VIEW self_sepsis_20230822 AS
SELECT
    sc.*,
    fdba.po2_min,
    fdba.po2_max,
    (fdba.po2_min + fdba.po2_max)/2 po2_avg,
    fdba.pco2_min,
    fdba.pco2_max,
    (fdba.pco2_min + fdba.pco2_max)/2 pco2_avg,
    fdba.ph_min,
    fdba.ph_max,
    (fdba.ph_min + fdba.ph_max)/2 ph_avg,
    fdba.baseexcess_min,
    fdba.baseexcess_max,
    (fdba.baseexcess_min + fdba.baseexcess_max)/2 baseexcess_avg,
    fdba.pao2fio2ratio_min,
    fdba.pao2fio2ratio_max,
    (fdba.pao2fio2ratio_min + fdba.pao2fio2ratio_max)/2 pao2fio2ratio_avg,
    sbd.wbc_min,
    sbd.wbc_max,
    sbd.wbc_avg,
    sbd.lymphocytes_abs_min,
    sbd.lymphocytes_abs_max,
    sbd.lymphocytes_abs_avg,
    sbd.monocytes_abs_min,
    sbd.monocytes_abs_max,
    sbd.monocytes_abs_avg,
    sbd.neutrophils_abs_min,
    sbd.neutrophils_abs_max,
    sbd.neutrophils_abs_avg,
    sbd.lymphocytes_min,
    sbd.lymphocytes_max,
    sbd.lymphocytes_avg,
    sbd.monocytes_min,
    sbd.monocytes_max,
    sbd.monocytes_avg,
    sbd.neutrophils_min,
    sbd.neutrophils_max,
    sbd.neutrophils_avg,
    ld.max_value lactate_dehydrogenase_max,
    tt.min_value troponin_t_min,
    tt.max_value troponin_t_max,
    tt.avg_value troponin_t_avg,
    ci.min_value creatine_kinase_isoenzyme_min,
    ci.max_value creatine_kinase_isoenzyme_max,
    ci.avg_value creatine_kinase_isoenzyme_avg,
    al.min_value albumin_min,
    al.max_value albumin_max,
    al.avg_value albumin_avg,
    ag.min_value anion_gap_min,
    ag.max_value anion_gap_max,
    ag.avg_value anion_gap_avg,
    un.min_value urea_nitrogen_min,
    un.max_value urea_nitrogen_max,
    un.avg_value urea_nitrogen_avg,
    cr.min_value creatinine_min,
    cr.max_value creatinine_max,
    cr.avg_value creatinine_avg,
    chloride.min_value chloride_min,
    chloride.max_value chloride_max,
    chloride.avg_value chloride_avg,
    sodium.min_value sodium_min,
    sodium.max_value sodium_max,
    sodium.avg_value sodium_avg,
    po.min_value potassium_min,
    po.max_value potassium_max,
    po.avg_value potassium_avg,
    glu.g_min_value glucose_min,
    glu.g_max_value glucose_max,
    glu.g_avg_value glucose_avg,
    hem.min_value hematocrit_min,
    hem.max_value hematocrit_max,
    hem.avg_value hematocrit_avg,
    hemo.g_min_value hemoglobin_min,
    hemo.g_max_value hemoglobin_max,
    hemo.g_avg_value hemoglobin_avg,
    pc.min_value platelet_count_min,
    pc.max_value platelet_count_max,
    pc.avg_value platelet_count_avg,
    alt.min_value alanine_aminotransferase_min,
    alt.max_value alanine_aminotransferase_max,
    alt.avg_value alanine_aminotransferase_avg,
    bil.min_value bilirubin_total_min,
    bil.max_value bilirubin_total_max,
    bil.avg_value bilirubin_total_avg,
    calcium.min_value calcium_min,
    calcium.max_value calcium_max,
    calcium.avg_value calcium_avg,
    alp.min_value alp_min,
    alp.max_value alp_max,
    alp.avg_value alp_avg,
-- #     gcs.max_value gcs_max,
    ch.height,
    cci.max_value charlson_comorbidity_index_max,
-- #     sofa.max_value sofa_max,
    urineoutput.max_value urineoutput_max,
    wa.max_value weight_admit_max,
    hmrt.min_heart_rate,
    hmrt.max_heart_rate,
    hmrt.avg_heart_rate,
    hmrt.min_mbp,
    hmrt.max_mbp,
    hmrt.avg_mbp,
    hmrt.min_resp_rate,
    hmrt.max_resp_rate,
    hmrt.avg_resp_rate,
    hmrt.min_temperature,
    hmrt.max_temperature,
    hmrt.avg_temperature,
    hmrt.min_sbp,
    hmrt.max_sbp,
    hmrt.avg_sbp,
    hmrt.min_dbp,
    hmrt.max_dbp,
    hmrt.avg_dbp,
    hmrt.min_spo2,
    hmrt.max_spo2,
    hmrt.avg_spo2,
    gender.gender,
-- #     lods.lods,
-- #     meld.meld,
-- #     oas.oasis,
-- #     sapsii.sapsii,
    spt.min_value pt_min,
    spt.max_value pt_max,
    spt.avg_value pt_avg,
    sinr.min_inr inr_min,
    sinr.max_inr inr_max,
    sinr.avg_inr inr_avg,
-- #     sdr.los,
-- #     sdr.cl,
    sdr.death28,
    shd.max_aki_stage aki_stage_max,
    shd.sepsis_aki,
    ssl.sepsis_li,
    sso.sepsis_co,
    ssa.sepsis_ali
FROM
    self_collection sc
    LEFT JOIN mimiciv_derived.first_day_bg_art fdba ON ( fdba.stay_id = sc.stay_id AND fdba.subject_id = sc.subject_id )
    LEFT JOIN self_blood_differential sbd ON ( sbd.stay_id = sc.stay_id and sbd.hadm_id = sc.hadm_id and sbd.subject_id = sc.subject_id )
    LEFT JOIN self_LactateDehydrogenase ld ON ( ld.stay_id = sc.stay_id and ld.subject_id = sc.subject_id )
    left join "public".self_troponin_t tt on (tt.stay_id = sc.stay_id and tt.hadm_id = sc.hadm_id and tt.subject_id = sc.subject_id)
    left join "public".self_creatinekinase_isoenzymemb ci on (ci.stay_id = sc.stay_id and ci.hadm_id = sc.hadm_id and ci.subject_id = sc.subject_id)
    left join "public".self_ntprobnp nt on (nt.stay_id = sc.stay_id and nt.hadm_id = sc.hadm_id and nt.subject_id = sc.subject_id)
    left join "public".self_albumin al on (al.stay_id = sc.stay_id and al.hadm_id = sc.hadm_id and al.subject_id = sc.subject_id)
    left join "public".self_aniongap ag on (ag.stay_id = sc.stay_id and ag.hadm_id = sc.hadm_id and ag.subject_id = sc.subject_id)
    left join "public".self_ureanitrogen un on (un.stay_id = sc.stay_id and un.hadm_id = sc.hadm_id and un.subject_id = sc.subject_id)
    left join "public".self_creatinine cr on (cr.stay_id = sc.stay_id and cr.hadm_id = sc.hadm_id and cr.subject_id = sc.subject_id)
    left join "public".self_chloride chloride on (chloride.stay_id = sc.stay_id and chloride.hadm_id = sc.hadm_id and chloride.subject_id = sc.subject_id)
    left join "public".self_sodium sodium on (sodium.stay_id = sc.stay_id and sodium.hadm_id = sc.hadm_id and sodium.subject_id = sc.subject_id)
    left join "public".self_potassium po on (po.stay_id = sc.stay_id and po.hadm_id = sc.hadm_id and po.subject_id = sc.subject_id)
    left join "public".self_glucose glu on (glu.stay_id = sc.stay_id and glu.hadm_id = sc.hadm_id and glu.subject_id = sc.subject_id)
    left join "public".self_ddimer ddimer on (ddimer.stay_id = sc.stay_id and ddimer.hadm_id = sc.hadm_id and ddimer.subject_id = sc.subject_id)
    left join "public".self_hematocrit hem on (hem.stay_id = sc.stay_id and hem.hadm_id = sc.hadm_id and hem.subject_id = sc.subject_id)
    left join "public".self_hemoglobin hemo on (hemo.stay_id = sc.stay_id and hemo.hadm_id = sc.hadm_id and hemo.subject_id = sc.subject_id)
    left join "public".self_plateletcount pc on (pc.stay_id = sc.stay_id and pc.hadm_id = sc.hadm_id and pc.subject_id = sc.subject_id)
    left join "public".self_alanineaminotransferase alt on (alt.stay_id = sc.stay_id and alt.hadm_id = sc.hadm_id and alt.subject_id = sc.subject_id)
    left join "public".self_amylase amy on (amy.stay_id = sc.stay_id and amy.hadm_id = sc.hadm_id and amy.subject_id = sc.subject_id)
    left join "public".self_bilirubintotal bil on (bil.stay_id = sc.stay_id and bil.hadm_id = sc.hadm_id and bil.subject_id = sc.subject_id)
    LEFT JOIN self_calcium_new calcium on (calcium.stay_id = sc.stay_id and calcium.hadm_id = sc.hadm_id and calcium.subject_id = sc.subject_id)
    LEFT JOIN self_alp alp on (alp.stay_id = sc.stay_id and alp.hadm_id = sc.hadm_id and alp.subject_id = sc.subject_id)
    LEFT JOIN self_gcs gcs on (gcs.stay_id = sc.stay_id and gcs.hadm_id = sc.hadm_id and gcs.subject_id = sc.subject_id)
    LEFT JOIN mimiciv_derived.height ch on (ch.stay_id = sc.stay_id and ch.subject_id = sc.subject_id)
    LEFT JOIN self_charlson_comorbidity_index cci on (cci.stay_id = sc.stay_id and cci.hadm_id = sc.hadm_id and cci.subject_id = sc.subject_id)
    LEFT JOIN self_sofa sofa on (sofa.stay_id = sc.stay_id and sofa.hadm_id = sc.hadm_id and sofa.subject_id = sc.subject_id)
    LEFT JOIN self_urineoutput urineoutput on (urineoutput.stay_id = sc.stay_id and urineoutput.hadm_id = sc.hadm_id and urineoutput.subject_id = sc.subject_id)
    LEFT JOIN self_weight_admit wa on (wa.stay_id = sc.stay_id and wa.hadm_id = sc.hadm_id and wa.subject_id = sc.subject_id)
    LEFT JOIN self_hr_mbp_resp_temperature hmrt on (hmrt.stay_id = sc.stay_id and hmrt.hadm_id = sc.hadm_id and hmrt.subject_id = sc.subject_id)
    LEFT JOIN self_icp icp on (icp.stay_id = sc.stay_id and icp.hadm_id = sc.hadm_id and icp.subject_id = sc.subject_id)
    LEFT JOIN mimiciv_derived.icustay_detail gender on (gender.stay_id = sc.stay_id and gender.hadm_id = sc.hadm_id and gender.subject_id = sc.subject_id)
    LEFT JOIN self_crp crp on (crp.stay_id = sc.stay_id and crp.hadm_id = sc.hadm_id and crp.subject_id = sc.subject_id)
    LEFT JOIN mimiciv_derived.lods lods on (lods.stay_id = sc.stay_id and lods.hadm_id = sc.hadm_id and lods.subject_id = sc.subject_id)
    LEFT JOIN mimiciv_derived.meld meld on (meld.stay_id = sc.stay_id and meld.hadm_id = sc.hadm_id and meld.subject_id = sc.subject_id)
    LEFT JOIN mimiciv_derived.oasis oas on (oas.stay_id = sc.stay_id and oas.hadm_id = sc.hadm_id and oas.subject_id = sc.subject_id)
    LEFT JOIN self_sapsii sapsii on (sapsii.stay_id = sc.stay_id and sapsii.hadm_id = sc.hadm_id and sapsii.subject_id = sc.subject_id)
    LEFT JOIN self_pt spt ON ( spt.stay_id = sc.stay_id AND spt.hadm_id = sc.hadm_id AND spt.subject_id = sc.subject_id )
    LEFT JOIN self_inr sinr ON ( sinr.stay_id = sc.stay_id AND sinr.hadm_id = sc.hadm_id AND sinr.subject_id = sc.subject_id )
    LEFT JOIN self_death_ratio sdr ON ( sdr.stay_id = sc.stay_id AND sdr.hadm_id = sc.hadm_id AND sdr.subject_id = sc.subject_id )
    LEFT JOIN self_sepsis_aki shd ON ( shd.stay_id = sc.stay_id AND shd.hadm_id = sc.hadm_id AND shd.subject_id = sc.subject_id )
    LEFT JOIN self_sepsis_li ssl ON ( ssl.stay_id = sc.stay_id AND ssl.hadm_id = sc.hadm_id AND ssl.subject_id = sc.subject_id )
    LEFT JOIN self_sepsis_co sso ON ( sso.stay_id = sc.stay_id AND sso.hadm_id = sc.hadm_id AND sso.subject_id = sc.subject_id )
    LEFT JOIN self_sepsis_ali ssa ON (ssa.stay_id = sc.stay_id AND ssa.hadm_id = sc.hadm_id AND ssa.subject_id = sc.subject_id)
;


DROP MATERIALIZED VIEW IF EXISTS self_icd_code_5x CASCADE;
CREATE MATERIALIZED VIEW self_icd_code_5x AS
SELECT
    ss.*,
    di.icd_code,
    substr(di.icd_code,1,3) x_type
FROM
    self_sepsis_20230822 ss
LEFT JOIN mimiciv_hosp.diagnoses_icd di ON ss.subject_id = di.subject_id AND ss.hadm_id = di.hadm_id
WHERE
   substr(di.icd_code,1,3) in ('582','585','586','V56');

select x_type, count(*) from self_icd_code_5x GROUP BY x_type;

SELECT
    sc.subject_id,
    sc.hadm_id,
    sc.stay_id,
    cl.chronic_pulmonary_disease,
    cl.congestive_heart_failure,
    CASE WHEN cl.chronic_pulmonary_disease = 0 AND cl.congestive_heart_failure = 0 THEN ssa.subject_id IS NOT NULL END sepsis_ali
FROM
    self_collection sc
        LEFT JOIN self_sepsis_ali_collection ssa ON ( ( ssa.stay_id = sc.stay_id OR ssa.stay_id = 0 ) AND ssa.hadm_id = sc.hadm_id AND ssa.subject_id = sc.subject_id)
        LEFT JOIN mimiciv_derived.charlson cl ON ( cl.hadm_id = sc.hadm_id AND cl.subject_id = sc.subject_id )
WHERE sc.sepsis is TRUE and
--   cl.chronic_pulmonary_disease !=0
--   cl.congestive_heart_failure !=0
     (sc.subject_id, sc.hadm_id, sc.stay_id) in
     (select subject_id,hadm_id,stay_id from mimiciv_derived.suspicion_of_infection soi where soi.specimen
        in ( 'TRACHEAL ASPIRATE', 'BRONCHIAL BRUSH', 'BRONCHIAL BRUSH - PROTECTED', 'BRONCHOALVEOLAR LAVAGE', 'BRONCHIAL WASHINGS', 'SPUTUM' )
        GROUP BY subject_id,hadm_id,stay_id)



DROP MATERIALIZED VIEW IF EXISTS self_specimen_count CASCADE;
CREATE MATERIALIZED VIEW self_specimen_count AS
SELECT
	soi.hadm_id,
	soi.subject_id,
	soi.stay_id,
	COUNT ( 1 ),
-- 	string_agg(specimen, ', ') ,
	bool_or(
		specimen = ANY ( ARRAY [ 'TRACHEAL ASPIRATE', 'BRONCHIAL BRUSH', 'BRONCHIAL BRUSH - PROTECTED', 'ASPIRATE', 'BRONCHOALVEOLAR LAVAGE', 'BRONCHIAL WASHINGS', 'SPUTUM', 'Rapid Respiratory Viral Screen & Culture', 'RAPID RESPIRATORY VIRAL ANTIGEN TEST', 'Mini-BAL', 'Influenza A/B by DFA', 'Influenza A/B by DFA - Bronch Lavage' ] )
	) AS pulmonary,
	bool_or(
		specimen = ANY ( ARRAY [ 'BLOOD CULTURE ( MYCO/F LYTIC BOTTLE)', 'BLOOD CULTURE', 'BLOOD', 'Blood (CMV AB)', 'Blood (Malaria)', 'Blood (Toxo)', 'DIALYSIS FLUID', 'Blood (LYME)', 'Blood (EBV)', 'Stem Cell - Blood Culture', 'SEROLOGY/BLOOD', 'BLOOD BAG FLUID' ] )
	) AS blood,
	bool_or( specimen = ANY ( ARRAY [ 'PERITONEAL FLUID', 'BILE', 'STOOL' ] ) ) AS abdomen,
	bool_or( specimen = ANY ( ARRAY [ 'URINE,KIDNEY', 'URINE', 'URINE,SUPRAPUBIC ASPIRATE' ] ) ) AS URINE,
	bool_or( specimen = ANY ( ARRAY [ 'CSF;SPINAL FLUID' ] ) ) AS "central nervous system",
bool_or( specimen = ANY ( ARRAY [ 'PROSTHETIC JOINT FLUID', 'JOINT FLUID', 'BONE MARROW', 'BONE MARROW - CYTOGENETICS' ] ) ) AS "Bones and joints",
bool_or( specimen = ANY ( ARRAY [ 'FLUID WOUND', 'SKIN SCRAPINGS', 'FOOT CULTURE', 'TISSUE' ] ) ) AS "Skin and soft tissue",
bool_or(
	specimen = ANY (
		ARRAY [ 'CORNEAL EYE SCRAPINGS',
		'NAIL SCRAPINGS',
		'ARTHROPOD',
		'Direct Antigen Test for Herpes Simplex Virus Types 1 & 2',
		'ABSCESS',
		'THROAT CULTURE',
		'SWAB, R/O GC',
		'FOREIGN BODY',
		'Swab R/O Yeast Screen',
		'ANORECTAL/VAGINAL',
		'PERIPHERAL BLOOD LYMPHOCYTES',
		'SWAB',
		'Isolate',
		'DIRECT ANTIGEN TEST FOR VARICELLA-ZOSTER VIRUS',
		'C, E, & A Screening',
		'CRE Screen',
		'Staph aureus swab',
		'Infection Control Yeast',
		'IMMUNOLOGY',
		'EYE',
		'BIOPSY',
		'NEOPLASTIC BLOOD',
		'VIRAL CULTURE',
		'BLOOD CULTURE - NEONATE',
		'VIRAL CULTURE:R/O HERPES SIMPLEX VIRUS',
		'FLUID,OTHER',
		'WORM',
		'STOOL (RECEIVED IN TRANSPORT SYSTEM)',
		'Swab',
		'EAR',
		'MRSA SCREEN',
		'VIRAL CULTURE: R/O CYTOMEGALOVIRUS',
		'Touch Prep/Sections',
		'CATHETER TIP-IV',
		'FECAL SWAB',
		'THROAT FOR STREP',
		'FECAL SWAB',
		'Foreign Body - Sonication Culture',
		'Influenza A/B by DFA - Bronch Lavage',
		'PLEURAL FLUID',
		'Immunology (CMV)',
		'POSTMORTEM CULTURE' ]
	)
) AS "Other"
FROM
	mimiciv_derived."suspicion_of_infection" soi
	LEFT JOIN self_sepsis_20230822 ss ON soi.hadm_id = ss.hadm_id
	AND soi.subject_id = ss.subject_id
	AND ( soi.stay_id = ss.stay_id OR soi.stay_id IS NULL )
WHERE
	soi.suspected_infection_time > ss.intime
	AND soi.suspected_infection_time < ss.outtime
GROUP BY
	soi.hadm_id,
	soi.subject_id,
	soi.stay_id

SELECT SUM
	( CASE WHEN pulmonary THEN 1 ELSE 0 END ) pulmonary_t,
	SUM ( CASE WHEN blood THEN 1 ELSE 0 END ) blood_t,
	SUM ( CASE WHEN abdomen THEN 1 ELSE 0 END ) abdomen_t,
	SUM ( CASE WHEN URINE THEN 1 ELSE 0 END ) URINE_t,
	SUM ( CASE WHEN "central nervous system" THEN 1 ELSE 0 END ) "central nervous system_t",
	SUM ( CASE WHEN "Bones and joints" THEN 1 ELSE 0 END ) "Bones and joints_t",
	SUM ( CASE WHEN "Skin and soft tissue" THEN 1 ELSE 0 END ) "Skin and soft tissue_t",
	SUM ( CASE WHEN "Other" THEN 1 ELSE 0 END ) Other_t
FROM
	self_specimen_count
--9681	97157	12257	52652	1285	993	4138	36721
--4681	8627	2299	4963	376	    48	258	    8996
--4128	6886	1763	4127	341	    34	213	    7544
------------------------------------------------------------------------------------------------------------------------

--统计最后的表
DROP MATERIALIZED VIEW IF EXISTS self_sepsis_ali_20230824 CASCADE;
CREATE MATERIALIZED VIEW self_sepsis_ali_20230824 AS
SELECT
	ss.*,
	ssc.pulmonary,
	ssc.blood,
	ssc.abdomen,
	ssc.URINE,
	ssc."central nervous system",
	ssc."Bones and joints",
	ssc."Skin and soft tissue",
	ssc."Other"
FROM
	self_sepsis_20230822 ss
	LEFT JOIN self_specimen_count ssc ON ss.subject_id = ssc.subject_id
	AND ss.hadm_id = ssc.hadm_id
	AND ( ss.stay_id = ssc.stay_id OR ssc.stay_id IS NULL )
WHERE
	ss.sepsis
	AND ( NOT ssc.pulmonary )