update icu_patient ip set charlson_comorbidity_index_max =
    COALESCE ( CAST ( ip.心梗1 AS FLOAT ), 0 ) +
	COALESCE ( CAST ( ip.心衰 AS FLOAT ), 0 ) +
	COALESCE ( CAST ( ip.周围血管 AS FLOAT ), 0 )+
	COALESCE ( CAST ( ip.痴呆 AS FLOAT ), 0 )+
	COALESCE ( CAST ( ip.脑血管（无后遗症） AS FLOAT ), 0 )+
	COALESCE ( CAST ( ip.结缔组织 AS FLOAT ), 0 )+
	COALESCE ( CAST ( ip.消化性溃疡 AS FLOAT ), 0 )+
	COALESCE ( CAST ( ip.糖尿病（无并发症） AS FLOAT ), 0 )+
	COALESCE ( CAST ( ip.慢性肺病 AS FLOAT ), 0 )+
	COALESCE ( CAST ( ip.轻度肝病 AS FLOAT ), 0 )+
	COALESCE ( CAST ( ip.偏瘫2 AS FLOAT ), 0 )+
	COALESCE ( CAST ( ip."CKD" AS FLOAT ), 0 )+
	COALESCE ( CAST ( ip.糖尿病（有并发症） AS FLOAT ), 0 )+
	COALESCE ( CAST ( ip.实体瘤 AS FLOAT ), 0 )+
	COALESCE ( CAST ( ip.白血病 AS FLOAT ), 0 )+
	COALESCE ( CAST ( ip.淋巴瘤 AS FLOAT ), 0 )+
	COALESCE ( CAST ( ip."3肝病（门脉高压、出血）" AS FLOAT ), 0 )+
	COALESCE ( CAST (ip."6转移肿瘤" AS FLOAT ), 0 )



-- 创建新视图 t_icu_patient 删除重复的住院号
DROP MATERIALIZED VIEW IF EXISTS self_icu_patient CASCADE;
CREATE MATERIALIZED VIEW self_icu_patient AS
SELECT t.*, TO_TIMESTAMP(t.入科时间, 'YYYY-MM-DD HH24:MI:SS') in_time, TO_TIMESTAMP(t.出科时间, 'YYYY-MM-DD HH24:MI:SS') out_time
FROM icu_patient t
INNER JOIN (SELECT 住院号, max(入科时间) intime FROM icu_patient GROUP BY 住院号) tmp
ON tmp.住院号 = t.住院号 AND tmp.intime = t.入科时间;

-- 共285
SELECT count(1) from self_icu_patient;

-- 计算pao2fio2ratio
DROP MATERIALIZED VIEW IF EXISTS self_icu_pao2fio2ratio CASCADE;
CREATE MATERIALIZED VIEW self_icu_pao2fio2ratio AS
SELECT
    po.病人号,
    po.采样时间,
    cast(po.检验结果 as FLOAT) po2,
    cast(f.检验结果 as FLOAT) fio2,
    cast(po.检验结果 as FLOAT) / cast(f.检验结果 as FLOAT) pao2fio2ratio
FROM
    pao2 po
LEFT JOIN fio2 f ON po.病人号 = f.病人号 and po.采样时间 = f.采样时间;


DROP MATERIALIZED VIEW IF EXISTS self_icu_xueqi CASCADE;
CREATE MATERIALIZED VIEW self_icu_xueqi AS
select ag.住院号,
       ag_max,
       ag_min,
       ag_avg,
       be_max,
       be_min,
       be_avg,
       ca_max,
       ca_min,
       ca_avg,
       ci_max,
       ci_min,
       ci_avg,
       fio2_max,
       fio2_min,
       fio2_avg,
       glu_max,
       glu_min,
       glu_avg,
       k_max,
       k_min,
       k_avg,
       lac_max,
       lac_min,
       lac_avg,
       na_max,
       na_min,
       na_avg,
       paco2_max,
       paco2_min,
       paco2_avg,
       pao2_max,
       pao2_min,
       pao2_avg,
       ph_max,
       ph_min,
       ph_avg,
       pao2fio2ratio_max,
       pao2fio2ratio_min,
       pao2fio2ratio_avg
from
(
    SELECT
        t.住院号,
        max(ag.res) ag_max,
        min(ag.res) ag_min,
        avg(ag.res) ag_avg
    FROM
        self_icu_patient t
        LEFT JOIN (select 病人号, TO_TIMESTAMP(采样时间, 'YYYY-MM-DD HH24:MI:SS') sample_time, cast(检验结果 as FLOAT) res from ag) as ag ON t.住院号 = ag.病人号 AND t.in_time <= ag.sample_time AND ag.sample_time <= t.out_time
    GROUP BY t.住院号
) ag
left join
(
    SELECT
        t.住院号,
        max(be.res) be_max,
        min(be.res) be_min,
        avg(be.res) be_avg
    FROM
        self_icu_patient t
        LEFT JOIN (select 病人号, TO_TIMESTAMP(采样时间, 'YYYY-MM-DD HH24:MI:SS') sample_time, cast(检验结果 as FLOAT) res from be) as be ON t.住院号 = be.病人号 AND t.in_time <= be.sample_time AND be.sample_time <= t.out_time
    GROUP BY t.住院号
) be on ag.住院号 = be.住院号
left join
(
    SELECT
        t.住院号,
        max(ca.res) ca_max,
        min(ca.res) ca_min,
        avg(ca.res) ca_avg
    FROM
        self_icu_patient t
        LEFT JOIN (select 病人号, TO_TIMESTAMP(采样时间, 'YYYY-MM-DD HH24:MI:SS') sample_time, cast(检验结果 as FLOAT) res from ca) as ca ON t.住院号 = ca.病人号 AND t.in_time <= ca.sample_time AND ca.sample_time <= t.out_time
    GROUP BY t.住院号
) ca on ag.住院号 = ca.住院号
left join
(
    SELECT
        t.住院号,
        max(ci.res) ci_max,
        min(ci.res) ci_min,
        avg(ci.res) ci_avg
    FROM
        self_icu_patient t
        LEFT JOIN (select 病人号, TO_TIMESTAMP(采样时间, 'YYYY-MM-DD HH24:MI:SS') sample_time, cast(检验结果 as FLOAT) res from ci) as ci ON t.住院号 = ci.病人号 AND t.in_time <= ci.sample_time AND ci.sample_time <= t.out_time
    GROUP BY t.住院号
) ci on ag.住院号 = ci.住院号
left join
(
    SELECT
        t.住院号,
        max(fio2.res) fio2_max,
        min(fio2.res) fio2_min,
        avg(fio2.res) fio2_avg
    FROM
        self_icu_patient t
    LEFT JOIN (select 病人号, TO_TIMESTAMP(采样时间, 'YYYY-MM-DD HH24:MI:SS') sample_time, cast(检验结果 as FLOAT) res from fio2) as fio2 ON t.住院号 = fio2.病人号 AND t.in_time <= fio2.sample_time AND fio2.sample_time <= t.out_time AND fio2.sample_time <= t.in_time + INTERVAL '24 hours'
    GROUP BY t.住院号
) fio2 on ag.住院号 = fio2.住院号
left join
(
    SELECT
        t.住院号,
        max(glu.res) glu_max,
        min(glu.res) glu_min,
        avg(glu.res) glu_avg
    FROM
        self_icu_patient t
    LEFT JOIN (select 病人号, TO_TIMESTAMP(采样时间, 'YYYY-MM-DD HH24:MI:SS') sample_time, cast(检验结果 as FLOAT) res from glu) as glu ON t.住院号 = glu.病人号 AND t.in_time <= glu.sample_time AND glu.sample_time <= t.out_time
    GROUP BY t.住院号
) glu on ag.住院号 = glu.住院号
left join
(
    SELECT
        t.住院号,
        max(k.res) k_max,
        min(k.res) k_min,
        avg(k.res) k_avg
    FROM
        self_icu_patient t
    LEFT JOIN (select 病人号, TO_TIMESTAMP(采样时间, 'YYYY-MM-DD HH24:MI:SS') sample_time, cast(检验结果 as FLOAT) res from k) as k ON t.住院号 = k.病人号 AND t.in_time <= k.sample_time AND k.sample_time <= t.out_time
    GROUP BY t.住院号
) k on ag.住院号 = k.住院号
left join
(
    SELECT
        t.住院号,
        max(lac.res) lac_max,
        min(lac.res) lac_min,
        avg(lac.res) lac_avg
    FROM
        self_icu_patient t
    LEFT JOIN (select 病人号, TO_TIMESTAMP(采样时间, 'YYYY-MM-DD HH24:MI:SS') sample_time, cast(检验结果 as FLOAT) res from lac) as lac ON t.住院号 = lac.病人号 AND t.in_time <= lac.sample_time AND lac.sample_time <= t.out_time AND lac.sample_time <= t.in_time + INTERVAL '24 hours'
    GROUP BY t.住院号
) lac on ag.住院号 = lac.住院号
left join
(
    SELECT
        t.住院号,
        max(na.res) na_max,
        min(na.res) na_min,
        avg(na.res) na_avg
    FROM
        self_icu_patient t
    LEFT JOIN (select 病人号, TO_TIMESTAMP(采样时间, 'YYYY-MM-DD HH24:MI:SS') sample_time, cast(检验结果 as FLOAT) res from na) as na ON t.住院号 = na.病人号 AND t.in_time <= na.sample_time AND na.sample_time <= t.out_time
    GROUP BY t.住院号
) na on ag.住院号 = na.住院号
left join
(
    SELECT
        t.住院号,
        max(paco2.res) paco2_max,
        min(paco2.res) paco2_min,
        avg(paco2.res) paco2_avg
    FROM
        self_icu_patient t
    LEFT JOIN (select 病人号, TO_TIMESTAMP(采样时间, 'YYYY-MM-DD HH24:MI:SS') sample_time, cast(检验结果 as FLOAT) res from paco2) as paco2 ON t.住院号 = paco2.病人号 AND t.in_time <= paco2.sample_time AND paco2.sample_time <= t.out_time AND paco2.sample_time <= t.in_time + INTERVAL '24 hours'
    GROUP BY t.住院号
) paco2 on ag.住院号 = paco2.住院号
left join
(
    SELECT
        t.住院号,
        max(pao2.res) pao2_max,
        min(pao2.res) pao2_min,
        avg(pao2.res) pao2_avg
    FROM
        self_icu_patient t
    LEFT JOIN (select 病人号, TO_TIMESTAMP(采样时间, 'YYYY-MM-DD HH24:MI:SS') sample_time, cast(检验结果 as FLOAT) res from pao2) as pao2 ON t.住院号 = pao2.病人号 AND t.in_time <= pao2.sample_time AND pao2.sample_time <= t.out_time AND pao2.sample_time <= t.in_time + INTERVAL '24 hours'
    GROUP BY t.住院号
) pao2 on ag.住院号 = pao2.住院号
left join
(
    SELECT
        t.住院号,
        max(ph.res) ph_max,
        min(ph.res) ph_min,
        avg(ph.res) ph_avg
    FROM
        self_icu_patient t
    LEFT JOIN (select 病人号, TO_TIMESTAMP(采样时间, 'YYYY-MM-DD HH24:MI:SS') sample_time, cast(检验结果 as FLOAT) res from ph) as ph ON t.住院号 = ph.病人号 AND t.in_time <= ph.sample_time AND ph.sample_time <= t.out_time
    GROUP BY t.住院号
) ph on ag.住院号 = ph.住院号
left join
(
    SELECT
        t.住院号,
        max(pf.res) pao2fio2ratio_max,
        min(pf.res) pao2fio2ratio_min,
        avg(pf.res) pao2fio2ratio_avg
    FROM
        self_icu_patient t
    LEFT JOIN (select 病人号, TO_TIMESTAMP(采样时间, 'YYYY-MM-DD HH24:MI:SS') sample_time, pao2fio2ratio res from self_icu_pao2fio2ratio) as pf ON t.住院号 = pf.病人号 AND t.in_time <= pf.sample_time AND pf.sample_time <= t.out_time AND pf.sample_time <= t.in_time + INTERVAL '24 hours'
    GROUP BY t.住院号
) pf on ag.住院号 = pf.住院号;

DROP MATERIALIZED VIEW IF EXISTS self_icu_data CASCADE;
CREATE MATERIALIZED VIEW self_icu_data AS
SELECT
    sip.姓名 name,
    sip."AGE" age,
    sip.住院号 subject_id,
    sip.in_time in_time,
    sip.out_time out_time,
    sip.sepsis sepsis,
    x.pao2_min po2_min,
    x.pao2_max po2_max,
    x.pao2_avg po2_avg,
    x.paco2_min pco2_min,
    x.paco2_max pco2_max,
    x.paco2_avg pco2_avg,
    x.ph_min ph_min,
    x.ph_max ph_max,
    x.ph_avg ph_avg,
    x.be_min baseexcess_min,
    x.be_max baseexcess_max,
    x.be_avg baseexcess_avg,
    x.pao2fio2ratio_min pao2fio2ratio_min,
    x.pao2fio2ratio_max pao2fio2ratio_max,
    x.pao2fio2ratio_avg pao2fio2ratio_avg,
    x.lac_min lactate_min,
    x.lac_max lactate_max,
    x.lac_avg lactate_avg,
    x.ag_min anion_gap_min,
    x.ag_max anion_gap_max,
    x.ag_avg anion_gap_avg,
    x.ci_min chloride_min,
    x.ci_max chloride_max,
    x.ag_avg chloride_avg,
    x.na_min sodium_min,
    x.na_max sodium_max,
    x.na_avg sodium_avg,
    x.k_min potassium_min,
    x.k_max potassium_max,
    x.k_avg potassium_avg,
    x.glu_min*18 glucose_min,
    x.glu_max*18 glucose_max,
    x.glu_avg*18 glucose_avg,
    x.ca_min calcium_min,
    x.ca_max calcium_max,
    x.ca_avg calcium_avg,
    sip.charlson_comorbidity_index_max,
    sip.白细胞_min wbc_min,
    sip.白细胞_max wbc_max,
    (cast(sip.白细胞_min as FLOAT) + cast(sip.白细胞_max as FLOAT))/2 wbc_avg,
    sip.淋巴细胞计数_min lymphocytes_abs_min,
    sip.淋巴细胞计数_max lymphocytes_abs_max,
    (cast(sip.淋巴细胞计数_min as FLOAT) + cast(sip.淋巴细胞计数_max as FLOAT))/2 lymphocytes_abs_avg,
    sip.单核细胞计数_min monocytes_abs_min,
    sip.单核细胞计数_max monocytes_abs_max,
    (cast(sip.单核细胞计数_min as FLOAT) + cast(sip.单核细胞计数_max as FLOAT))/2 monocytes_abs_avg,
    sip.中性粒细胞计数_min neutrophils_abs_min,
    sip.中性粒细胞计数_max neutrophils_abs_max,
    (cast(sip.中性粒细胞计数_min as FLOAT) + cast(sip.中性粒细胞计数_max as FLOAT))/2 neutrophils_abs_avg,
    sip.淋巴细胞百分比_min lymphocytes_min,
    sip.淋巴细胞百分比_max lymphocytes_max,
    (cast(sip.淋巴细胞百分比_min as FLOAT) + cast(sip.淋巴细胞百分比_max as FLOAT))/2 lymphocytes_avg,
    sip.单核细胞百分比_min monocytes_min,
    sip.单核细胞百分比_max monocytes_max,
    (cast(sip.单核细胞百分比_min as FLOAT) + cast(sip.单核细胞百分比_max as FLOAT))/2 monocytes_avg,
    sip.中性粒细胞百分比_min neutrophils_min,
    sip.中性粒细胞百分比_max neutrophils_max,
    (cast(sip.中性粒细胞百分比_min as FLOAT) + cast(sip.中性粒细胞百分比_max as FLOAT))/2 neutrophils_avg,
    sip.肌酸激酶_min creatine_kinase_isoenzyme_min,
    sip.肌酸激酶_max creatine_kinase_isoenzyme_max,
    (cast(sip.肌酸激酶_min as FLOAT) + cast(sip.肌酸激酶_max as FLOAT))/2 creatine_kinase_isoenzyme_avg,
    sip.总蛋白_min albumin_min,
    sip.总蛋白_max albumin_max,
    (cast(sip.总蛋白_min as FLOAT) + cast(sip.总蛋白_max as FLOAT))/2 albumin_avg,
    sip.尿素_min urea_nitrogen_min,
    sip.尿素_max urea_nitrogen_max,
    (cast(sip.尿素_min as FLOAT) + cast(sip.尿素_max as FLOAT))/2 urea_nitrogen_avg,
    sip.肌酐_min creatinine_min,
    sip.肌酐_max creatinine_max,
    (cast(sip.肌酐_min as FLOAT) + cast(sip.肌酐_max as FLOAT))/2 creatinine_avg,
    sip.红细胞压积_min hematocrit_min,
    sip.红细胞压积_max hematocrit_max,
    (cast(sip.红细胞压积_min as FLOAT) + cast(sip.红细胞压积_max as FLOAT))/2 hematocrit_avg,
    sip.血红蛋白_min hemoglobin_min,
    sip.血红蛋白_max hemoglobin_max,
    (cast(sip.血红蛋白_min as FLOAT) + cast(sip.血红蛋白_max as FLOAT))/2 hemoglobin_avg,
    sip.血小板_min platelet_count_min,
    sip.血小板_max platelet_count_max,
    (cast(sip.血小板_min as FLOAT) + cast(sip.血小板_max as FLOAT))/2 platelet_count_avg,
    sip.谷丙转氨酶_min alanine_aminotransferase_min,
    sip.谷丙转氨酶_max alanine_aminotransferase_max,
    (cast(sip.谷丙转氨酶_min as FLOAT) + cast(sip.谷丙转氨酶_max as FLOAT))/2 alanine_aminotransferase_avg,
    sip.总胆红素_min bilirubin_total_min,
    sip.总胆红素_max bilirubin_total_max,
    (cast(sip.总胆红素_min as FLOAT) + cast(sip.总胆红素_max as FLOAT))/2 bilirubin_total_avg,
    sip.碱性磷酸酶_min alp_min,
    sip.碱性磷酸酶_max alp_max,
    (cast(sip.碱性磷酸酶_min as FLOAT) + cast(sip.碱性磷酸酶_max as FLOAT))/2 alp_avg,
    sip."24小时尿量" urineoutput_max,
    sip.心率最小值 min_heart_rate,
    sip.心率最大值 max_heart_rate,
    (cast(sip.心率最小值 as FLOAT) + cast(sip.心率最大值 as FLOAT))/2 avg_heart_rate,
    sip.呼吸最小值 min_resp_rate,
    sip.呼吸最大值 max_resp_rate,
    (cast(sip.呼吸最小值 as FLOAT) + cast(sip.呼吸最大值 as FLOAT))/2 avg_resp_rate,
    sip.体温最小值 min_temperature,
    sip.体温最大值 max_temperature,
    (cast(sip.体温最小值 as FLOAT) + cast(sip.体温最大值 as FLOAT))/2 avg_temperature,
    sip.收缩压最小值 min_sbp,
    sip.收缩压最大值 max_sbp,
    (cast(sip.收缩压最小值 as FLOAT) + cast(sip.收缩压最大值 as FLOAT))/2 avg_sbp,
    sip.舒张压最小值 min_dbp,
    sip.舒张压最大值 max_dbp,
    (cast(sip.舒张压最小值 as FLOAT) + cast(sip.舒张压最大值 as FLOAT))/2 avg_dbp,
    (cast(sip.收缩压最小值 as FLOAT) - cast(sip.舒张压最小值 as FLOAT))/3 + cast(sip.舒张压最小值 as FLOAT) min_mbp,
    (cast(sip.收缩压最大值 as FLOAT) - cast(sip.舒张压最大值 as FLOAT))/3 + cast(sip.舒张压最大值 as FLOAT) max_mbp,
    ((cast(sip.收缩压最小值 as FLOAT) - cast(sip.舒张压最小值 as FLOAT))/3 + cast(sip.舒张压最小值 as FLOAT) +
     (cast(sip.收缩压最大值 as FLOAT) - cast(sip.舒张压最大值 as FLOAT))/3 + cast(sip.舒张压最大值 as FLOAT)) /2 avg_mbp,
    sip."SpO2最小值" min_spo2,
    sip."SpO2最大值" max_spo2,
    (cast(sip."SpO2最小值" as FLOAT) + cast(sip."SpO2最大值" as FLOAT))/2 avg_spo2,
    CASE
        WHEN sip.男性1 = '1' THEN 'M'
        ELSE 'F'
    END AS gender,
    CASE WHEN REPLACE(sip."PT_min", '测不出', '') = '' THEN NULL ELSE REPLACE(sip."PT_min", '测不出', '')::float END pt_min,
    CASE WHEN REPLACE(sip."PT_max", '测不出', '') = '' THEN NULL ELSE REPLACE(sip."PT_max", '测不出', '')::float END pt_max,
    (CASE WHEN REPLACE(sip."PT_min", '测不出', '') = '' THEN NULL ELSE REPLACE(sip."PT_min", '测不出', '')::float END +
     CASE WHEN REPLACE(sip."PT_max", '测不出', '') = '' THEN NULL ELSE REPLACE(sip."PT_max", '测不出', '')::float END)/2 pt_avg,

    CASE WHEN REPLACE(REPLACE(sip."INR_min", '测不出', ''),'-','') = '' THEN NULL ELSE REPLACE(REPLACE(sip."INR_min", '测不出', ''),'-','')::float END inr_min,
    CASE WHEN REPLACE(sip."INR_max", '测不出', '') = '' THEN NULL ELSE REPLACE(sip."INR_max", '测不出', '')::float END inr_max,
    (CASE WHEN REPLACE(REPLACE(sip."INR_min", '测不出', ''),'-','') = '' THEN NULL ELSE REPLACE(REPLACE(sip."INR_min", '测不出', ''),'-','')::float END +
     CASE WHEN REPLACE(sip."INR_max", '测不出', '') = '' THEN NULL ELSE REPLACE(sip."INR_max", '测不出', '')::float END)/2 inr_avg,
    CASE
        WHEN cast(sip.死亡1 as FLOAT) = 1 THEN 't'
        ELSE 'f'
    END AS death28,
    CASE
        WHEN sip."sepsis_aki" = '1' THEN 't'
        WHEN sip."sepsis_aki" = '0' THEN 'f'
    END AS sepsis_aki,
--     sip."sepsis_aki" sepsis_aki,
    CASE
        WHEN sip."sepsis_li" = '1' THEN 't'
        WHEN sip."sepsis_li" = '0' THEN 'f'
    END AS sepsis_li,
--     sip."sepsis_li" sepsis_li,
    CASE
        WHEN sip."sepsis_co" = '1' THEN 't'
        WHEN sip."sepsis_co" = '0' THEN 'f'
    END AS sepsis_co,
--     sip."sepsis_co" sepsis_co,
    CASE
        WHEN sip."sepsis_ali" = '1' THEN 't'
        WHEN sip."sepsis_ali" = '0' THEN 'f'
    END AS sepsis_ali
--     sip."sepsis_ali" sepsis_ali
FROM
    self_icu_patient sip
LEFT JOIN self_icu_xueqi x ON sip.住院号 = x.住院号
