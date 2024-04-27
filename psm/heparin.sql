DROP MATERIALIZED VIEW IF EXISTS itemid_exclude CASCADE;
CREATE MATERIALIZED VIEW itemid_exclude AS
SELECT
    subject_id,
    hadm_id,
    stay_id,
    json_agg(itemid) itemids
FROM
    (
        SELECT
            ali.subject_id,
            ali.hadm_id,
            ali.stay_id,
            ii.itemid
        FROM
            ( cal.sepsis_ali ali LEFT JOIN mimiciv_icu.inputevents ii ON ( ( ali.stay_id = ii.stay_id ) ) )
        WHERE
            (
                        ii.itemid = 225152
                    OR ii.itemid = 229597
                    OR ii.itemid = 230044
                    OR ii.itemid = 225906
                    OR ii.itemid = 225913
                    OR ii.itemid = 225906
                    OR ii.itemid = 225147
                    OR ii.itemid = 225908
                )
        GROUP BY
            ali.subject_id,
            ali.hadm_id,
            ali.stay_id,
            ii.itemid
        UNION
        SELECT
            ali.subject_id,
            ali.hadm_id,
            ali.stay_id,
            ic.itemid
        FROM
            ( cal.sepsis_ali ali LEFT JOIN mimiciv_icu.chartevents ic ON ( ( ali.stay_id = ic.stay_id ) ) )
        WHERE
            ( ic.itemid = 224145 OR ic.itemid = 225958 )
        GROUP BY
            ali.subject_id,
            ali.hadm_id,
            ali.stay_id,
            ic.itemid
    ) union_exclude
GROUP BY
    subject_id,
    hadm_id,
    stay_id


SELECT
    ali.*,
    dose.itemid_225975_dose,
    excl.itemids
FROM
    sepsis_ali ali
        LEFT JOIN itemid_225975_dose dose ON dose.stay_id = ali.stay_id
        LEFT JOIN itemid_exclude excl ON excl.stay_id = ali.stay_id



DROP MATERIALIZED VIEW IF EXISTS itemid_225975_dose CASCADE;
CREATE MATERIALIZED VIEW itemid_225975_dose AS
SELECT ali.subject_id,
       ali.hadm_id,
       ali.stay_id,
       sum(ii.amount) AS itemid_225975_dose
FROM (cal.sepsis_ali ali
    LEFT JOIN mimiciv_icu.inputevents ii ON ((ali.stay_id = ii.stay_id)))
WHERE (ii.itemid = 225975)
GROUP BY ali.subject_id, ali.hadm_id, ali.stay_id


SELECT ali.subject_id,
       ali.hadm_id,
       ali.stay_id,
       sum(ic.valuenum) AS itemid_224145_unit
FROM (cal.sepsis_ali ali
    LEFT JOIN mimiciv_icu.chartevents ic ON ((ali.stay_id = ic.stay_id)))
WHERE (ic.itemid = 224145)
GROUP BY ali.subject_id, ali.hadm_id, ali.stay_id


SELECT ali.subject_id,
       ali.hadm_id,
       ali.stay_id,
       sum(ii.amount) AS itemid_225152_unit
FROM (cal.sepsis_ali ali
    LEFT JOIN mimiciv_icu.inputevents ii ON ((ali.stay_id = ii.stay_id)))
WHERE (ii.itemid = 225152)
GROUP BY ali.subject_id, ali.hadm_id, ali.stay_id
