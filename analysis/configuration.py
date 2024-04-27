import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = os.path.join(BASE_DIR, 'data')
OUTPUT_DIR = os.path.join(BASE_DIR, 'result')

INPUT_EXCEL = {
    "file_name": os.path.join(DATA_DIR, 'sepsis_ali.xlsx'),
    "start_column": 'po2_min',
    "end_column": 'inr_avg',
    "extra_column": ['subject_id', 'hadm_id', 'stay_id']
}

VERIFY_EXCEL = {
    "file_name": os.path.join(DATA_DIR, 'self_icu_data.xlsx'),
    "start_column": 'po2_min',
    "end_column": 'inr_avg',
    "extra_column": ['name']
}

########################################################################################################################
# True: 内部验证, False: 外部验证
VERIFY_TYPE = True
TRAIN_RATIO = 0.8
THRESHOLD = 0.95
MAX_COLUMN_NUMBER = 10
PREDICT_TARGET = 'sepsis_ali'
TARGET_COLUMNS = ['alp_max', 'hematocrit_min', 'min_heart_rate', 'min_mbp', 'urea_nitrogen_min', 'po2_max', 'avg_sbp', 'alp_min', 'platelet_count_min', 'glucose_min', 'max_sbp', 'platelet_count_max', 'glucose_max', 'pao2fio2ratio_max', 'pao2fio2ratio_min', 'avg_dbp', 'baseexcess_min', 'platelet_count_avg', 'creatine_kinase_isoenzyme_max', 'pao2fio2ratio_avg']


########################################################################################################################

LEGAL_COLUMNS = \
    ['min_dbp', 'alp_min', 'pt_min', 'pco2_min', 'alp_max', 'avg_mbp', 'min_heart_rate', 'albumin_min',
     'potassium_avg', 'avg_spo2', 'glucose_avg', 'monocytes_abs_avg', 'hematocrit_avg', 'platelet_count_max',
     'chloride_avg', 'anion_gap_max', 'max_spo2', 'lymphocytes_abs_min', 'avg_dbp', 'pt_avg', 'hematocrit_min',
     'min_mbp', 'avg_heart_rate', 'baseexcess_max', 'sodium_avg', 'baseexcess_min',
     'creatine_kinase_isoenzyme_avg', 'anion_gap_avg', 'min_resp_rate', 'creatinine_min', 'wbc_avg', 'min_spo2',
     'pt_max', 'monocytes_abs_min', 'hemoglobin_avg', 'sodium_max', 'platelet_count_min', 'monocytes_abs_max',
     'pao2fio2ratio_max', 'pco2_avg', 'ph_min', 'alanine_aminotransferase_min', 'neutrophils_abs_avg',
     'baseexcess_avg', 'ph_avg', 'monocytes_max', 'bilirubin_total_max', 'alanine_aminotransferase_max',
     'urea_nitrogen_max', 'ph_max', 'pco2_max', 'bilirubin_total_min', 'po2_max', 'avg_sbp', 'glucose_min',
     'urea_nitrogen_min', 'albumin_max', 'max_sbp', 'bilirubin_total_avg', 'inr_min', 'wbc_max', 'chloride_min',
     'urea_nitrogen_avg', 'creatine_kinase_isoenzyme_min', 'lymphocytes_abs_avg', 'max_dbp', 'po2_avg',
     'creatinine_avg', 'min_sbp', 'potassium_min', 'lymphocytes_abs_max', 'neutrophils_abs_min', 'max_resp_rate',
     'urineoutput_max', 'hematocrit_max', 'anion_gap_min', 'neutrophils_max', 'lymphocytes_avg', 'calcium_min',
     'monocytes_min', 'max_heart_rate', 'calcium_max', 'hemoglobin_max', 'po2_min', 'alp_avg',
     'pao2fio2ratio_min', 'creatine_kinase_isoenzyme_max', 'lymphocytes_max', 'sodium_min', 'neutrophils_avg',
     'max_mbp', 'wbc_min', 'max_temperature', 'neutrophils_abs_max', 'hemoglobin_min', 'gender',
     'alanine_aminotransferase_avg', 'potassium_max', 'neutrophils_min', 'creatinine_max', 'albumin_avg',
     'pao2fio2ratio_avg', 'avg_resp_rate', 'lymphocytes_min', 'monocytes_avg', 'calcium_avg',
     'platelet_count_avg', 'chloride_max', 'glucose_max', 'avg_temperature', 'min_temperature', 'lactate_max',
     'lactate_min', 'lactate_avg', 'charlson_comorbidity_index_max']
