import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.neighbors import NearestNeighbors

data = pd.read_excel('heparin_225975.xlsx')

data = data[[
    'subject_id',
    'hadm_id',
    'stay_id',
    'po2_avg',
    'pco2_avg',
    'ph_avg',
    'bicarbonate_avg',
    'pao2fio2ratio_avg',
    'wbc_avg',
    'lymphocytes_abs_avg',
    'hemoglobin_avg',
    'albumin_avg',
    'lactate_max',
    'charlson_comorbidity_index_max',
    'platelet_count_avg',
    'alanine_aminotransferase_avg',
    'bilirubin_total_avg',
    'creatinine_avg',
    'glucose_avg',
    'avg_heart_rate',
    'avg_mbp',
    'avg_resp_rate',
    'avg_temperature',
    'pt_avg',
    'age',
    'gender',
    'itemid_225975_dose',
    'itemids',
    'per_day'
]]

data = data[data['per_day'].isna() | (data['per_day'] > 2) ]

data['gender'].replace({'M': 1, 'F': 0}, inplace=True)
data['per_day_gt1'] = data['per_day'].apply(lambda x: x if pd.notna(x) else 0)
# data = data.dropna()

treatment = data['per_day_gt1']
X = data.drop(['subject_id', 'hadm_id', 'stay_id', 'itemid_225975_dose', 'itemids', 'per_day', 'per_day_gt1'], axis=1)

# 逻辑回归模型估计倾向得分
model = LogisticRegression()
model.fit(X, round(treatment))
data['propensity_score'] = model.predict_proba(X)[:, 1]

treated = data[data['per_day_gt1'] > 0]
control = data[data['per_day'].isna()]

# 初始化最近邻模型，用于查找最接近的倾向得分
nn = NearestNeighbors(n_neighbors=1, metric='euclidean')
nn.fit(control[['propensity_score']])

# 为每个处理组成员找到最接近的控制组成员
treated_index = treated.index
control_index = nn.kneighbors(treated[['propensity_score']], return_distance=False).flatten()

# 创建匹配的DataFrame
matched_data = pd.concat([treated.reset_index(drop=True), control.iloc[control_index].reset_index(drop=True)], axis=1)

matched_data.to_excel('imputer_psm_20240420_B12.xlsx', index=False, engine='openpyxl')
print(data)
