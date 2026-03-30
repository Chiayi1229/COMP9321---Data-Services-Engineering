import pandas as pd
import numpy as np
import sys
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import mean_absolute_error, f1_score
import xgboost as xgb

np.random.seed(42)

train_csv = sys.argv[1]
test_csv = sys.argv[2]

train_df = pd.read_csv(train_csv)
test_df = pd.read_csv(test_csv)

def preprocess(df, have_train=True):
    df = df.copy()

    df['date_sold'] = pd.to_datetime(df['date_sold'], errors='coerce')
    df['year_sold'] = df['date_sold'].dt.year
    df['month_sold'] = df['date_sold'].dt.month
    df['day_sold'] = df['date_sold'].dt.day
    df['quarter'] = df['date_sold'].dt.quarter
    df['day_of_week'] = df['date_sold'].dt.dayofweek
    weekend_flags = []
  
    for item in df['day_of_week']:
        if item >= 5:
            weekend_flags.append(1)
        else:
            weekend_flags.append(0)

    df['weekend'] = weekend_flags

    def get_season(month):
        if month in [12, 1, 2]:
            return 1
        elif month in [3, 4, 5]:
            return 2
        elif month in [6, 7, 8]:
            return 3
        elif month in [9, 10, 11]:
            return 4

    df['season'] = df['month_sold'].apply(get_season)


    if 'num_bed' in df.columns and 'num_bath' in df.columns:
        df['bed_bath_ratio'] = df['num_bed'] / df['num_bath'].replace(0, 1)
        df['total_rooms'] = df['num_bed'] + df['num_bath']
        df['bath_per_bedroom'] = df['num_bath'] / df['num_bed'].replace(0, 1)

    if 'num_bed' in df.columns and 'property_size' in df.columns:
        df['land_per_bedroom'] = df['property_size'] / df['num_bed'].replace(0, 1)

    if 'num_park' in df.columns and 'num_bed' in df.columns and 'num_bath' in df.columns:
        total_rooms = df['num_bed'] + df['num_bath']
        df['park_per_room'] = df['num_park'] / total_rooms.replace(0, 1)

    if 'ethnic_breakdown' in df.columns:
        all_ethnicities = set()

        for data in df['ethnic_breakdown']:
            language = str(data).split(',')
            for lang in language:
                seperate = lang.strip().split(' ')
                if len(seperate) >= 2:
                    ethnicity = ' '.join(seperate[:-1])
                    all_ethnicities.add(ethnicity)

        all_ethnicities_cleaned = []
        for item in all_ethnicities:
            clean_data = item.lower().strip()
            all_ethnicities_cleaned.append(clean_data)

        all_ethnicities=all_ethnicities_cleaned
        for item in all_ethnicities:
            df[f'{item}_percentage'] = 0.0

        for index, data in df['ethnic_breakdown'].items():
            language = str(data).split(',')
            for lang in language:
                seperate = lang.strip().split(' ')
                if len(seperate) >= 2:
                    ethnicity = ' '.join(seperate[:-1]).lower().strip()
                    try:
                        percentage = float(seperate[-1].replace('%', '').replace(',', '.'))
                    except:
                        percentage = 0.0
                    col_name = f'{ethnicity}_percentage'
                    if col_name in df.columns:
                        df.at[index, col_name] = percentage

        max_ethnic = []

        for data in df.columns:
            if data.endswith('_percentage'):
                max_ethnic.append(data)
        if max_ethnic:
            df[max_ethnic] = df[max_ethnic].apply(pd.to_numeric, errors='coerce')
            df['max_ethnic_percentage'] = df[max_ethnic].max(axis=1)


    extended_area_cols = [
        'traffic', 'public_transport', 'affordability_rental', 'affordability_buying',
        'nature', 'noise', 'things_to_see_do', 'family_friendliness', 'pet_friendliness', 'safety'
    ]

    all_columns_exist = True
    for item in extended_area_cols:
        if item not in df.columns:
            all_columns_exist = False

    if all_columns_exist:
        rental_score = df['affordability_rental']
        buying_score = df['affordability_buying']
        df['affordability_index'] = (rental_score + buying_score) / 2
        traffic_score = df['traffic']
        public_transport_score = df['public_transport']
        df['transport_convenience'] = (traffic_score + public_transport_score) / 2
        nature_score = df['nature']
        things_to_see_do_score = df['things_to_see_do']
        family_friendly_score = df['family_friendliness']
        pet_friendly_score = df['pet_friendliness']
        safety_score = df['safety']
        lifestyle_positive = (
            nature_score +
            things_to_see_do_score +
            family_friendly_score +
            pet_friendly_score +
            safety_score
        ) / 5

        noise_score = df['noise']

        df['lifestyle_index'] = (lifestyle_positive - noise_score) / 2

        df['lifestyle_score_total'] = (
            nature_score +
            things_to_see_do_score +
            family_friendly_score +
            pet_friendly_score +
            safety_score
        )

    object_columns = df.select_dtypes(include='object').columns

    for item in object_columns:
        values = df[item].astype(str).tolist()
        encoder = LabelEncoder()
        encoder.fit(values)
        encoded_values = encoder.transform(values)
        df[item] = encoded_values

    drop_columns = [
        'id',
        'price', 'type',
        'date_sold',
        'ethnic_breakdown',
        'suburb',
        'nearest_train_station',
        'highlights_attractions',
        'ideal_for'
    ]

    all_to_drop = []
    for item in drop_columns:
        if item in df.columns:
            all_to_drop.append(item)

    df = df.drop(columns=all_to_drop)

    return df


train_process = preprocess(train_df, have_train=True)
test_process = preprocess(test_df, have_train=False)

feature = list(set(train_process.columns) & set(test_process.columns))
train_process = train_process[feature]
test_process = test_process[feature]

X = train_process
y_price = train_df['price']
y_type = train_df['type']

label_type = LabelEncoder()
label_type.fit(y_type)
y_type_encoded = label_type.transform(y_type)

X_train = X
y_price_train_raw = y_price
y_price_train = np.log1p(y_price)
y_type_train = y_type_encoded

X_value = test_process
y_price_value_raw = test_df['price']
y_price_value = np.log1p(y_price_value_raw)
y_type_value = label_type.transform(test_df['type'])

reg_model = xgb.XGBRegressor(
    n_estimators=400,
    learning_rate=0.02,
    max_depth=8,
    min_child_weight=3,
    gamma=0.05,
    subsample=0.8,
    colsample_bytree=0.85,
    reg_alpha=0.3,
    reg_lambda=1.2,
    random_state=42,
    n_jobs=1,
    seed=42, 
    eval_metric='mae',
)

reg_model.fit(
    X_train, y_price_train,
    eval_set=[(X_value, y_price_value)],
    verbose=False
)

y_price_predict_log = reg_model.predict(X_value)
y_price_predict = np.expm1(y_price_predict_log)
mae = mean_absolute_error(y_price_value_raw, y_price_predict)
print(f"Validation MAE: {mae:.2f}")

class_model = xgb.XGBClassifier(
    n_estimators=500,
    learning_rate=0.04,
    max_depth=8,
    min_child_weight=1,
    gamma=0.1,
    subsample=0.85,
    colsample_bytree=0.8,
    reg_alpha=0.1,
    reg_lambda=0.5,
    objective='multi:softprob',
    use_label_encoder=False,
    random_state=42,
    n_jobs=1,
    seed=42, 
    eval_metric='mlogloss',
)


class_model.fit(
    X_train, y_type_train,
    eval_set=[(X_value, y_type_value)],
    verbose=False
)

y_type_pred = class_model.predict(X_value)
f1 = f1_score(y_type_value, y_type_pred, average='weighted')
print(f"Validation F1 Score: {f1:.3f}")

test_id = test_df['id']
test_data = test_process

test_data = test_data[feature]

price_predict_log = reg_model.predict(test_data)
price_predict = np.expm1(price_predict_log)
type_predict = class_model.predict(test_data)
type_predict = label_type.inverse_transform(type_predict)

zid = "z5467129"

pd.DataFrame({'id': test_id, 'price': price_predict}).to_csv(f'{zid}.regression.csv', index=False)
pd.DataFrame({'id': test_id, 'type': type_predict}).to_csv(f'{zid}.classification.csv', index=False)