import pandas as pd
import numpy as np
import joblib
import lightgbm as lgb
import xgboost as xgb
import catboost as cb
from datetime import datetime

# =======================
# 1. Загрузка моделей и метаданных
# =======================
def load_all_models_and_meta(prefix="retail_sales_"):
    """
    Загружает обученные модели, веса ансамбля, список признаков и список категориальных признаков.
    Все файлы должны быть созданы и сохранены при обучении!
    """
    lgb_model = joblib.load(f"{prefix}lgb_model.pkl")
    xgb_model = joblib.load(f"{prefix}xgb_model.pkl")
    cb_model = joblib.load(f"{prefix}cb_model.pkl")
    ensemble_weights = joblib.load(f"{prefix}ensemble_weights.pkl")  # tuple/list (w_lgb, w_xgb, w_cb)
    feature_list = joblib.load(f"{prefix}feature_list.pkl")  # Список фичей (колонки X_train)
    cat_features = joblib.load(f"{prefix}cat_features.pkl")  # Список категориальных признаков
    return lgb_model, xgb_model, cb_model, ensemble_weights, feature_list, cat_features

# =======================
# 2. Препроцессинг одного примера для предсказания
# =======================
def prepare_features_for_predict(user_input: dict, feature_list, cat_features, reference_df=None):
    """
    user_input: словарь {имя_признака: значение}, например {'SKU': '12345', 'Магазин': '1', 'Дата': '2025-05-10', ...}
    feature_list: список всех признаков, используемых в модели
    cat_features: список категориальных признаков (имена)
    reference_df: DataFrame с историей для генерации лагов и rolling (если нужно)
    """
    df = pd.DataFrame([user_input]).copy()
    # Приведение типов
    for col in cat_features:
        if col in df.columns:
            df[col] = df[col].astype("category")
    # Если даты есть в признаках — преобразуем
    if 'Дата' in df.columns:
        df['Дата'] = pd.to_datetime(df['Дата'])
    # Добавляем отсутствующие признаки нулями/дефолтами
    for col in feature_list:
        if col not in df.columns:
            df[col] = 0
    # Сохраняем порядок признаков как при обучении модели
    df = df[feature_list]
    return df

def encode_cats_for_xgb(df, cat_features):
    """
    Для XGBoost: категориальные признаки кодируем в int.
    """
    df = df.copy()
    for col in cat_features:
        if col in df.columns:
            df[col] = df[col].astype('category').cat.codes
    return df

def inverse_target_transform(y_pred):
    """
    Обратное логарифмическое преобразование для целевой переменной, если использовалось log1p.
    """
    return np.expm1(y_pred)

# =======================
# 3. Функция предсказания
# =======================
def predict_sales(user_input: dict, lgb_model, xgb_model, cb_model, ensemble_weights, feature_list, cat_features):
    """
    Выполняет предсказание продаж для одного примера по всем моделям и ансамблю.
    user_input: словарь с фичами
    """
    # Преобразуем вход к DataFrame
    X = prepare_features_for_predict(user_input, feature_list, cat_features)
    # LightGBM
    lgb_pred = lgb_model.predict(X)
    lgb_pred = inverse_target_transform(lgb_pred)
    # XGBoost
    X_xgb = encode_cats_for_xgb(X, cat_features)
    dmatrix = xgb.DMatrix(X_xgb)
    xgb_pred = xgb_model.predict(dmatrix)
    xgb_pred = inverse_target_transform(xgb_pred)
    # CatBoost
    cb_pred = cb_model.predict(X)
    cb_pred = inverse_target_transform(cb_pred)
    # Ансамбль
    w_lgb, w_xgb, w_cb = ensemble_weights
    ensemble_pred = w_lgb * lgb_pred + w_xgb * xgb_pred + w_cb * cb_pred
    return {
        "LightGBM": float(lgb_pred[0]),
        "XGBoost": float(xgb_pred[0]),
        "CatBoost": float(cb_pred[0]),
        "Ensemble": float(ensemble_pred[0])
    }

# =======================
# 4. Пример использования: консольный ввод
# =======================
def main():
    print("==== ПРОГНОЗ ПРОДАЖ ПО ОДНОМУ ТОВАРУ ====")
    # Загрузка моделей и признаков
    lgb_model, xgb_model, cb_model, ensemble_weights, feature_list, cat_features = load_all_models_and_meta()
    # Пример диалога с пользователем
    print("Введите значения признаков для прогноза.")
    user_input = {
        'SKU': '369314',
        'Магазин': 'E14',
        'Дата': '2025-04-11'
    }
    # Можно добавить дополнительные признаки по надобности
    # Например:
    # user_input["Тип_акции"] = input("Тип_акции: ")
    # user_input["Весовой"] = input("Весовой (0/1): ")
    # и т.д.
    # Прогноз
    result = predict_sales(user_input, lgb_model, xgb_model, cb_model, ensemble_weights, feature_list, cat_features)
    print("\n--- Результаты прогноза ---")
    print(f"LightGBM:  {result['LightGBM']:.3f}")
    print(f"XGBoost:   {result['XGBoost']:.3f}")
    print(f"CatBoost:  {result['CatBoost']:.3f}")
    print(f"АНСАМБЛЬ:  {result['Ensemble']:.3f}")

if __name__ == "__main__":
    main()
