import pandas as pd
import numpy as np
import lightgbm as lgb
import xgboost as xgb
import catboost as cb
import joblib
import gc
import optuna
from datetime import datetime, timedelta
import warnings
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.preprocessing import StandardScaler, PowerTransformer
from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import Pipeline
from concurrent.futures import ProcessPoolExecutor
from xgboost.callback import EarlyStopping
import multiprocessing

# Отключение предупреждений для более чистого вывода
warnings.filterwarnings('ignore')

def clean_column_to_int(series, false_values=('Нет', 'False', 'false', '-', '', None), true_values=('Да', 'True', 'true')):
    """
    Универсально очищает и преобразует строковую/смесь колонку к int (0/1).
    false_values — любые эквиваленты "нет"/0
    true_values  — любые эквиваленты "да"/1
    """
    ser = series.copy()
    # Приведение строк к нижнему регистру для унификации
    ser = ser.astype(str).str.strip().str.lower()
    # Замена значений на 0 (false) или 1 (true)
    ser = ser.replace(list(false_values), 0, regex=False)
    ser = ser.replace(list(true_values), 1, regex=False)
    # Явное преобразование типов
    ser = ser.infer_objects(copy=False)  # Убирает предупреждение
    # Преобразование к числовому типу
    ser = pd.to_numeric(ser, errors='coerce').fillna(0).astype('int8')
    return ser

# ================================================
# 1. Загрузка и начальная обработка данных
# ================================================
def load_data():
    print("DEBUG: Начало загрузки данных")

    sales = pd.read_csv('data/sales.csv', parse_dates=['Дата'])
    returns = pd.read_csv('data/returns.csv', parse_dates=['Дата_возврата'])
    promotions = pd.read_csv('data/promotions.csv', parse_dates=['Дата_начала', 'Дата_окончания'], dayfirst=True)
    holidays = pd.read_csv('data/holidays.csv', parse_dates=['Дата'])
    
    ###############################################################################
    sales = sales.head(100_000)  # Ограничить размер для теста
    ###############################################################################

    # Если есть информация о категориях товаров - загрузим её
    try:
        products = pd.read_csv('data/products.csv')
        has_product_info = True
    except:
        has_product_info = False
        print("DEBUG: Информация о товарах не найдена")

    promotions['Это_уценка'] = clean_column_to_int(promotions['Это_уценка'])

    # Фильтрация возвратов по существующим продажам
    valid_guids = set(sales['GUID_продажи'])
    returns = returns[returns['GUID_продажи'].isin(valid_guids)].copy()
    returns_agg = returns.groupby(['GUID_продажи', 'SKU', 'Магазин']).agg(
        Количество_возвращено=('Количество_возвращено', 'sum')
    ).reset_index()

    # Объединение продаж с возвратами
    sales = pd.merge(sales, returns_agg, on=['GUID_продажи', 'SKU', 'Магазин'], how='left')
    sales['Количество_возвращено'] = sales['Количество_возвращено'].fillna(0)
    sales['Чистые_продажи'] = sales['Количество'] - sales['Количество_возвращено']

    # Сумма чека и сертификата
    sales['Сумма_чека'] = sales.groupby('GUID_продажи')['Цена_со_скидкой'].transform('sum')
    negative_prices = sales[sales['Цена_со_скидкой'] < 0].groupby('GUID_продажи')['Цена_со_скидкой'].sum()
    sales['Сумма_сертификата'] = sales['GUID_продажи'].map(negative_prices).fillna(0).abs()

    # Информация о праздниках
    sales = pd.merge(sales, holidays[['Дата', 'Название_праздника', 'Тип_праздника', 'Выходной']], on='Дата', how='left')
    sales['Праздник'] = sales['Название_праздника'].notnull().astype('int8')
    sales['Праздник_тип'] = sales['Тип_праздника'].fillna('Нет')
    sales['Выходной_день'] = sales['Выходной'].fillna(0).astype('int8')
    sales.drop(columns=['Название_праздника', 'Тип_праздника', 'Выходной'], inplace=True)

    # Объединение с акциями
    sales = pd.merge(sales, promotions, on='Номер_акции', how='left')
    sales['Акция_активна'] = (
        (sales['Номер_акции'] != 0)
        & (sales['Дата'] >= sales['Дата_начала']) 
        & (sales['Дата'] <= sales['Дата_окончания'])
    ).astype('int8')
    sales['Тип_акции'] = sales['Тип_акции'].fillna('Нет акции')
    sales['Процент_скидки'] = sales['Процент_скидки'].fillna(0)
    
    if sales['Это_уценка'].isna().any():
        print("Пропуски найдены в 'Это_уценка', они будут заполнены 0.")
    sales['Это_уценка'] = sales['Это_уценка'].fillna(0).astype('int8')
    
    sales['Промо_код_применён'] = sales['Промо_код'].notnull().astype('int8')
    sales.drop(columns=['Дата_начала', 'Дата_окончания', 'Промо_код'], inplace=True)

    # Добавление информации о товарах, если она доступна
    if has_product_info:
        sales = pd.merge(sales, products, on='SKU', how='left')
    
    # Сортировка и оптимизация типов
    sales = sales.sort_values(by=['SKU', 'Магазин', 'Дата'])
    sales = sales.astype({col: 'float32' for col in sales.select_dtypes('float64').columns})
    sales = sales.astype({col: 'int32' for col in sales.select_dtypes('int64').columns if col not in ['Магазин', 'SKU']})
    sales['SKU'] = sales['SKU'].astype(str).astype('category')  # SKU как категория
    sales.drop(columns=['GUID_продажи'], inplace=True, errors='ignore')

    print("DEBUG: Данные загружены. Размер DataFrame:", sales.shape)
    return sales, holidays, promotions

# ================================================
# 2. Новые и расширенные признаки (feature engineering)
# ================================================
def create_price_features(df):
    """Создание признаков связанных с ценами"""
    # Базовые ценовые признаки
    df['Скидка_фактическая'] = 1 - (df['Цена_со_скидкой'] / df['Цена_без_скидки'].replace(0, np.nan))
    df['Скидка_фактическая'] = df['Скидка_фактическая'].fillna(0).clip(0, 1).astype('float32')
    df['Была_ли_скидка'] = (df['Цена_со_скидкой'] < df['Цена_без_скидки']).astype('int8')
    
    # Относительное положение цены товара в магазине
    df['Цена_относительно_среднего'] = df['Цена_со_скидкой'] / df.groupby(['SKU'])['Цена_со_скидкой'].transform('mean')
    df['Цена_относительно_среднего'] = df['Цена_относительно_среднего'].fillna(1).astype('float32')
    
    # Изменение цены по сравнению с предыдущим периодом
    df['Цена_изменение'] = df.groupby(['SKU', 'Магазин'])['Цена_со_скидкой'].pct_change().fillna(0).astype('float32')
    
    # Разница между ценой товара и средней ценой по магазину
    df['Цена_отклонение_от_магазина'] = (
        df['Цена_со_скидкой'] - df.groupby(['Магазин', 'Дата'])['Цена_со_скидкой'].transform('mean')
    ).astype('float32')
    
    return df

def create_promotion_features(df, promotions_df):
    """Расширенные признаки акций"""
    # Акции и уценки (категориальные)
    promo_typemap = promotions_df.set_index('Номер_акции')['Тип_акции'].to_dict()
    clearance_map = promotions_df.set_index('Номер_акции')['Это_уценка'].to_dict()
    df['Тип_акции_расширенный'] = df['Номер_акции'].map(promo_typemap).fillna('Нет акции')
    df['Является_уценкой'] = df['Номер_акции'].map(clearance_map).fillna(0).astype('int8')
    
    # Количество активных акций на данную дату в магазине
    df['Кол_акций_в_магазине'] = df.groupby(['Магазин', 'Дата'])['Акция_активна'].transform('sum').astype('int16')
    
    # Среднее количество акций на товар за последний месяц
    df['Акций_за_30д_товар'] = df.groupby(['SKU'])['Акция_активна'].transform(
        lambda x: x.rolling(30, min_periods=1).mean()
    ).astype('float32')
    
    # Средняя скидка по этому товару
    df['Скидка_средняя_товар'] = df.groupby(['SKU'])['Скидка_фактическая'].transform('mean').astype('float32')
    
    # История акций на товар (был ли товар на акции недавно)
    df['Был_на_акции_7д'] = df.groupby(['SKU', 'Магазин'])['Акция_активна'].transform(
        lambda x: x.rolling(7, min_periods=1).max()
    ).shift(1).fillna(0).astype('int8')
    
    # Признаки промо-эффективности - как продажи реагируют на акции
    df['Продажи_на_акции'] = df.groupby(['SKU', 'Магазин', 'Акция_активна'])['Чистые_продажи'].transform('mean')
    df['Эффективность_акции'] = df['Продажи_на_акции'] / df['Продажи_на_акции'].where(~df['Акция_активна'].astype(bool), np.nan)
    df['Эффективность_акции'] = df['Эффективность_акции'].fillna(1).replace([np.inf, -np.inf], 1).astype('float32')
    
    return df

def create_time_features(df):
    """Создание временных признаков"""
    # Базовые временные признаки
    df['День_недели'] = df['Дата'].dt.dayofweek.astype('int8')
    df['Месяц'] = df['Дата'].dt.month.astype('int8')
    df['Год'] = df['Дата'].dt.year.astype('int16')
    df['Выходной'] = (df['День_недели'] >= 5).astype('int8')
    df['Дни_с_начала'] = (df['Дата'] - df['Дата'].min()).dt.days.astype('int32')
    df['День_года'] = df['Дата'].dt.dayofyear.astype('int16')
    df['Неделя_года'] = df['Дата'].dt.isocalendar().week.astype('int8')
    df['Квартал'] = df['Дата'].dt.quarter.astype('int8')
    
    # Циклические признаки времени
    df['Sin_День'] = np.sin(2 * np.pi * df['День_года'] / 365).astype('float32')
    df['Cos_День'] = np.cos(2 * np.pi * df['День_года'] / 365).astype('float32')
    df['Sin_Неделя'] = np.sin(2 * np.pi * df['День_недели'] / 7).astype('float32')
    df['Cos_Неделя'] = np.cos(2 * np.pi * df['День_недели'] / 7).astype('float32')
    df['Sin_Месяц'] = np.sin(2 * np.pi * df['Месяц'] / 12).astype('float32')
    df['Cos_Месяц'] = np.cos(2 * np.pi * df['Месяц'] / 12).astype('float32')
    
    # День месяца и его "особенности" (начало/конец месяца)
    df['День_месяца'] = df['Дата'].dt.day.astype('int8')
    df['Начало_месяца'] = (df['День_месяца'] <= 5).astype('int8')
    df['Конец_месяца'] = (df['День_месяца'] >= 25).astype('int8')
    
    # Признаки для дат зарплат (15 и 30 числа +/- 1 день)
    df['Зарплатный_день'] = (
        ((df['День_месяца'] >= 14) & (df['День_месяца'] <= 16)) | 
        ((df['День_месяца'] >= 29) | (df['День_месяца'] <= 1))
    ).astype('int8')
    
    return df

def add_holiday_features(df, holidays_df):
    """Добавление признаков связанных с праздниками"""
    # Создаем множество праздничных дат для быстрого поиска
    holidays_set = set(holidays_df['Дата'].dt.date)
    
    # Дни до ближайшего праздника
    df['Дней_до_праздника'] = df['Дата'].apply(
        lambda x: min([(h - x.date()).days for h in holidays_set if h >= x.date()] + [999])
    ).astype('int16')
    
    # Дни после ближайшего прошедшего праздника
    df['Дней_после_праздника'] = df['Дата'].apply(
        lambda x: min([(x.date() - h).days for h in holidays_set if h <= x.date()] + [999])
    ).astype('int16')
    
    # Признаки по типам праздников (если они есть в данных)
    if 'Тип_праздника' in holidays_df.columns:
        holiday_types = holidays_df['Тип_праздника'].dropna().unique()
        for h_type in holiday_types:
            specific_holidays = set(holidays_df[holidays_df['Тип_праздника'] == h_type]['Дата'].dt.date)
            
            # Дни до ближайшего праздника этого типа
            df[f'Дней_до_{h_type}'] = df['Дата'].apply(
                lambda x: min([(h - x.date()).days for h in specific_holidays if h >= x.date()] + [999])
            ).astype('int16')
            
    # Флаг для сезона распродаж (ноябрь-декабрь)
    df['Сезон_распродаж'] = ((df['Месяц'] == 11) | (df['Месяц'] == 12)).astype('int8')
    
    return df

def create_lags_vectorized(df, lags=[1, 2, 3, 7, 14, 21, 30, 60, 90], target_col='Чистые_продажи'):
    """Создание лаговых признаков"""
    print(f"DEBUG: Создание {len(lags)} лаговых признаков")
    group = df.groupby(['SKU', 'Магазин'], observed=True)[target_col]
    
    for lag in lags:
        df[f'Lag_{lag}'] = group.shift(lag)
        # Заполнение пропусков медианой по SKU
        df[f'Lag_{lag}'] = df[f'Lag_{lag}'].fillna(df.groupby('SKU')[target_col].transform('median'))
        df[f'Lag_{lag}'] = df[f'Lag_{lag}'].astype('float32')
    
    return df

def create_rolling_vectorized(df, windows=[3, 7, 14, 30, 90], target_col='Чистые_продажи'):
    """Создание признаков скользящих средних"""
    print(f"DEBUG: Создание скользящих средних для {len(windows)} окон")
    df = df.sort_values(by=['SKU', 'Магазин', 'Дата'])
    group = df.groupby(['SKU', 'Магазин'], observed=True)[target_col]
    
    for window in windows:
        # Скользящее среднее
        df[f'MA_{window}'] = group.transform(lambda x: x.rolling(window, min_periods=1).mean()).astype('float32')
        
        # Скользящая медиана (более устойчива к выбросам)
        df[f'Median_{window}'] = group.transform(lambda x: x.rolling(window, min_periods=1).median()).astype('float32')
        
        # Максимум за период
        df[f'Max_{window}'] = group.transform(lambda x: x.rolling(window, min_periods=1).max()).astype('float32')
        
        # Минимум за период
        df[f'Min_{window}'] = group.transform(lambda x: x.rolling(window, min_periods=1).min()).astype('float32')
        
        # Стандартное отклонение (волатильность продаж)
        df[f'Std_{window}'] = group.transform(lambda x: x.rolling(window, min_periods=1).std()).fillna(0).astype('float32')
    
    return df

def create_advanced_volume_features(df):
    """Создание признаков для весовых и штучных товаров"""
    # Флаг весового товара и его преобразование
    df['Весовой'] = df['Весовой'].astype('int8')
    
    # Среднее кол-во продаж по весовым и штучным товарам раздельно
    df['Среднее_по_весовой_группе'] = df.groupby(['Весовой', 'Магазин'])['Чистые_продажи'].transform('mean').astype('float32')
    
    # Отношение продаж к среднему в своей группе (весовые/штучные)
    df['Отношение_к_среднему_группы'] = (df['Чистые_продажи'] / df['Среднее_по_весовой_группе']).fillna(1).astype('float32')
    
    # Отдельно рассчитываем статистики для весовых и штучных
    for is_weighted in [0, 1]:
        subset = df[df['Весовой'] == is_weighted]
        if subset.empty:
            continue
            
        weight_type = 'весовой' if is_weighted else 'штучный'
        
        # Средняя цена в группе
        avg_price = subset.groupby(['Магазин'])['Цена_со_скидкой'].transform('mean')
        df.loc[df['Весовой'] == is_weighted, f'Цена_отн_средней_{weight_type}'] = (
            subset['Цена_со_скидкой'] / avg_price
        ).astype('float32')
        
        # Квантили продаж в группе
        q75 = subset.groupby(['Магазин'])['Чистые_продажи'].transform(lambda x: x.quantile(0.75))
        q25 = subset.groupby(['Магазин'])['Чистые_продажи'].transform(lambda x: x.quantile(0.25))
        df.loc[df['Весовой'] == is_weighted, f'Продажи_квантиль_{weight_type}'] = (
            (subset['Чистые_продажи'] - q25) / (q75 - q25).replace(0, 1)
        ).fillna(0.5).clip(0, 1).astype('float32')
    
    # Заполняем пропуски образовавшиеся в процессе
    for col in df.columns:
        if df[col].isna().any():
            if df[col].dtype == 'float32' or df[col].dtype == 'float64':
                df[col] = df[col].fillna(0).astype('float32')
    
    return df

def create_store_features(df):
    """Создание признаков на основе магазина"""
    # Общая активность магазина (средний объем продаж)
    df['Активность_магазина'] = df.groupby(['Магазин', 'Дата'])['Чистые_продажи'].transform('sum').astype('float32')
    
    # Ранг магазина по продажам (нормализованный от 0 до 1)
    store_ranks = df.groupby('Магазин')['Чистые_продажи'].mean().rank(pct=True)
    df['Ранг_магазина'] = df['Магазин'].map(store_ranks).astype('float32')
    
    # Доля товара в общих продажах магазина за день
    daily_store_sales = df.groupby(['Магазин', 'Дата'])['Чистые_продажи'].transform('sum')
    df['Доля_в_магазине'] = (df['Чистые_продажи'] / daily_store_sales).fillna(0).astype('float32')
    
    # Продажи товара относительно среднего по этому товару во всех магазинах на эту дату
    df['Продажи_относительно_среднего'] = (
        df['Чистые_продажи'] / df.groupby(['SKU', 'Дата'])['Чистые_продажи'].transform('mean')
    ).fillna(1).replace([np.inf, -np.inf], 1).astype('float32')
    
    return df

def create_cross_features(df):
    """Создание кросс-признаков между разными сущностями"""
    # Взаимодействие SKU и Магазина - уникальные комбинации
    df['SKU_Магазин'] = df['SKU'].astype(str) + "_" + df['Магазин'].astype(str)
    df['SKU_Магазин'] = df['SKU_Магазин'].astype('category')
    
    # Взаимодействие времени и типа товара
    df['День_недели_Весовой'] = df['День_недели'].astype(str) + "_" + df['Весовой'].astype(str)
    df['День_недели_Весовой'] = df['День_недели_Весовой'].astype('category')
    
    # Взаимодействие акций и типа товара
    df['Акция_Весовой'] = df['Акция_активна'].astype(str) + "_" + df['Весовой'].astype(str)
    df['Акция_Весовой'] = df['Акция_Весовой'].astype('category')
    
    # Взаимодействие времени и акций
    df['Выходной_Акция'] = df['Выходной'].astype(str) + "_" + df['Акция_активна'].astype(str)
    df['Выходной_Акция'] = df['Выходной_Акция'].astype('category')
    
    return df

def compute_trends(df):
    """Создание признаков трендов продаж"""
    # Тренд между последним известным значением и скользящим средним
    df['Trend_1_7'] = (df['Lag_1'] - df['MA_7']).astype('float32')
    df['Trend_7_30'] = (df['MA_7'] - df['MA_30']).astype('float32')
    
    # Тренд за последнюю неделю (наклон линии тренда)
    df['Trend_slope_7'] = df.groupby(['SKU', 'Магазин'])['Чистые_продажи'].transform(
        lambda x: (x.rolling(7, min_periods=3).apply(
            lambda y: np.nan if len(y) < 3 else np.polyfit(np.arange(len(y)), y, 1)[0], raw=True)
        )
    ).fillna(0).astype('float32')
    
    # Ускорение/замедление продаж (вторая производная)
    df['Acceleration_7'] = df.groupby(['SKU', 'Магазин'])['Trend_slope_7'].diff().fillna(0).astype('float32')
    
    # Изменение относительно того же периода в прошлом году (сезонность)
    df['YoY_change'] = (
        df['Чистые_продажи'] / df.groupby(['SKU', 'Магазин', 'День_года'])['Чистые_продажи'].shift(1)
    ).fillna(1).replace([np.inf, -np.inf], 1).astype('float32')
    
    return df

def create_target_encodings(df, target_col='Чистые_продажи'):
    """Создание таргет-энкодингов для категориальных признаков"""
    # Список категориальных признаков для энкодинга
    cat_features = ['SKU', 'Магазин', 'Тип_акции', 'День_недели', 'Месяц', 'Весовой']
    
    # Сплит на train/test для предотвращения утечки данных
    split_date = df['Дата'].max() - pd.Timedelta(days=30)
    train_mask = df['Дата'] < split_date
    
    for feature in cat_features:
        if feature in df.columns:
            # Вычисляем средние значения целевой переменной для каждой категории
            encoding_map = df[train_mask].groupby(feature)[target_col].mean().to_dict()
            
            # Применяем энкодинг
            df[f'{feature}_target_mean'] = df[feature].map(encoding_map).astype('float32')
            
            # Для тестовых данных, где могут быть новые категории, заполняем пропуски средним
            df[f'{feature}_target_mean'].fillna(df[train_mask][target_col].mean(), inplace=True)
    
    return df

def transform_target_variable(df, target_col='Чистые_продажи'):
    """Трансформация целевой переменной для улучшения распределения"""
    # Ограничение выбросов
    upper_limit = df[target_col].quantile(0.995)
    df[target_col] = df[target_col].clip(0, upper_limit)
    
    # Логарифмическое преобразование (log1p для обработки нулей)
    df['log_Чистые_продажи'] = np.log1p(df[target_col]).astype('float32')
    
    # Box-Cox преобразование для штучных товаров, где это имеет смысл
    try:
        non_zero_mask = (df[target_col] > 0) & (df['Весовой'] == 0)
        if non_zero_mask.sum() > 100:  # Достаточное количество ненулевых значений
            pt = PowerTransformer(method='box-cox')
            transformed = pt.fit_transform(df.loc[non_zero_mask, target_col].values.reshape(-1, 1))
            df.loc[non_zero_mask, 'boxcox_Чистые_продажи'] = transformed.flatten()
            df['boxcox_Чистые_продажи'] = df['boxcox_Чистые_продажи'].fillna(0).astype('float32')
    except Exception as e:
        print(f"Не удалось применить Box-Cox преобразование: {e}")
        df['boxcox_Чистые_продажи'] = df['log_Чистые_продажи']
    
    return df

def feature_engineering(sales_df, holidays_df, promotions_df):
    """Комплексное создание признаков"""
    print("DEBUG: Начало создания признаков")
    
    # Базовые ценовые признаки
    sales_df = create_price_features(sales_df)
    
    # Временные признаки
    sales_df = create_time_features(sales_df)
    
    # Признаки связанные с акциями
    sales_df = create_promotion_features(sales_df, promotions_df)
    
    # Признаки связанные с праздниками
    sales_df = add_holiday_features(sales_df, holidays_df)
    
    # Признаки для весовых и штучных товаров
    sales_df = create_advanced_volume_features(sales_df)
    
    # Признаки магазинов
    sales_df = create_store_features(sales_df)
    
    # Лаговые признаки
    sales_df = create_lags_vectorized(sales_df)
    
    # Скользящие средние и другие статистики
    sales_df = create_rolling_vectorized(sales_df)
    
    # Признаки трендов
    sales_df = compute_trends(sales_df)
    
    # Кросс-признаки
    sales_df = create_cross_features(sales_df)
    
    # Target encoding
    sales_df = create_target_encodings(sales_df)
    
    # Трансформация целевой переменной
    sales_df = transform_target_variable(sales_df)
    
    print("DEBUG: Завершение создания признаков. Размер DataFrame:", sales_df.shape)
    
    return sales_df

# ================================================
# 3. Подготовка данных для обучения модели
# ================================================
def prepare_train_test_data(df, test_size_days=30, target_col='Чистые_продажи', transformed_target='log_Чистые_продажи'):
    """Подготовка тренировочных и тестовых данных"""
    print("DEBUG: Подготовка данных для модели")
    
    # Сортировка по дате
    df = df.sort_values('Дата')
    
    # Разделение на тренировочные и тестовые по последним датам
    split_date = df['Дата'].max() - pd.Timedelta(days=test_size_days)
    train_df = df[df['Дата'] <= split_date].copy()
    test_df = df[df['Дата'] > split_date].copy()
    
    print(f"DEBUG: Размер тренировочных данных: {train_df.shape}, тестовых: {test_df.shape}")
    
    # Исключаем колонки, которые не должны быть в признаках
    exclude_cols = ['Дата', target_col, 'log_Чистые_продажи', 'boxcox_Чистые_продажи']
    feature_cols = [col for col in df.columns if col not in exclude_cols]
    
    # Для категориальных признаков создаем список
    cat_features = df.select_dtypes(include=['category']).columns.tolist()
    
    # Подготовка данных для обучения
    X_train = train_df[feature_cols]
    y_train = train_df[transformed_target]
    
    X_test = test_df[feature_cols]
    y_test = test_df[transformed_target]

    for df in [X_train, X_test]:
        for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].astype('category')
    
    # Сохраняем оригинальные значения для обратного преобразования
    y_test_original = test_df[target_col]
    
    print("DEBUG: Подготовка данных завершена")
    
    return X_train, y_train, X_test, y_test, y_test_original, cat_features, train_df, test_df

# ================================================
# 4. Функции для оптимизации гиперпараметров
# ================================================
def optimize_lightgbm(X_train, y_train, X_test, y_test, cat_features=None, n_trials=50):
    """Оптимизация гиперпараметров LightGBM с использованием Optuna"""
    print("DEBUG: Начало оптимизации LightGBM")
    
    # Функция для обратного преобразования предсказаний
    def inverse_transform(y_pred):
        return np.expm1(y_pred)
    
    # Функция для вычисления RMSE
    def rmse(y_true, y_pred):
        return np.sqrt(mean_squared_error(y_true, y_pred))
    
    # Функция для вычисления MAPE
    def mape(y_true, y_pred):
        mask = y_true != 0
        return np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100
    
    # Функция для вычисления среднего абсолютного процентного отклонения
    def mean_absolute_percentage_deviation(y_true, y_pred):
        return np.mean(np.abs((y_true - y_pred) / (y_true + 1e-7))) * 100
    
    # Функция для оптимизации
    def objective(trial):
        # Параметры для оптимизации
        params = {
            'boosting_type': 'gbdt',
            'objective': 'regression',
            'metric': 'rmse',
            'verbosity': -1,
            'num_leaves': trial.suggest_int('num_leaves', 20, 150),
            'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.1, log=True),
            'feature_fraction': trial.suggest_float('feature_fraction', 0.5, 1.0),
            'bagging_fraction': trial.suggest_float('bagging_fraction', 0.5, 1.0),
            'bagging_freq': trial.suggest_int('bagging_freq', 1, 10),
            'min_child_samples': trial.suggest_int('min_child_samples', 5, 100),
            'lambda_l1': trial.suggest_float('lambda_l1', 1e-8, 10.0, log=True),
            'lambda_l2': trial.suggest_float('lambda_l2', 1e-8, 10.0, log=True),
            'n_jobs': 60,
            'random_state': 42
        }
        
        # Кросс-валидация с учетом временных рядов
        tscv = TimeSeriesSplit(n_splits=5)
        scores = []
        
        for train_idx, valid_idx in tscv.split(X_train):
            X_train_fold, X_valid_fold = X_train.iloc[train_idx], X_train.iloc[valid_idx]
            y_train_fold, y_valid_fold = y_train.iloc[train_idx], y_train.iloc[valid_idx]
            
            # Создание датасета LightGBM
            train_data = lgb.Dataset(
                X_train_fold, 
                label=y_train_fold, 
                categorical_feature=cat_features if cat_features else 'auto'
            )
            valid_data = lgb.Dataset(
                X_valid_fold, 
                label=y_valid_fold, 
                categorical_feature=cat_features if cat_features else 'auto'
            )
            
            # Обучение модели
            model = lgb.train(
                params,
                train_data,
                valid_sets=[valid_data],
                num_boost_round=10000,
                callbacks=[
                    lgb.early_stopping(100),
                    lgb.log_evaluation(period=100)]
            )
            
            # Предсказание и обратная трансформация
            valid_pred = model.predict(X_valid_fold)
            valid_pred = inverse_transform(valid_pred)
            valid_true = inverse_transform(y_valid_fold)
            
            # Расчет метрик
            fold_rmse = rmse(valid_true, valid_pred)
            scores.append(fold_rmse)
        
        print("DEBUG: optimize_lightgbm завершена, возвращаем модель")
        # Возвращаем среднее значение метрики для всех фолдов
        return np.mean(scores)
    
    # Создание Optuna исследования для оптимизации
    study = optuna.create_study(direction='minimize')

    study.optimize(objective, n_trials=n_trials)

    # Лучшие параметры
    best_params = study.best_params
    print(f"DEBUG: Лучшие параметры LightGBM: {best_params}")
    
    # Создание финальной модели с лучшими параметрами
    best_params['boosting_type'] = 'gbdt'
    best_params['objective'] = 'regression'
    best_params['metric'] = 'rmse'
    best_params['verbosity'] = -1
    best_params['n_jobs'] = -1
    best_params['random_state'] = 42
    
    # Создание датасета
    train_data = lgb.Dataset(
        X_train, 
        label=y_train, 
        categorical_feature=cat_features if cat_features else 'auto'
    )
    
    # Обучение финальной модели
    final_model = lgb.train(
        best_params,
        train_data,
        num_boost_round=10000,
        callbacks=[lgb.log_evaluation(period=100)]
    )
    
    # Предсказание и оценка на тестовом наборе
    test_pred = final_model.predict(X_test)
    test_pred_inv = inverse_transform(test_pred)
    test_true_inv = inverse_transform(y_test)
    
    # Метрики
    test_rmse = rmse(test_true_inv, test_pred_inv)
    test_mae = mean_absolute_error(test_true_inv, test_pred_inv)
    test_mapd = mean_absolute_percentage_deviation(test_true_inv, test_pred_inv)
    
    print(f"DEBUG: LightGBM Test RMSE: {test_rmse:.4f}")
    print(f"DEBUG: LightGBM Test MAE: {test_mae:.4f}")
    print(f"DEBUG: LightGBM Test MAPD: {test_mapd:.4f}%")
    
    return final_model, best_params, test_pred_inv

def optimize_xgboost(X_train, y_train, X_test, y_test, n_trials=50):
    """Оптимизация гиперпараметров XGBoost с использованием Optuna"""
    print("DEBUG: Начало оптимизации XGBoost")
    
    # Кодировка категориальных признаков
    def encode_cats(df):
        df = df.copy()
        for col in df.select_dtypes(include='category').columns:
            df[col] = df[col].cat.codes
        return df
    
    X_train_enc = encode_cats(X_train)
    X_test_enc = encode_cats(X_test)

    # Функция для обратного преобразования предсказаний
    def inverse_transform(y_pred):
        return np.expm1(y_pred)
    
    # Функция для вычисления RMSE
    def rmse(y_true, y_pred):
        return np.sqrt(mean_squared_error(y_true, y_pred))
    
    # Функция для оптимизации
    def objective(trial):
        # Параметры для оптимизации
        params = {
            'objective': 'reg:squarederror',
            'eval_metric': 'rmse',
            'verbosity': 0,
            'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.1, log=True),
            'max_depth': trial.suggest_int('max_depth', 3, 10),
            'subsample': trial.suggest_float('subsample', 0.5, 1.0),
            'colsample_bytree': trial.suggest_float('colsample_bytree', 0.5, 1.0),
            'min_child_weight': trial.suggest_int('min_child_weight', 1, 20),
            'alpha': trial.suggest_float('alpha', 1e-8, 1.0, log=True),
            'lambda': trial.suggest_float('lambda', 1e-8, 1.0, log=True),
            'gamma': trial.suggest_float('gamma', 1e-8, 1.0, log=True),
            'n_jobs': 60,
            'random_state': 42
        }
        
        # Кросс-валидация с учетом временных рядов
        tscv = TimeSeriesSplit(n_splits=5)
        scores = []
        
        for train_idx, valid_idx in tscv.split(X_train_enc):
            X_train_fold, X_valid_fold = X_train_enc.iloc[train_idx], X_train_enc.iloc[valid_idx]
            y_train_fold, y_valid_fold = y_train.iloc[train_idx], y_train.iloc[valid_idx]
            
            # Создание DMatrix для XGBoost
            dtrain = xgb.DMatrix(X_train_fold, label=y_train_fold)
            dvalid = xgb.DMatrix(X_valid_fold, label=y_valid_fold)
            
            # Обучение модели
            model = xgb.train(
                params,
                dtrain,
                num_boost_round=10000,
                evals=[(dvalid, 'validation')],
                callbacks=[EarlyStopping(rounds=100)],
                verbose_eval=False
            )
            
            # Предсказание и обратная трансформация
            valid_pred = model.predict(dvalid)
            valid_pred = inverse_transform(valid_pred)
            valid_true = inverse_transform(y_valid_fold)
            
            # Расчет метрик
            fold_rmse = rmse(valid_true, valid_pred)
            scores.append(fold_rmse)
        
        # Возвращаем среднее значение метрики для всех фолдов
        return np.mean(scores)
    
    # Создание Optuna исследования для оптимизации
    study = optuna.create_study(direction='minimize')
    study.optimize(objective, n_trials=n_trials)
    
    # Лучшие параметры
    best_params = study.best_params
    print(f"DEBUG: Лучшие параметры XGBoost: {best_params}")
    
    # Создание финальной модели с лучшими параметрами
    best_params['objective'] = 'reg:squarederror'
    best_params['eval_metric'] = 'rmse'
    best_params['verbosity'] = 0
    best_params['n_jobs'] = -1
    best_params['random_state'] = 42
    
    # Создание датасета
    dtrain = xgb.DMatrix(X_train_enc, label=y_train)
    dtest = xgb.DMatrix(X_test_enc, label=y_test)
    
    # Обучение финальной модели
    final_model = xgb.train(
        best_params,
        dtrain,
        num_boost_round=10000,
        evals=[(dtest, 'test')],
        callbacks=[EarlyStopping(100)],
        verbose_eval=100
    )
    
    # Предсказание и оценка на тестовом наборе
    test_pred = final_model.predict(dtest)
    test_pred_inv = inverse_transform(test_pred)
    test_true_inv = inverse_transform(y_test)
    
    # Метрики
    test_rmse = rmse(test_true_inv, test_pred_inv)
    test_mae = mean_absolute_error(test_true_inv, test_pred_inv)
    test_mapd = np.mean(np.abs((test_true_inv - test_pred_inv) / (test_true_inv + 1e-7))) * 100
    
    print(f"DEBUG: XGBoost Test RMSE: {test_rmse:.4f}")
    print(f"DEBUG: XGBoost Test MAE: {test_mae:.4f}")
    print(f"DEBUG: XGBoost Test MAPD: {test_mapd:.4f}%")
    
    return final_model, best_params, test_pred_inv

def optimize_catboost(X_train, y_train, X_test, y_test, cat_features=None, n_trials=50):
    """Оптимизация гиперпараметров CatBoost с использованием Optuna"""
    print("DEBUG: Начало оптимизации CatBoost")

    # Функция для обратного преобразования предсказаний
    def inverse_transform(y_pred):
        return np.expm1(y_pred)

    # Функция для вычисления RMSE
    def rmse(y_true, y_pred):
        return np.sqrt(mean_squared_error(y_true, y_pred))

    # Функция для оптимизации
    def objective(trial):
        params = {
            'loss_function': 'RMSE',
            'eval_metric': 'RMSE',
            'verbose': 0,
            'iterations': 10000,
            'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.1, log=True),
            'depth': trial.suggest_int('depth', 4, 10),
            'l2_leaf_reg': trial.suggest_float('l2_leaf_reg', 1, 100, log=True),
            'bootstrap_type': trial.suggest_categorical('bootstrap_type', ['Bayesian', 'Bernoulli', 'MVS']),
            'random_seed': 42,
            'allow_writing_files': False,
            'task_type': 'CPU',
            'thread_count': 60
        }

        if params['bootstrap_type'] == 'Bayesian':
            params['bagging_temperature'] = trial.suggest_float('bagging_temperature', 0, 10)
        elif params['bootstrap_type'] == 'Bernoulli':
            params['subsample'] = trial.suggest_float('subsample', 0.5, 1)

        # Кросс-валидация с учетом временных рядов
        tscv = TimeSeriesSplit(n_splits=5)
        scores = []

        for train_idx, valid_idx in tscv.split(X_train):
            X_train_fold, X_valid_fold = X_train.iloc[train_idx], X_train.iloc[valid_idx]
            y_train_fold, y_valid_fold = y_train.iloc[train_idx], y_train.iloc[valid_idx]

            # Получаем список всех категориальных колонок для этого разбиения
            cat_features_fold = X_train_fold.select_dtypes(include='category').columns.tolist()

            train_pool = cb.Pool(X_train_fold, label=y_train_fold, cat_features=cat_features_fold)
            valid_pool = cb.Pool(X_valid_fold, label=y_valid_fold, cat_features=cat_features_fold)

            model = cb.CatBoostRegressor(**params)
            model.fit(
                train_pool,
                eval_set=valid_pool,
                early_stopping_rounds=100,
                verbose=0
            )

            valid_pred = model.predict(X_valid_fold)
            valid_pred = inverse_transform(valid_pred)
            valid_true = inverse_transform(y_valid_fold)

            fold_rmse = rmse(valid_true, valid_pred)
            scores.append(fold_rmse)

        return np.mean(scores)

    # Создание Optuna исследования для оптимизации
    study = optuna.create_study(direction='minimize')
    study.optimize(objective, n_trials=n_trials)

    best_params = study.best_params
    print(f"DEBUG: Лучшие параметры CatBoost: {best_params}")

    # Создание финальной модели с лучшими параметрами
    best_params['loss_function'] = 'RMSE'
    best_params['eval_metric'] = 'RMSE'
    best_params['verbose'] = 100
    best_params['iterations'] = 10000
    best_params['random_seed'] = 42
    best_params['allow_writing_files'] = False
    best_params['task_type'] = 'CPU'

    # Получаем список всех категориальных признаков для train/test
    final_cat_features = X_train.select_dtypes(include='category').columns.tolist()

    train_pool = cb.Pool(X_train, label=y_train, cat_features=final_cat_features)
    test_pool = cb.Pool(X_test, label=y_test, cat_features=final_cat_features)

    final_model = cb.CatBoostRegressor(**best_params)
    final_model.fit(
        train_pool,
        eval_set=test_pool,
        early_stopping_rounds=100,
        verbose=100
    )

    test_pred = final_model.predict(X_test)
    test_pred_inv = inverse_transform(test_pred)
    test_true_inv = inverse_transform(y_test)

    test_rmse = rmse(test_true_inv, test_pred_inv)
    test_mae = mean_absolute_error(test_true_inv, test_pred_inv)
    test_mapd = np.mean(np.abs((test_true_inv - test_pred_inv) / (test_true_inv + 1e-7))) * 100

    print(f"DEBUG: CatBoost Test RMSE: {test_rmse:.4f}")
    print(f"DEBUG: CatBoost Test MAE: {test_mae:.4f}")
    print(f"DEBUG: CatBoost Test MAPD: {test_mapd:.4f}%")

    return final_model, best_params, test_pred_inv

# ================================================
# 5. Ансамблирование моделей для улучшения точности
# ================================================
def create_ensemble(X_train, y_train, X_test, y_test, cat_features=None, n_trials=30):
    """Создание ансамбля моделей"""
    print("DEBUG: Создание ансамбля моделей")
    
    # Оптимизация и обучение отдельных моделей
    print("DEBUG: Начало оптимизации LightGBM")
    lgb_model, lgb_params, lgb_pred = optimize_lightgbm(X_train, y_train, X_test, y_test, cat_features, n_trials)
    gc.collect()
    print("DEBUG: LightGBM завершен, начинаем XGBoost")
    xgb_model, xgb_params, xgb_pred = optimize_xgboost(X_train, y_train, X_test, y_test, n_trials)
    gc.collect()
    print("DEBUG: XGBoost завершен, начинаем CatBoost")
    cb_model, cb_params, cb_pred = optimize_catboost(X_train, y_train, X_test, y_test, cat_features, n_trials)
    gc.collect()
    print("DEBUG: CatBoost завершен, начинаем ансамблирование")
    
    # Функция для обратного преобразования предсказаний
    def inverse_transform(y_pred):
        return np.expm1(y_pred)
    
    # Находим оптимальные веса для ансамбля
    def find_optimal_weights():
        # Определяем диапазон весов для поиска
        weights_range = np.linspace(0, 1, 11)
        best_rmse = float('inf')
        best_weights = (1/3, 1/3, 1/3)  # Равные веса по умолчанию
        
        # Перебираем всевозможные комбинации весов
        for w1 in weights_range:
            for w2 in weights_range:
                if w1 + w2 <= 1:  # Проверка, что веса в сумме не превышают 1
                    w3 = 1 - w1 - w2
                    
                    # Считаем взвешенный ансамбль
                    ensemble_pred = w1 * lgb_pred + w2 * xgb_pred + w3 * cb_pred
                    true_values = inverse_transform(y_test)
                    
                    # Оцениваем RMSE
                    current_rmse = np.sqrt(mean_squared_error(true_values, ensemble_pred))
                    
                    if current_rmse < best_rmse:
                        best_rmse = current_rmse
                        best_weights = (w1, w2, w3)
        
        return best_weights
    
    # Находим оптимальные веса
    weights = find_optimal_weights()
    print(f"DEBUG: Оптимальные веса ансамбля: {weights}")
    
    # Создаем взвешенный ансамбль
    ensemble_pred = weights[0] * lgb_pred + weights[1] * xgb_pred + weights[2] * cb_pred
    
    # Оцениваем метрики ансамбля
    true_values = inverse_transform(y_test)
    ensemble_rmse = np.sqrt(mean_squared_error(true_values, ensemble_pred))
    ensemble_mae = mean_absolute_error(true_values, ensemble_pred)
    ensemble_mapd = np.mean(np.abs((true_values - ensemble_pred) / (true_values + 1e-7))) * 100
    
    print(f"DEBUG: Ensemble Test RMSE: {ensemble_rmse:.4f}")
    print(f"DEBUG: Ensemble Test MAE: {ensemble_mae:.4f}")
    print(f"DEBUG: Ensemble Test MAPD: {ensemble_mapd:.4f}%")
    
    # Сохраняем результаты в словарь
    ensemble_results = {
        'models': {
            'lgb': lgb_model,
            'xgb': xgb_model, 
            'cb': cb_model
        },
        'params': {
            'lgb': lgb_params,
            'xgb': xgb_params,
            'cb': cb_params
        },
        'weights': weights,
        'metrics': {
            'rmse': ensemble_rmse,
            'mae': ensemble_mae,
            'mapd': ensemble_mapd
        }
    }
    
    return ensemble_results, ensemble_pred

# ================================================
# 6. Функции для анализа и интерпретации моделей
# ================================================
def analyze_model_performance(ensemble_results, test_df, ensemble_pred, y_test_original):
    """Анализ производительности модели"""
    print("DEBUG: Анализ производительности модели")
    
    # Добавляем предсказания в тестовый датафрейм
    test_df['Предсказано'] = ensemble_pred
    
    # Анализ ошибок по типам товаров (весовой/штучный)
    if 'Весовой' in test_df.columns:
        print("\nАнализ по типам товаров:")
        for is_weight in [0, 1]:
            weight_type = "Весовые" if is_weight == 1 else "Штучные"
            subset = test_df[test_df['Весовой'] == is_weight]
            
            if not subset.empty:
                rmse = np.sqrt(mean_squared_error(subset['Чистые_продажи'], subset['Предсказано']))
                mae = mean_absolute_error(subset['Чистые_продажи'], subset['Предсказано'])
                mapd = np.mean(np.abs((subset['Чистые_продажи'] - subset['Предсказано']) / 
                                     (subset['Чистые_продажи'] + 1e-7))) * 100
                
                print(f"{weight_type}: RMSE={rmse:.4f}, MAE={mae:.4f}, MAPD={mapd:.4f}%")
    
    # Анализ ошибок по наличию акций
    if 'Акция_активна' in test_df.columns:
        print("\nАнализ по акциям:")
        for promo in [0, 1]:
            promo_type = "С акцией" if promo == 1 else "Без акции"
            subset = test_df[test_df['Акция_активна'] == promo]
            
            if not subset.empty:
                rmse = np.sqrt(mean_squared_error(subset['Чистые_продажи'], subset['Предсказано']))
                mae = mean_absolute_error(subset['Чистые_продажи'], subset['Предсказано'])
                mapd = np.mean(np.abs((subset['Чистые_продажи'] - subset['Предсказано']) / 
                                    (subset['Чистые_продажи'] + 1e-7))) * 100
                
                print(f"{promo_type}: RMSE={rmse:.4f}, MAE={mae:.4f}, MAPD={mapd:.4f}%")
    
    # Анализ ошибок по дням недели
    if 'День_недели' in test_df.columns:
        print("\nАнализ по дням недели:")
        weekday_errors = test_df.groupby('День_недели').apply(
            lambda x: pd.Series({
                'RMSE': np.sqrt(mean_squared_error(x['Чистые_продажи'], x['Предсказано'])),
                'MAE': mean_absolute_error(x['Чистые_продажи'], x['Предсказано']),
                'MAPD': np.mean(np.abs((x['Чистые_продажи'] - x['Предсказано']) / 
                                        (x['Чистые_продажи'] + 1e-7))) * 100
            })
        )
        print(weekday_errors)
    
    # Анализ 10 товаров с наибольшей и наименьшей ошибкой
    test_df['Абс_ошибка'] = np.abs(test_df['Чистые_продажи'] - test_df['Предсказано'])
    test_df['Отн_ошибка'] = np.abs((test_df['Чистые_продажи'] - test_df['Предсказано']) / 
                                  (test_df['Чистые_продажи'] + 1e-7)) * 100
    
    print("\nТоп-10 товаров с наибольшей абсолютной ошибкой:")
    worst_items = test_df.groupby('SKU')['Абс_ошибка'].mean().sort_values(ascending=False).head(10)
    print(worst_items)
    
    print("\nТоп-10 товаров с наименьшей абсолютной ошибкой:")
    best_items = test_df.groupby('SKU')['Абс_ошибка'].mean().sort_values().head(10)
    print(best_items)
    
    # Визуализация предсказаний vs фактических значений (если запускается в интерактивном режиме)
    try:
        import matplotlib.pyplot as plt
        plt.figure(figsize=(12, 6))
        plt.scatter(test_df['Чистые_продажи'], test_df['Предсказано'], alpha=0.3)
        plt.plot([0, test_df['Чистые_продажи'].max()], [0, test_df['Чистые_продажи'].max()], 'r--')
        plt.xlabel('Фактические продажи')
        plt.ylabel('Предсказанные продажи')
        plt.title('Предсказанные vs Фактические продажи')
        plt.savefig('prediction_vs_actual.png')
        print("\nГрафик сохранен в файл 'prediction_vs_actual.png'")
    except Exception as e:
        print(f"Не удалось создать визуализацию: {e}")
        
    return test_df

def feature_importance_analysis(ensemble_results, X_test):
    """Анализ важности признаков"""
    print("DEBUG: Анализ важности признаков")
    
    # Получение важности признаков из моделей
    importances = {}
    
    # LightGBM
    if 'lgb' in ensemble_results['models']:
        lgb_importance = pd.Series(
            ensemble_results['models']['lgb'].feature_importance(importance_type='gain'),
            index=X_test.columns
        ).sort_values(ascending=False)
        importances['LightGBM'] = lgb_importance
    
    # XGBoost
    if 'xgb' in ensemble_results['models']:
        xgb_importance = pd.Series(
            ensemble_results['models']['xgb'].get_score(importance_type='gain'),
            index=[f for f in X_test.columns if f in ensemble_results['models']['xgb'].get_score()]
        ).sort_values(ascending=False)
        importances['XGBoost'] = xgb_importance
    
    # CatBoost
    if 'cb' in ensemble_results['models']:
        cb_importance = pd.Series(
            ensemble_results['models']['cb'].get_feature_importance(),
            index=X_test.columns
        ).sort_values(ascending=False)
        importances['CatBoost'] = cb_importance
    
    # Вычисление среднего рейтинга важности признаков
    all_features = set()
    for imp in importances.values():
        all_features.update(imp.index)
    
    # Создаем DataFrame для хранения всех рейтингов
    importance_df = pd.DataFrame(index=sorted(all_features))  # сортировка для красоты
    
    # Заполняем DataFrame данными важности признаков из каждой модели
    for model_name, imp in importances.items():
        importance_df[f'{model_name}_Importance'] = imp
    
    # Заполняем пропуски нулями
    importance_df = importance_df.fillna(0)
    
    # Вычисляем средний рейтинг
    importance_df['Mean_Importance'] = importance_df.mean(axis=1)
    importance_df = importance_df.sort_values('Mean_Importance', ascending=False)
    
    # Вывод топ-20 важных признаков
    print("\nТоп-20 важных признаков:")
    print(importance_df.head(20))
    
    # Визуализация (если запускается в интерактивном режиме)
    try:
        import matplotlib.pyplot as plt
        plt.figure(figsize=(12, 10))
        importance_df['Mean_Importance'].head(20).sort_values().plot(kind='barh')
        plt.title('Топ-20 важных признаков')
        plt.savefig('feature_importance.png')
        print("\nГрафик важности признаков сохранен в файл 'feature_importance.png'")
    except Exception as e:
        print(f"Не удалось создать визуализацию важности признаков: {e}")
    
    return importance_df

def save_models(ensemble_results, file_prefix='retail_sales_'):
    """Сохранение обученных моделей"""
    print("DEBUG: Сохранение моделей")
    
    # Сохранение моделей
    for model_name, model in ensemble_results['models'].items():
        if model_name == 'lgb':
            joblib.dump(model, f"{file_prefix}lgb_model.pkl")
            #model.save_model(f"{file_prefix}lgb_model.txt")
        elif model_name == 'xgb':
            joblib.dump(model, f"{file_prefix}xgb_model.pkl")
            #model.save_model(f"{file_prefix}xgb_model.json")
        elif model_name == 'cb':
            joblib.dump(model, f"{file_prefix}cb_model.pkl")
            #model.save_model(f"{file_prefix}cb_model.cbm")
    
    # Сохранение параметров и весов ансамбля
    joblib.dump(ensemble_results['params'], f"{file_prefix}model_params.pkl")
    joblib.dump(ensemble_results['weights'], f"{file_prefix}ensemble_weights.pkl")

    # Сохранение списка признаков
    feature_list = X_train.columns.tolist()
    joblib.dump(feature_list, f"{file_prefix}feature_list.pkl")

    # Сохранение списка категориальных признаков
    cat_features = X_train.select_dtypes(include='category').columns.tolist()
    joblib.dump(cat_features, f"{file_prefix}cat_features.pkl")
    
    print(f"DEBUG: Модели сохранены с префиксом '{file_prefix}'")

def load_models(file_prefix='retail_sales_'):
    """Загрузка сохраненных моделей"""
    print("DEBUG: Загрузка моделей")
    
    ensemble_results = {'models': {}, 'params': {}, 'weights': None}
    
    # Загрузка параметров и весов ансамбля
    try:
        ensemble_results['params'] = joblib.load(f"{file_prefix}model_params.pkl")
        ensemble_results['weights'] = joblib.load(f"{file_prefix}ensemble_weights.pkl")
        
        # Загрузка LightGBM модели
        lgb_model = lgb.Booster(model_file=f"{file_prefix}lgb_model.txt")
        ensemble_results['models']['lgb'] = lgb_model
        
        # Загрузка XGBoost модели
        xgb_model = xgb.Booster()
        xgb_model.load_model(f"{file_prefix}xgb_model.json")
        ensemble_results['models']['xgb'] = xgb_model
        
        # Загрузка CatBoost модели
        cb_model = cb.CatBoostRegressor()
        cb_model.load_model(f"{file_prefix}cb_model.cbm")
        ensemble_results['models']['cb'] = cb_model
        
        print("DEBUG: Модели успешно загружены")
        return ensemble_results
    
    except Exception as e:
        print(f"DEBUG: Ошибка при загрузке моделей: {e}")
        return None

def prepare_data_for_prediction(data, holidays_df, promotions_df):
    """Подготовка данных для прогнозирования"""
    print("DEBUG: Подготовка данных для прогнозирования")
    
    # Применяем все те же преобразования, что и при обучении
    data = create_price_features(data)
    data = create_time_features(data)
    data = create_promotion_features(data, promotions_df)
    data = add_holiday_features(data, holidays_df)
    data = create_advanced_volume_features(data)
    data = create_store_features(data)
    data = create_lags_vectorized(data)
    data = create_rolling_vectorized(data)
    data = compute_trends(data)
    data = create_cross_features(data)
    
    # Target encoding не применяем, так как нам неизвестны будущие значения целевой переменной
    # Вместо этого используем средние значения из тренировочных данных
    
    print("DEBUG: Данные подготовлены для прогнозирования")
    return data

def predict_future_sales(ensemble_results, last_data, holidays_df, promotions_df, days_ahead=30):
    """Прогнозирование продаж на будущие даты"""
    print(f"DEBUG: Прогнозирование продаж на {days_ahead} дней вперед")
    
    # Копируем данные, чтобы не изменять оригинал
    future_data = last_data.copy()
    
    # Последняя дата в данных
    last_date = future_data['Дата'].max()
    
    # Список всех уникальных комбинаций SKU и Магазин
    sku_store_pairs = future_data[['SKU', 'Магазин']].drop_duplicates()
    
    # Создаем датафрейм с будущими датами для каждой пары SKU-Магазин
    future_dates = pd.DataFrame({
        'Дата': [last_date + pd.Timedelta(days=i+1) for i in range(days_ahead)]
    })
    
    # Кросс-соединение для получения всех комбинаций дат и пар SKU-Магазин
    future_df = pd.merge(
        sku_store_pairs,
        future_dates,
        how='cross'
    )
    
    # Копируем атрибуты товаров из последних известных данных
    product_attrs = future_data.drop_duplicates(subset=['SKU']).set_index('SKU')
    for col in product_attrs.columns:
        if col not in ['Дата', 'Чистые_продажи', 'Магазин'] and col in future_df.columns:
            future_df[col] = future_df['SKU'].map(product_attrs[col])
    
    # Объединяем исторические и будущие данные для корректного создания лаговых признаков
    combined_df = pd.concat([future_data, future_df], ignore_index=True)
    combined_df = combined_df.sort_values(['SKU', 'Магазин', 'Дата'])
    
    # Подготавливаем данные
    prepared_df = prepare_data_for_prediction(combined_df, holidays_df, promotions_df)
    
    # Извлекаем только будущие даты для прогноза
    future_df = prepared_df[prepared_df['Дата'] > last_date]
    
    # Получаем список признаков для прогноза (исключаем целевую переменную и связанные с ней)
    exclude_cols = ['Дата', 'Чистые_продажи', 'log_Чистые_продажи', 'boxcox_Чистые_продажи']
    feature_cols = [col for col in future_df.columns if col not in exclude_cols]
    
    # Прогнозирование с помощью каждой модели
    lgb_pred = ensemble_results['models']['lgb'].predict(future_df[feature_cols])
    xgb_pred = ensemble_results['models']['xgb'].predict(xgb.DMatrix(future_df[feature_cols]))
    cb_pred = ensemble_results['models']['cb'].predict(future_df[feature_cols])
    
    # Взвешенный ансамбль
    weights = ensemble_results['weights']
    ensemble_pred = weights[0] * lgb_pred + weights[1] * xgb_pred + weights[2] * cb_pred
    
    # Обратное преобразование логарифма
    future_df['Прогноз_продаж'] = np.expm1(ensemble_pred)
    
    # Округляем прогноз для штучных товаров
    if 'Весовой' in future_df.columns:
        future_df.loc[future_df['Весовой'] == 0, 'Прогноз_продаж'] = future_df.loc[future_df['Весовой'] == 0, 'Прогноз_продаж'].round()
    
    # Выбираем нужные колонки для результата
    result_df = future_df[['Дата', 'SKU', 'Магазин', 'Прогноз_продаж']]
    
    print("DEBUG: Прогноз выполнен")
    return result_df

def anomaly_detection(df, window=30, std_threshold=3.0):
    """Обнаружение аномалий в продажах"""
    print("DEBUG: Поиск аномалий в продажах")
    
    # Копируем данные
    anomalies_df = df.copy()
    
    # Вычисляем скользящее среднее и стандартное отклонение
    anomalies_df['MA'] = df.groupby(['SKU', 'Магазин'])['Чистые_продажи'].transform(
        lambda x: x.rolling(window=window, min_periods=5).mean())
    anomalies_df['STD'] = df.groupby(['SKU', 'Магазин'])['Чистые_продажи'].transform(
        lambda x: x.rolling(window=window, min_periods=5).std())
    
    # Вычисляем Z-score (стандартизованное отклонение)
    anomalies_df['Z_score'] = (anomalies_df['Чистые_продажи'] - anomalies_df['MA']) / anomalies_df['STD'].replace(0, 1)
    
    # Определяем аномалии
    anomalies_df['Is_anomaly'] = np.abs(anomalies_df['Z_score']) > std_threshold
    
    # Подсчет аномалий по товарам и магазинам
    anomaly_count = anomalies_df[anomalies_df['Is_anomaly']].groupby(['SKU', 'Магазин']).size().reset_index(name='anomaly_count')
    anomaly_count = anomaly_count.sort_values('anomaly_count', ascending=False)
    
    print(f"\nНайдено {anomalies_df['Is_anomaly'].sum()} аномальных значений.")
    print("\nТоп-10 товаров с наибольшим количеством аномалий:")
    print(anomaly_count.head(10))
    
    return anomalies_df[anomalies_df['Is_anomaly']]

def forecast_evaluation(test_df):
    """Оценка точности прогноза с разными метриками"""
    print("DEBUG: Расширенная оценка прогноза")
    
    # Основные метрики
    rmse = np.sqrt(mean_squared_error(test_df['Чистые_продажи'], test_df['Предсказано']))
    mae = mean_absolute_error(test_df['Чистые_продажи'], test_df['Предсказано'])
    
    # MAPE - средняя абсолютная процентная ошибка (только для ненулевых значений)
    non_zero = test_df['Чистые_продажи'] != 0
    mape = np.mean(np.abs((test_df.loc[non_zero, 'Чистые_продажи'] - test_df.loc[non_zero, 'Предсказано']) / 
                          test_df.loc[non_zero, 'Чистые_продажи'])) * 100
    
    # SMAPE - симметричная средняя абсолютная процентная ошибка
    smape = np.mean(2 * np.abs(test_df['Предсказано'] - test_df['Чистые_продажи']) / 
                   (np.abs(test_df['Чистые_продажи']) + np.abs(test_df['Предсказано']) + 1e-7)) * 100
    
    # Метрики по процентилям
    q_errors = {}
    for q in [0.5, 0.75, 0.9, 0.95, 0.99]:
        q_errors[f'Ошибка_{int(q*100)}_процентиль'] = np.abs(test_df['Чистые_продажи'] - test_df['Предсказано']).quantile(q)
    
    # Процент точных прогнозов (с определенным допуском)
    for tolerance in [0.1, 0.2, 0.3]:
        within_tolerance = np.abs((test_df['Чистые_продажи'] - test_df['Предсказано']) / 
                                  (test_df['Чистые_продажи'] + 1e-7)) <= tolerance
        q_errors[f'Точность_в_пределах_{int(tolerance*100)}%'] = within_tolerance.mean() * 100
    
    # Объединяем все метрики
    metrics = {
        'RMSE': rmse,
        'MAE': mae,
        'MAPE': mape,
        'SMAPE': smape,
        **q_errors
    }
    
    # Вывод результатов
    print("\nРасширенные метрики прогноза:")
    for name, value in metrics.items():
        print(f"{name}: {value:.4f}")
    
    return metrics

def seasonality_analysis(df):
    """Анализ сезонности продаж"""
    print("DEBUG: Анализ сезонности продаж")
    
    # Анализ по дням недели
    if 'День_недели' in df.columns:
        weekday_sales = df.groupby('День_недели')['Чистые_продажи'].agg(['mean', 'median', 'std']).reset_index()
        weekday_sales['День'] = weekday_sales['День_недели'].map({
            0: 'Понедельник', 1: 'Вторник', 2: 'Среда', 3: 'Четверг', 
            4: 'Пятница', 5: 'Суббота', 6: 'Воскресенье'
        })
        print("\nСредние продажи по дням недели:")
        print(weekday_sales[['День', 'mean', 'median', 'std']])
        
        # Нормализованные продажи по дням недели
        weekday_ratio = weekday_sales['mean'] / weekday_sales['mean'].mean()
        print("\nОтношение к среднему по дням недели:")
        for i, day in enumerate(weekday_sales['День']):
            print(f"{day}: {weekday_ratio.iloc[i]:.2f}")
    
    # Анализ по месяцам
    if 'Месяц' in df.columns:
        monthly_sales = df.groupby('Месяц')['Чистые_продажи'].agg(['mean', 'median', 'std']).reset_index()
        monthly_sales['Месяц_название'] = monthly_sales['Месяц'].map({
            1: 'Январь', 2: 'Февраль', 3: 'Март', 4: 'Апрель', 5: 'Май', 6: 'Июнь',
            7: 'Июль', 8: 'Август', 9: 'Сентябрь', 10: 'Октябрь', 11: 'Ноябрь', 12: 'Декабрь'
        })
        print("\nСредние продажи по месяцам:")
        print(monthly_sales[['Месяц_название', 'mean', 'median', 'std']])
        
        # Нормализованные продажи по месяцам
        monthly_ratio = monthly_sales['mean'] / monthly_sales['mean'].mean()
        print("\nОтношение к среднему по месяцам:")
        for i, month in enumerate(monthly_sales['Месяц_название']):
            print(f"{month}: {monthly_ratio.iloc[i]:.2f}")
    
    # Анализ перед/во время/после праздников
    if 'Дней_до_праздника' in df.columns and 'Дней_после_праздника' in df.columns:
        # Группировка по дням до ближайшего праздника
        near_holiday_sales = []
        
        # Перед праздником (1-7 дней)
        before = df[(df['Дней_до_праздника'] >= 1) & (df['Дней_до_праздника'] <= 7)]
        if not before.empty:
            near_holiday_sales.append({
                'Период': 'За 1-7 дней до праздника',
                'Средние продажи': before['Чистые_продажи'].mean(),
                'Отношение к среднему': before['Чистые_продажи'].mean() / df['Чистые_продажи'].mean()
            })
        
        # В день праздника
        on_holiday = df[df['Праздник'] == 1]
        if not on_holiday.empty:
            near_holiday_sales.append({
                'Период': 'В день праздника',
                'Средние продажи': on_holiday['Чистые_продажи'].mean(),
                'Отношение к среднему': on_holiday['Чистые_продажи'].mean() / df['Чистые_продажи'].mean()
            })
        
        # После праздника (1-7 дней)
        after = df[(df['Дней_после_праздника'] >= 1) & (df['Дней_после_праздника'] <= 7)]
        if not after.empty:
            near_holiday_sales.append({
                'Период': 'В течение 1-7 дней после праздника',
                'Средние продажи': after['Чистые_продажи'].mean(),
                'Отношение к среднему': after['Чистые_продажи'].mean() / df['Чистые_продажи'].mean()
            })
        
        # Обычные дни (не вблизи праздников)
        normal_days = df[(df['Дней_до_праздника'] > 7) & (df['Дней_после_праздника'] > 7)]
        if not normal_days.empty:
            near_holiday_sales.append({
                'Период': 'Обычные дни',
                'Средние продажи': normal_days['Чистые_продажи'].mean(),
                'Отношение к среднему': normal_days['Чистые_продажи'].mean() / df['Чистые_продажи'].mean()
            })
        
        print("\nВлияние праздников на продажи:")
        for item in near_holiday_sales:
            print(f"{item['Период']}: {item['Средние продажи']:.2f} (x{item['Отношение к среднему']:.2f})")
    
    return {
        'weekday_analysis': weekday_sales if 'День_недели' in df.columns else None,
        'monthly_analysis': monthly_sales if 'Месяц' in df.columns else None,
        'holiday_analysis': near_holiday_sales if 'Праздник' in df.columns else None
    }

def generate_sales_report(test_df, ensemble_results, importance_df, metrics, seasonality_analysis):
    """Генерация отчета о прогнозе продаж"""
    print("DEBUG: Генерация отчета о прогнозе продаж")
    
    report = []
    
    # Заголовок отчета
    report.append("# Отчет о прогнозе продаж\n")
    report.append(f"Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Основные метрики
    report.append("## Точность прогноза\n")
    report.append(f"* MAPE: {metrics.get('MAPE', 0):.2f}%\n")
    report.append(f"* SMAPE: {metrics.get('SMAPE', 0):.2f}%\n")
    report.append(f"* Точность в пределах 10%: {metrics.get('Точность_в_пределах_10%', 0):.2f}%\n")
    report.append(f"* Точность в пределах 20%: {metrics.get('Точность_в_пределах_20%', 0):.2f}%\n")
    report.append(f"* Точность в пределах 30%: {metrics.get('Точность_в_пределах_30%', 0):.2f}%\n")
    
    # Важность признаков
    report.append("## Топ-10 важных признаков\n")
    for i, (feature, importance) in enumerate(importance_df.head(10).iterrows(), 1):
        report.append(f"{i}. {feature}: {importance['Mean_Importance']:.4f}\n")
    report.append("")
    
    # Анализ сезонности
    report.append("## Анализ сезонности\n")
    
    # По дням недели
    weekday_analysis = seasonality_analysis.get('weekday_analysis')
    if weekday_analysis is not None:
        report.append("### Продажи по дням недели\n")
        report.append("| День недели | Средние продажи | Медиана | Стандартное отклонение | Отношение к среднему |")
        report.append("|-------------|-----------------|---------|------------------------|----------------------|")
        weekday_mean = weekday_analysis['mean'].mean()
        for _, row in weekday_analysis.iterrows():
            report.append(f"| {row['День']} | {row['mean']:.2f} | {row['median']:.2f} | {row['std']:.2f} | {row['mean']/weekday_mean:.2f} |")
        report.append("")
    
    # По месяцам
    monthly_analysis = seasonality_analysis.get('monthly_analysis')
    if monthly_analysis is not None:
        report.append("### Продажи по месяцам\n")
        report.append("| Месяц | Средние продажи | Медиана | Стандартное отклонение | Отношение к среднему |")
        report.append("|-------|-----------------|---------|------------------------|----------------------|")
        
        monthly_mean = monthly_analysis['mean'].mean()
        for _, row in monthly_analysis.iterrows():
            report.append(f"| {row['Месяц_название']} | {row['mean']:.2f} | {row['median']:.2f} | {row['std']:.2f} | {row['mean']/monthly_mean:.2f} |")
        report.append("")
    
    # По влиянию праздников
    holiday_analysis = seasonality_analysis.get('holiday_analysis')
    if holiday_analysis:
        report.append("### Влияние праздников на продажи\n")
        report.append("| Период | Средние продажи | Отношение к среднему |")
        report.append("|--------|-----------------|----------------------|")
        
        for item in holiday_analysis:
            report.append(f"| {item['Период']} | {item['Средние продажи']:.2f} | {item['Отношение к среднему']:.2f} |")
        report.append("")
    
    # Анализ товаров
    report.append("## Анализ товаров\n")
    
    # Товары с наибольшей ошибкой
    worst_items = test_df.groupby('SKU')['Абс_ошибка'].mean().sort_values(ascending=False).head(5)
    report.append("### Топ-5 товаров с наибольшей ошибкой\n")
    report.append("| SKU | Средняя абсолютная ошибка |")
    report.append("|-----|----------------------------|")
    for sku, error in worst_items.items():
        report.append(f"| {sku} | {error:.2f} |")
    report.append("")
    
    # Товары с наименьшей ошибкой
    best_items = test_df.groupby('SKU')['Абс_ошибка'].mean().sort_values().head(5)
    report.append("### Топ-5 товаров с наименьшей ошибкой\n")
    report.append("| SKU | Средняя абсолютная ошибка |")
    report.append("|-----|----------------------------|")
    for sku, error in best_items.items():
        report.append(f"| {sku} | {error:.2f} |")
    report.append("")
    
    # Информация о модели
    report.append("## Информация о модели\n")
    report.append("Ансамбль из трех моделей машинного обучения:")
    report.append(f"* LightGBM: вес {ensemble_results.get('lgb_weight', 0):.2f}")
    report.append(f"* XGBoost: вес {ensemble_results.get('xgb_weight', 0):.2f}")
    report.append(f"* CatBoost: вес {ensemble_results.get('cat_weight', 0):.2f}")
    
    # Сохраняем отчет в файл
    with open('sales_forecast_report.md', 'w', encoding='utf-8') as f:
        f.write('\n'.join(report))
    
    print("DEBUG: Отчет сохранен в файл 'sales_forecast_report.md'")
    return report

# ================================================
# 7. Основная функция запуска прогнозирования
# ================================================
def run_sales_forecast(test_size_days=30, forecast_days=30, n_trials=30, save_model=True):
    """Основная функция запуска процесса прогнозирования продаж"""
    print("DEBUG: Запуск прогнозирования продаж")
    
    # 1. Загрузка и обработка данных
    sales_df, holidays_df, promotions_df = load_data()
    
    # 2. Инженерия признаков
    processed_df = feature_engineering(sales_df, holidays_df, promotions_df)
    
    # 3. Подготовка данных для обучения
    X_train, y_train, X_test, y_test, y_test_original, cat_features, train_df, test_df = prepare_train_test_data(
        processed_df, test_size_days=test_size_days
    )
    
    # 4. Обучение и оптимизация ансамбля моделей
    ensemble_results, ensemble_pred = create_ensemble(
        X_train, y_train, X_test, y_test, cat_features, n_trials
    )
    
    # 5. Сохранение моделей
    if save_model:
        save_models(ensemble_results)
    
    # 6. Анализ результатов
    test_df['Предсказано'] = ensemble_pred
    test_with_pred = analyze_model_performance(ensemble_results, test_df, ensemble_pred, y_test_original)
    
    # 7. Анализ важности признаков
    importance_df = feature_importance_analysis(ensemble_results, X_test)
    
    # 8. Расширенная оценка прогноза
    metrics = forecast_evaluation(test_with_pred)
    
    # 9. Анализ сезонности
    seasonality_data = seasonality_analysis(processed_df)
    
    # 10. Поиск аномалий
    anomalies = anomaly_detection(processed_df)
    
    # 11. Генерация отчета
    generate_sales_report(test_with_pred, ensemble_results, importance_df, metrics, seasonality_data)
    
    # 12. Прогноз на будущее
    if forecast_days > 0:
        future_forecast = predict_future_sales(
            ensemble_results, processed_df, holidays_df, promotions_df, days_ahead=forecast_days
        )
        future_forecast.to_csv('future_sales_forecast.csv', index=False)
        print(f"DEBUG: Прогноз на {forecast_days} дней вперед сохранен в 'future_sales_forecast.csv'")
    
    print("DEBUG: Процесс прогнозирования завершен")
    return {
        'ensemble_results': ensemble_results,
        'test_with_pred': test_with_pred,
        'importance': importance_df,
        'metrics': metrics,
        'seasonality': seasonality_data,
        'anomalies': anomalies
    }

# ================================================
# 8. Дополнительные функции для интерактивного использования
# ================================================
def get_store_performance_summary(df):
    """Получение сводки по магазинам"""
    if 'Магазин' not in df.columns:
        return "Информация о магазинах недоступна"
    
    store_summary = df.groupby('Магазин').agg(
        Средний_объем_продаж=('Чистые_продажи', 'mean'),
        Общий_объем_продаж=('Чистые_продажи', 'sum'),
        Кол_во_SKU=('SKU', 'nunique'),
        Кол_во_транзакций=('Дата', 'count')
    ).sort_values('Общий_объем_продаж', ascending=False).reset_index()
    
    # Добавление нормализованного рейтинга
    store_summary['Рейтинг'] = store_summary['Общий_объем_продаж'].rank(pct=True).round(3)
    
    return store_summary

def get_product_performance_summary(df):
    """Получение сводки по товарам"""
    if 'SKU' not in df.columns:
        return "Информация о товарах недоступна"
    
    product_summary = df.groupby('SKU').agg(
        Средний_объем_продаж=('Чистые_продажи', 'mean'),
        Общий_объем_продаж=('Чистые_продажи', 'sum'),
        Кол_во_магазинов=('Магазин', 'nunique'),
        Кол_во_дней_продаж=('Дата', 'nunique')
    ).sort_values('Общий_объем_продаж', ascending=False).reset_index()
    
    # Добавляем информацию о весовых товарах, если есть
    if 'Весовой' in df.columns:
        weight_map = df.groupby('SKU')['Весовой'].first().to_dict()
        product_summary['Весовой'] = product_summary['SKU'].map(weight_map)
    
    return product_summary

def analyze_promotion_effectiveness(df):
    """Анализ эффективности акций"""
    if 'Акция_активна' not in df.columns or 'Тип_акции' not in df.columns:
        return "Информация об акциях недоступна"
    
    # Сравнение продаж с акцией и без
    promo_effect = df.groupby(['SKU', 'Акция_активна']).agg(
        Средние_продажи=('Чистые_продажи', 'mean'),
        Кол_во_дней=('Дата', 'nunique')
    ).reset_index()
    
    # Переформатируем данные для удобства анализа
    promo_effect_pivot = promo_effect.pivot(
        index='SKU', 
        columns='Акция_активна', 
        values=['Средние_продажи', 'Кол_во_дней']
    )
    
    # Заполняем пропуски
    promo_effect_pivot = promo_effect_pivot.fillna(0)
    
    # Создаем удобные имена колонок
    promo_effect_pivot.columns = [
        'Продажи_без_акции' if col == ('Средние_продажи', 0)
        else 'Продажи_с_акцией' if col == ('Средние_продажи', 1)
        else 'Дней_без_акции' if col == ('Кол_во_дней', 0)
        else 'Дней_с_акцией'
        for col in promo_effect_pivot.columns
    ]
    
    # Вычисляем эффект акции
    promo_effect_pivot['Эффект_акции'] = (
        promo_effect_pivot['Продажи_с_акцией'] / 
        promo_effect_pivot['Продажи_без_акции'].replace(0, 1)
    )
    
    # Фильтруем товары, у которых было достаточно дней с акцией
    promo_effect_summary = promo_effect_pivot[promo_effect_pivot['Дней_с_акцией'] >= 5].reset_index()
    
    # Сортируем по эффекту акции
    promo_effect_summary = promo_effect_summary.sort_values('Эффект_акции', ascending=False)
    
    # Анализ по типам акций
    if 'Тип_акции' in df.columns:
        promo_type_effect = df.groupby(['Тип_акции']).agg(
            Средние_продажи=('Чистые_продажи', 'mean'),
            Кол_во_SKU=('SKU', 'nunique'),
            Кол_во_дней=('Дата', 'nunique')
        ).reset_index()
        
        # Сравнение с обычными продажами (без акции)
        avg_sales_no_promo = df[df['Акция_активна'] == 0]['Чистые_продажи'].mean()
        promo_type_effect['Эффект_типа_акции'] = promo_type_effect['Средние_продажи'] / avg_sales_no_promo
        
        # Сортировка по эффективности
        promo_type_effect = promo_type_effect.sort_values('Эффект_типа_акции', ascending=False)
    
    return {'promo_effect_by_sku': promo_effect_summary, 'promo_effect_by_type': promo_type_effect}

def interactive_forecast_query(ensemble_results, holidays_df, promotions_df):
    """Интерактивный запрос для прогнозирования продаж по конкретным параметрам"""
    print("\nИнтерактивный прогноз продаж")
    
    try:
        # Запрос данных для прогноза
        store_id = int(input("Введите ID магазина: "))
        sku = input("Введите SKU товара: ")
        
        days_ahead = int(input("На сколько дней вперед построить прогноз? [1-100]: "))
        days_ahead = max(1, min(100, days_ahead))  # Ограничиваем диапазон
        
        # Получаем последние данные о продажах этого SKU в этом магазине
        sales_df, _, _ = load_data()
        
        # Фильтруем данные по запрошенному магазину и SKU
        item_data = sales_df[(sales_df['Магазин'] == store_id) & (sales_df['SKU'] == sku)].copy()
        
        if item_data.empty:
            print(f"Нет данных для SKU {sku} в магазине {store_id}")
            return None
        
        # Подготавливаем данные и делаем прогноз
        processed_df = feature_engineering(sales_df, holidays_df, promotions_df)
        item_data_processed = processed_df[(processed_df['Магазин'] == store_id) & (processed_df['SKU'] == sku)].copy()
        
        forecast_result = predict_future_sales(
            ensemble_results, item_data_processed, holidays_df, promotions_df, days_ahead=days_ahead
        )
        
        print(f"\nПрогноз продаж для SKU {sku} в магазине {store_id} на {days_ahead} дней:")
        print(forecast_result[['Дата', 'Прогноз_продаж']])
        
        return forecast_result
    
    except Exception as e:
        print(f"Ошибка при выполнении интерактивного запроса: {e}")
        return None

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Прогнозирование продаж")
    parser.add_argument("--test_days", type=int, default=30, help="Количество дней для теста")
    parser.add_argument("--forecast_days", type=int, default=30, help="Горизонт прогноза")
    parser.add_argument("--trials", type=int, default=30, help="Количество итераций Optuna")
    parser.add_argument("--interactive", action="store_true", help="Режим анализа и визуализации")

    args = parser.parse_args()

    # 1. Загрузка данных
    sales_df, holidays_df, promotions_df = load_data()

    # 2. Feature engineering
    sales_df = feature_engineering(sales_df, holidays_df, promotions_df)

    # 3. Подготовка данных
    X_train, y_train, X_test, y_test, y_test_original, cat_features, train_df, test_df = prepare_train_test_data(
        sales_df, test_size_days=args.test_days
    )

    # 4. Обучение и ансамблирование
    ensemble_results, ensemble_pred = create_ensemble(
        X_train, y_train, X_test, y_test, cat_features=cat_features, n_trials=args.trials
    )

    # 5. Анализ, если включён интерактивный режим
    if args.interactive:
        test_df = analyze_model_performance(ensemble_results, test_df, ensemble_pred, y_test_original)
        feature_importance_analysis(ensemble_results, X_test)
        forecast_evaluation(test_df)
        seasonality_analysis(train_df)

    # 6. Сохранение моделей
    save_models(ensemble_results)
