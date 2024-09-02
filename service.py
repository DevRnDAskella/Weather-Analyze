from datetime import datetime, timedelta
import re
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import pandas as pd
import requests as req
import bs4
import time


def get_content(airport, from_year, from_day=1, to_day=31, hour_start=0, hour_end=23, format='txt'):
    content_list = []
    current_year = datetime.now().year
    current_month = datetime.now().month

    for year in range(from_year, current_year + 1):
        max_month = current_month if year == current_year else 12

        for month in range(1, max_month + 1):
            url = (
                f"https://www.ogimet.com/display_metars2.php?lang=en&lugar={
                    airport}"
                f"&tipo=ALL&ord=REV&nil=SI&fmt={format}&ano={
                    year}&mes={month}&day={from_day}"
                f"&hora={hour_start}&anof={year}&mesf={month}&dayf={
                    to_day}&horaf={hour_end}&minf=59&send=send"
            )

            try:
                page = req.get(url)

                soup = bs4.BeautifulSoup(page.text, 'html.parser')
                content = soup.find('body').text.split('\n')

                content_list.extend(content)

                time.sleep(1)

            except req.exceptions.RequestException as e:
                print(f"Ошибка при получении данных: {e}")

    return content_list


def clean_content(content):
    cleaned_content = []
    for row in content:
        row = row.strip()
        if len(row) >= 5 and "#" not in row:
            cleaned_content.append(row)

    return cleaned_content


def split_content_by_type_weather(content):
    metar_data = []
    taf_data = []

    for row in content:
        if "METAR" in row:
            metar_data.append(row)
        elif "TAF" in row:
            taf_data.append(row)

    return {"METAR": metar_data, "TAF": taf_data}


def generate_datetime_sequence(min_time, max_time):
    """
    Функция для генерации последовательности времени с шагом в 30 минут между заданными минимальным и максимальным временем.

    Аргументы:
    - min_time: Минимальное время (datetime).
    - max_time: Максимальное время (datetime).

    Возвращает:
    - DataFrame с последовательностью времени с шагом в 30 минут.
    """

    def round_down_to_nearest_30_minutes(dt):
        return dt - timedelta(minutes=dt.minute % 30, seconds=dt.second, microseconds=dt.microsecond)

    min_time_rounded = round_down_to_nearest_30_minutes(min_time)

    date_time_sequence = []
    current_time = min_time_rounded

    while current_time <= max_time:
        date_time_sequence.append(current_time)
        current_time += timedelta(minutes=30)

    df_seq = pd.DataFrame({'datetime': date_time_sequence})

    return df_seq


def transform_metar_file(file):
    df_metar = pd.read_csv(file)

    if 'metar' not in df_metar.columns:
        raise ValueError(f"Отсутствует колонка 'metar' в файле {file}")

    df_metar['metar'] = df_metar['metar'].str.strip()
    df_metar['datetime'] = df_metar['metar'].apply(lambda row: pd.to_datetime(row[:12], format='%Y%m%d%H%M', errors='coerce') if len(
        row) >= 12 else pd.NaT)
    df_metar = df_metar[df_metar['metar'].str.len() > 40].sort_values(
        'datetime').reset_index(drop=True)
    df_metar = df_metar[df_metar['metar'].str.count(
        '/') <= 10].reset_index(drop=True)

    # Мелкие исправления вынести в отдельную функцию
    df_metar['metar'] = df_metar['metar'].str.replace(
        'VPS', 'MPS', regex=False)
    df_metar = df_metar[~df_metar['metar'].str.contains(
        'QQMPS', case=False, na=False)]

    airport_pattern = re.compile(r'\b[a-zA-Z]{4}\b')

    wind_pattern = re.compile(
        r'(?P<WIND_DIRECT>VRB|\d{3})(?P<WIND_POWER>\d{2,3})G?(?P<WIND_GUST>\d{2})?(?P<WIND_UOM>KT|MPS)\s?(?P<WIND_VARIABLE>\d+V\d+)?')

    temp_pattern = re.compile(
        r'\b(?P<TEMPERATURE>M?\d{2})/(?P<DEW_POINT>M?\d{2})\b')

    min_time = df_metar['datetime'].min()
    max_time = df_metar['datetime'].max()

    df_seq = generate_datetime_sequence(min_time, max_time)

    df = pd.merge(df_seq, df_metar, how='left',
                  on='datetime').sort_values('datetime')

    df['metar'] = df['metar'].ffill()

    df['airport'] = df['metar'].apply(lambda row: re.search(
        airport_pattern, row).group() if re.search(airport_pattern, row) else pd.NA)

    df['wind_direct'] = df['metar'].apply(
        lambda row: int(match.group('WIND_DIRECT')) if (match := re.search(wind_pattern, row)) and match.group('WIND_DIRECT').isdigit()
        else match.group('WIND_DIRECT') if match and match.group('WIND_DIRECT') == 'VRB'
        else pd.NA
    )

    df['wind_power'] = df['metar'].apply(
        lambda row: int(re.search(wind_pattern, row).group('WIND_POWER')) if re.search(wind_pattern, row) else pd.NA)

    df['wind_gust'] = df['metar'].apply(
        lambda row: int(re.search(wind_pattern, row).group('WIND_GUST'))
        if re.search(wind_pattern, row) and re.search(wind_pattern, row).group('WIND_GUST') is not None else pd.NA
    )

    df['wind_uom'] = df['metar'].apply(
        lambda row: re.search(wind_pattern, row).group('WIND_UOM') if re.search(wind_pattern, row) else pd.NA)

    df['wind_variable'] = df['metar'].apply(
        lambda row: re.search(wind_pattern, row).group('WIND_VARIABLE') if re.search(wind_pattern, row) else pd.NA)

    df['temp'] = df['metar'].apply(
        lambda row: convert_temperature(
            re.search(temp_pattern, row).group('TEMPERATURE'))
        if re.search(temp_pattern, row) and re.search(temp_pattern, row).group('TEMPERATURE') is not None
        else pd.NA
    )

    df['dew_point'] = df['metar'].apply(
        lambda row: convert_temperature(match.group('DEW_POINT'))
        if (match := re.search(temp_pattern, row)) and match.group('DEW_POINT') is not None
        else pd.NA
    )

    columns_to_save = ['datetime', 'airport', 'wind_direct', 'wind_power',
                       'wind_gust', 'wind_uom', 'wind_variable', 'temp', 'dew_point', 'metar']

    return df[columns_to_save]


def transform_taf_file(file):
    pass


def get_connection(db, db_username=None, db_password=None, db_host=None, db_port=None, db_name=None):
    """
    Функция для загрузки переменных окружения и создания подключения к базе данных PostgreSQL.
    Возвращает объект подключения psycopg2.
    """
    conn = None

    if db == "weather":
        load_dotenv()

        db_username = os.getenv('DB_USERNAME_WEATHER') or db_username
        db_password = os.getenv('DB_PASSWORD_WEATHER') or db_password
        db_host = os.getenv('DB_HOST_WEATHER') or db_host
        db_port = os.getenv('DB_PORT_WEATHER') or db_port
        db_name = os.getenv('DB_NAME_WEATHER') or db_name

        if not all([db_username, db_password, db_host, db_port, db_name]):
            raise ValueError(
                "Одна или несколько переменных окружения не загружены. Проверьте файл .env.")

        # Создание подключения к базе данных PostgreSQL
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_username,
            password=db_password,
            host=db_host,
            port=db_port
        )

    else:
        raise ValueError(f"Нет такой БД")

    return conn


def exec_query(conn, query: str = None, file: str = None) -> pd.DataFrame:
    """
    Функция для выполнения SQL-запросов из строки или файла с использованием psycopg2.

    Аргументы:
    - conn: Объект подключения psycopg2.
    - query: Строка с SQL-запросом (опционально).
    - file: Путь к файлу .sql, содержащему SQL-запросы (опционально).

    Возвращает:
    - Результат выполнения запроса в виде DataFrame (для запросов SELECT) или None, если запрос не возвращает данные.

    Исключения:
    - Вызывает исключение в случае ошибки при выполнении SQL-запроса.
    """
    try:
        with conn.cursor() as cur:
            if file:
                with open(file, 'r') as f:
                    sql_queries = f.read()

                for sql_query in sql_queries.split(';'):
                    sql_query = sql_query.strip()
                    if sql_query:
                        cur.execute(sql_query)
                        if cur.description:
                            columns = [desc[0] for desc in cur.description]
                            df = pd.DataFrame(cur.fetchall(), columns=columns)
                            return df

            elif query:
                cur.execute(query)
                if cur.description:
                    columns = [desc[0] for desc in cur.description]
                    df = pd.DataFrame(cur.fetchall(), columns=columns)
                    return df

            else:
                raise ValueError("Нужно указать либо 'file', либо 'query'.")

    except Exception as e:
        print("Ошибка при выполнении SQL:", e)
        return None
    finally:
        conn.commit()


def write_df_to_table(conn, df: pd.DataFrame, table_name: str):
    """
    Функция для записи данных из DataFrame в таблицу базы данных PostgreSQL с использованием psycopg2.

    Аргументы:
    - conn: Существующее подключение к базе данных psycopg2.
    - df: DataFrame, который нужно записать в таблицу.
    - table_name: Название таблицы в базе данных.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {table_name};")

            data_tuples = list(df.itertuples(index=False, name=None))

            cols = ', '.join(df.columns)
            insert_query = f"INSERT INTO {
                table_name} ({cols}) VALUES (%s, %s, %s)"

            cur.executemany(insert_query, data_tuples)
            print(f"Данные успешно вставлены в таблицу '{table_name}'.")

    except Exception as e:
        print("Ошибка при записи данных в таблицу:", e)
    finally:
        conn.commit()


def convert_temperature(value):
    if value.startswith('M'):
        return -int(value[1:])
    else:
        return int(value)


def get_engine(db, db_username=None, db_password=None, db_host=None, db_port=None, db_name=None):
    """
    Функция для создания объекта SQLAlchemy engine для подключения к базе данных PostgreSQL.
    """
    load_dotenv()

    db_username = os.getenv('DB_USERNAME_WEATHER') or db_username
    db_password = os.getenv('DB_PASSWORD_WEATHER') or db_password
    db_host = os.getenv('DB_HOST_WEATHER') or db_host
    db_port = os.getenv('DB_PORT_WEATHER') or db_port
    db_name = os.getenv('DB_NAME_WEATHER') or db_name

    if not all([db_username, db_password, db_host, db_port, db_name]):
        raise ValueError(
            "Одна или несколько переменных окружения не загружены. Проверьте файл .env или передайте параметры в функцию.")

    engine = create_engine(
        f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'
    )

    return engine


def combine_csv_files_with_metar(directory_path):
    """
    Функция для объединения всех CSV-файлов в указанной директории, которые содержат 'metar' в названии, в один DataFrame.

    Аргументы:
    - directory_path: Путь к директории, в которой находятся файлы CSV.

    Возвращает:
    - DataFrame, содержащий данные из всех подходящих CSV-файлов.
    """
    combined_df = pd.DataFrame()

    for filename in os.listdir(directory_path):
        if filename.endswith('.csv') and 'metar' in filename.lower():
            file_path = os.path.join(directory_path, filename)
            try:
                df = pd.read_csv(file_path)
                combined_df = pd.concat([combined_df, df], ignore_index=True)
                print(f"Файл '{filename}' успешно добавлен в общий DataFrame.")
            except Exception as e:
                print(f"Ошибка при чтении файла '{filename}': {e}")

    return combined_df
