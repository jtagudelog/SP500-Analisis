# from flask import Flask
# from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine

import pandas as pd
import urllib
import logging
import yfinance as yf
from io import StringIO
import requests
# import pyobdc
from bs4 import BeautifulSoup

logging.basicConfig(filename='ETL.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_sp500_list(url):
    logging.info(f'Extrayendo datos de {url}')
    response = requests.get(url)
    if response.status_code != 200:
        logging.error(f'Error al hacer la solicitud HTTP: {response.status_code}')
        return None

    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', {'id': 'constituents'})

    if table:
        html_string = str(table)
        df = pd.read_html(StringIO(html_string))[0]
        logging.info('Datos extraídos exitosamente')
        return df
    else:
        logging.error('No se encontró la tabla de empresas en la página')
        return None

def get_stock_prices(ticker, start_date, end_date):
    logging.info(f'Obteniendo precios de {ticker} desde {start_date} hasta {end_date}')
    try:
        stock_data = yf.download(ticker, start=start_date, end=end_date)
        if stock_data.empty:
            logging.warning(f"No se encontraron datos de precios para {ticker}")
            return None
        return stock_data[['Close']]
    except Exception as e:
        logging.error(f'Error al obtener datos de {ticker}: {e}')
        return None

def extract_stock_prices(df, start_date, end_date):
    prices = {}
    ticker_column = None
    for col in ['Símbolo', 'Symbol']:
        if col in df.columns:
            ticker_column = col
            break
    if ticker_column is None:
        logging.error('No se encontró la columna de símbolos en el DataFrame')
        return None

    for ticker in df[ticker_column]:
        price_data = get_stock_prices(ticker, start_date, end_date)
        if price_data is not None:
            prices[ticker] = price_data
    return prices

def transform_sp500_list(df):
    logging.info('Transformando datos de la lista de empresas')
    logging.info(f"Columnas del DataFrame original: {df.columns}")

    column_mapping = {
        'Symbol': 'Symbol',  # Mantener 'Symbol'
        'Security': 'Company',
        'GICS Sector': 'Sector',
        'GICS Sub-Industry': 'SubIndustry'
    }
    
    df = df.rename(columns=column_mapping)
    
    required_columns = ['Symbol', 'Company', 'Sector', 'SubIndustry']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logging.error(f"Las siguientes columnas faltan en el DataFrame: {missing_columns}")
        return None

    df = df[required_columns]
    df = df.dropna()
    logging.info('Transformación de la lista de empresas completada')
    return df

def transform_stock_prices(prices):
    logging.info('Transformando datos de los precios de las empresas')
    frames = []
    for ticker, df in prices.items():
        df.reset_index(inplace=True)
        df['Symbol'] = ticker  # Cambiado de 'Ticker' a 'Symbol'
        frames.append(df[['Date', 'Symbol', 'Close']])
    result = pd.concat(frames)
    result['Date'] = pd.to_datetime(result['Date'])
    logging.info('Transformación de los precios de las empresas completada')
    return result

def load_to_sql(df, table_name, engine):
    logging.info(f'Insertando datos en la tabla {table_name}')
    try:
        df.to_sql(table_name, con=engine, if_exists='append', index=False)
        logging.info(f'Datos insertados exitosamente en la tabla {table_name}')
    except Exception as e:
        logging.error(f'Error al insertar datos en {table_name}: {e}')

# Configuración de la conexión a la base de datos SQL Server
server = 'JTAG-01\\SQLEXPRESS04' 
database = 'sp1'
username = 'sp1'
password = '123'
driver = 'ODBC Driver 17 for SQL Server'

params = urllib.parse.quote_plus(
    f"DRIVER={driver};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password}"
)
connection_string = f"mssql+pyodbc:///?odbc_connect={params}"
engine = create_engine(connection_string)

# Probar la conexión a la base de datos
try:
    with engine.connect() as connection:
        print("Conexión a la base de datos exitosa")
except Exception as e:
    print(f"Error al conectar a la base de datos: {e}")

# URL de la lista S&P 500
url_en = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'

# Proceso ETL
df_sp500 = extract_sp500_list(url_en)

if df_sp500 is not None:
    start_date = '2024-01-01'
    end_date = '2024-03-31'

    stock_prices = extract_stock_prices(df_sp500, start_date, end_date)

    if stock_prices is not None:
        df_sp500_clean = transform_sp500_list(df_sp500)
        if df_sp500_clean is not None:
            df_stock_prices_clean = transform_stock_prices(stock_prices)

            load_to_sql(df_sp500_clean, 'CompanyProfiles', engine)
            load_to_sql(df_stock_prices_clean, 'Companies', engine)