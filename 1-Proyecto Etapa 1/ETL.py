"""
Proyecto

Integrantes:
    Sandra Patricia Carrillo
    José Tobias Agudelo Gutiérrez
    José William Salcedo Pérez
"""



import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
import yfinance as yf # type: ignore
from io import StringIO

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
    # Imprime las columnas del DataFrame para inspeccionarlas
    print("Columnas del DataFrame original:", df.columns)
    logging.info(f"Columnas del DataFrame original: {df.columns}")

    column_mapping = {
        'Symbol': 'Ticker',
        'Security': 'Company',
        'GICS Sector': 'Sector',
        'GICS Sub-Industry': 'SubIndustry'
    }
    
    df = df.rename(columns=column_mapping)
    
    required_columns = ['Ticker', 'Company', 'Sector', 'SubIndustry']
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
        df['Ticker'] = ticker
        frames.append(df[['Date', 'Ticker', 'Close']])
    result = pd.concat(frames)
    result['Date'] = pd.to_datetime(result['Date'])
    logging.info('Transformación de los precios de las empresas completada')
    return result

def load_to_csv(df, filename):
    logging.info(f'Guardando datos en {filename}')
    df.to_csv(filename, index=False)
    logging.info(f'Datos guardados exitosamente en {filename}')

url_es = 'https://es.wikipedia.org/wiki/Anexo:Compa%C3%B1%C3%ADas_del_S%26P_500'
url_en = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'

df_sp500 = extract_sp500_list(url_es)
#if df_sp500 is None:
#    df_sp500 = extract_sp500_list(url_en)

if df_sp500 is not None:
    start_date = '2024-01-01'
    end_date = '2024-03-31'

    stock_prices = extract_stock_prices(df_sp500, start_date, end_date)

    if stock_prices is not None:
        df_sp500_clean = transform_sp500_list(df_sp500)
        if df_sp500_clean is not None:
            df_stock_prices_clean = transform_stock_prices(stock_prices)

            load_to_csv(df_sp500_clean, 'sp500_companies.csv')
            load_to_csv(df_stock_prices_clean, 'stock_prices.csv')
