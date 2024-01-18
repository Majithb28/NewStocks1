import yfinance as yf
from yahooquery import Ticker
import pyodbc
import pandas as pd

# Specify the stock symbol (ticker) for which you want information
stock_symbol = "TSM"

# Create a Ticker object for the specified stock symbol
stock_ticker = yf.Ticker(stock_symbol)

# Get general information about the stock
stock_info = stock_ticker.info

# Fetch historical data for the last day
historical_data = stock_ticker.history(period='1d')

# Extracting relevant information from historical data
open_price = historical_data['Open'].iloc[-1]
high_price = historical_data['High'].iloc[-1]
low_price = historical_data['Low'].iloc[-1]
close_price = historical_data['Close'].iloc[-1]

# Fetch dividends using yfinance
dividends = stock_ticker.dividends

# Fetch financial reports using yahooquery
financial_reports = Ticker(stock_symbol).income_statement()

# Resetting the index of financial_reports DataFrame
financial_reports_reset = financial_reports.reset_index()

# MSSQL connection details
server = 'chat' #use your server name
database = 'master' #choose your db name
username = 'sa' #use your username
password = 'dsm@123'#use your password
driver = 'ODBC Driver 17 for SQL Server'#choose your db driver

# Create a connection
conn = pyodbc.connect(
    'DRIVER=' + driver + ';'
    'SERVER=' + server + ';'
    'DATABASE=' + database + ';'
    'UID=' + username + ';'
    'PWD=' + password + ';'
)

# Create a cursor
cursor = conn.cursor()

# Create a table if it doesn't exist
cursor.execute('''
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='stocks' AND xtype='U')
    BEGIN
        USE Chatbot1 #use db name
        CREATE TABLE stocks (
            id INT PRIMARY KEY IDENTITY(1,1),
            symbol NVARCHAR(255),
            company_name NVARCHAR(255),
            industry NVARCHAR(255),
            sector NVARCHAR(255),
            open_price FLOAT,
            high_price FLOAT,
            low_price FLOAT,
            close_price FLOAT,
            current_price FLOAT,
            market_cap NVARCHAR(255),
            dividends NVARCHAR(MAX),
            financial_reports NVARCHAR(MAX)
        )
    END
''')

# Insert data into the database
cursor.execute('''
    USE Chatbot1 #use db name
    INSERT INTO stocks (
        symbol, company_name, industry, sector,
        open_price, high_price, low_price, close_price,
        current_price, market_cap, dividends, financial_reports
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
''', (
    stock_symbol,
    stock_info.get('longName', 'N/A'),
    stock_info.get('industry', 'N/A'),
    stock_info.get('sector', 'N/A'),
    open_price,
    high_price,
    low_price,
    close_price,
    historical_data['Close'].iloc[-1],
    stock_info.get('marketCap', 'N/A'),
    dividends.to_json(),  # Save dividends as JSON (modify as needed)
    financial_reports_reset.to_json(),  # Save financial reports as JSON (modify as needed)
))

# Commit the changes and close the connection
conn.commit()
conn.close()

