import os
import time
from datetime import datetime

import psycopg2
import requests
import schedule
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor


def price_converter(curr_from, curr_to, amount):
    params = (
        ('access_key', os.getenv('api_key')),
        ('from', curr_from),
        ('to', curr_to),
        ('amount', amount),
    )

    response = requests.get('https://data.fixer.io/api/convert', params=params)
    json_response = response.json()
    if json_response.get('success'):
        return json_response.get('result')
    else:
        return None


def db_connection():
    conn = psycopg2.connect(
        host=os.getenv('host'),
        database=os.getenv('database'),
        user=os.getenv('user'),
        password=os.getenv('password'))
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    # creating additional Columns
    cursor.execute(
        f'Alter Table {os.getenv("table")} ADD COLUMN IF NOT EXISTS to_USD_price FLOAT,ADD COLUMN IF NOT EXISTS is_to_USD_price_done bool,ADD COLUMN IF NOT EXISTS USD_price_done_at TIMESTAMP,ADD COLUMN IF NOT EXISTS to_EUR_price FLOAT,ADD COLUMN IF NOT EXISTS is_to_EUR_price_done bool,ADD COLUMN IF NOT EXISTS EUR_price_done_at TIMESTAMP')

    # checking rows have price/date_of_sale null or date_of_sale<1999
    cursor.execute(
        f"SELECT count(*) FROM {os.getenv('table')} where (price is Null or date_of_sale is null or date_of_sale<'1999-01-01'::date) and is_to_USD_price_done is Null or is_to_USD_price_done=false")
    fetched = cursor.fetchone()

    # if filtered rows exists lets update them.
    if fetched['count'] > 0:
        cursor.execute(
            f"UPDATE {os.getenv('table')} SET is_to_USD_price_done=true,USD_price_done_at='{datetime.now()}',is_to_EUR_price_done=true,EUR_price_done_at='{datetime.now()}' WHERE date_of_sale is Null or price is Null or date_of_sale<'1999-01-01'::date");
        conn.commit()
    return conn


def main_job():
    conn = db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    step = 1
    limit = 20
    while True:
        offset = (step - 1) * limit
        print("[*] updating rows from:", offset, "- ", offset + 20)
        cur.execute(
            f"SELECT * FROM {os.getenv('table')} where is_to_USD_price_done=false or is_to_USD_price_done is Null or is_to_EUR_price_done=false or is_to_EUR_price_done is Null order by id asc OFFSET {offset} LIMIT {limit}")

        rows = cur.fetchall()
        for row in rows:
            row_id = row['id']
            print("[+] Row Id: ", row_id)
            currency = row['currency']
            price_to = row['price']
            if currency == "USD":
                usd = price_to
                usd_done = True
                usd_time = datetime.now()
            else:
                usd = price_converter(curr_from=currency, curr_to="USD", amount=price_to)
                usd_done = True
                usd_time = datetime.now()

            if currency == "EUR":
                eur = price_to
                eur_done = True
                eur_time = datetime.now()
            else:
                eur = price_converter(curr_from=currency, curr_to="EUR", amount=price_to)
                eur_done = True
                eur_time = datetime.now()
            cur.execute(
                f"UPDATE {os.getenv('table')} SET to_USD_price='{usd}',is_to_USD_price_done='{usd_done}',USD_price_done_at='{usd_time}',to_EUR_price='{eur}',is_to_EUR_price_done='{eur_done}',EUR_price_done_at='{eur_time}' where id='{row_id}' ")
        conn.commit()
        step += 1


if __name__ == '__main__':
    load_dotenv()
    # running on launch
    main_job()
    
    # scheduled for every 30 minutes.
    schedule.every(30).minutes.do(main_job)
    while True:
        schedule.run_pending()
        time.sleep(1)
