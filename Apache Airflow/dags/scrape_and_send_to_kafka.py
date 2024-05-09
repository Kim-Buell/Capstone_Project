from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import pandas as pd
from bs4 import BeautifulSoup
from kafka import KafkaProducer

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 14, 6, 0),
    'email_on_failure': False,
    'email_on_retry': False,
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
}


# Function to scrape data
def stream_data(date):
    """Scrape NHL season data for given years and game types and combine into one DataFrame"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")

    # Set Season
    year = date[:4]
    month = date[5:7]

    if month in ['10', '11', '12']:
        year_2 = str(int(year) + 1)
        season = year + year_2
    else:
        year_2 = str(int(year) - 1)
        season = year_2 + year

    # Define the format of yesterday's date string
    date_format = '%Y-%m-%d'

    # Convert yesterday's date string to a datetime object
    date_obj = datetime.strptime(date, date_format)

    # Define the start and end dates for the Season Type Range range
    start_date = datetime(2024, 4, 19)
    end_date = datetime(2024, 7, 1)

    # Check if yesterday's date is within the specified range
    if start_date <= date_obj <= end_date:
        gameType = "3"
    else:
        gameType = "2"

    # Create Driver
    options = chrome_options
    remote_webdriver = 'remote_chromedriver'
    driver = webdriver.Remote(f'{remote_webdriver}:4444/wd/hub', options=options)

    data = []  # Initialize an empty list to store data

    # Construct the URL
    url = f"https://www.nhl.com/stats/teams?aggregate=0&reportType=game&dateFrom={date}&dateTo={date}&sort=points,wins&page=0&pageSize=100"

    # Set up Chrome WebDriver
    driver.get(url)

    # Scroll to the bottom of the page
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(5)

    # Retrieve HTML content
    html_content = driver.page_source

    # Close the browser
    driver.quit()

    # Create a BeautifulSoup object
    soup = BeautifulSoup(html_content, 'html.parser')

    # Extract relevant data
    rows = soup.find_all('div', class_='rt-tr-group')

    # Append data based on game type
    for row in rows:
        columns = row.find_all('div', class_='rt-td')
        team_name = columns[1].text.strip()
        game_date = columns[2].text.strip()
        GP = columns[3].text.strip()
        W = columns[4].text.strip()
        L = columns[5].text.strip()
        T = columns[6].text.strip()

        if gameType == "2" or season in ["20192020", "20202021"]:
            OT = columns[7].text.strip()
            P = columns[8].text.strip()
            P_percent = columns[9].text.strip()
            RW = columns[10].text.strip()
            ROW = columns[11].text.strip()
            SO_win = columns[12].text.strip()
            GF = columns[13].text.strip()
            GA = columns[14].text.strip()
            GF_GP = columns[15].text.strip()
            GA_GP = columns[16].text.strip()
            PP_percent = columns[17].text.strip()
            PK_percent = columns[18].text.strip()
            Net_PP_percent = columns[19].text.strip()
            Net_PK_percent = columns[20].text.strip()
            Shots_GP = columns[21].text.strip()
            SA_GP = columns[22].text.strip()
            FOW_percent = columns[23].text.strip()

            data.append({
                "Team Name": team_name,
                "Game Date": game_date,
                "GP": GP,
                "W": W,
                "L": L,
                "T": T,
                "OT": OT,
                "P": P,
                "P%": P_percent,
                "RW": RW,
                "ROW": ROW,
                "SO_win": SO_win,
                "GF": GF,
                "GA": GA,
                "GF/GP": GF_GP,
                "GA/GP": GA_GP,
                "PP%": PP_percent,
                "PK%": PK_percent,
                "Net PP%": Net_PP_percent,
                "Net PK%": Net_PK_percent,
                "Shots/GP": Shots_GP,
                "SA/GP": SA_GP,
                "FOW%": FOW_percent
            })

        elif gameType == "3":
            OT = 'NA'
            P = columns[7].text.strip()
            P_percent = columns[8].text.strip()
            RW = columns[9].text.strip()
            ROW = columns[10].text.strip()
            SO_win = columns[11].text.strip()
            GF = columns[12].text.strip()
            GA = columns[13].text.strip()
            GF_GP = columns[14].text.strip()
            GA_GP = columns[15].text.strip()
            PP_percent = columns[16].text.strip()
            PK_percent = columns[17].text.strip()
            Net_PP_percent = columns[18].text.strip()
            Net_PK_percent = columns[19].text.strip()
            Shots_GP = columns[20].text.strip()
            SA_GP = columns[21].text.strip()
            FOW_percent = columns[22].text.strip()

            data.append({
                "Team Name": team_name,
                "Game Date": game_date,
                "GP": GP,
                "W": W,
                "L": L,
                "T": T,
                "OT": OT,
                "P": P,
                "P%": P_percent,
                "RW": RW,
                "ROW": ROW,
                "SO_win": SO_win,
                "GF": GF,
                "GA": GA,
                "GF/GP": GF_GP,
                "GA/GP": GA_GP,
                "PP%": PP_percent,
                "PK%": PK_percent,
                "Net PP%": Net_PP_percent,
                "Net PK%": Net_PK_percent,
                "Shots/GP": Shots_GP,
                "SA/GP": SA_GP,
                "FOW%": FOW_percent
            })

    # Combine all dictionaries into one
    combined_dict = {}
    for dictionary in data:
        for key, value in dictionary.items():
            combined_dict.setdefault(key, []).append(value)

    # Convert combined dictionary into a DataFrame
    df = pd.DataFrame(combined_dict)

    # Add Season and Game Type Columns
    df['Season'] = season
    df['Type'] = 'Regular Season' if gameType == "2" else 'Playoff'

    # Return DataFrame
    return df


def send_data_to_kafka(**kwargs):
    # Define Kafka bootstrap servers
    KAFKA_BOOTSTRAP_SERVERS = 'ip_address' # Enter IP Address

    # Get the DataFrame from task instance
    dataframe = kwargs['ti'].xcom_pull(task_ids='scrape_data')

    # Convert DataFrame to JSON string
    json_data = dataframe.to_json()

    # Create KafkaProducer instance
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

    # Send JSON data to Kafka topic
    producer.send(kwargs['kafka_topic'], json_data.encode('utf-8'))

    # Flush and close the producer
    producer.flush()
    producer.close()

    print("DataFrame sent to Kafka")


# Function to calculate yesterday's date
def get_yesterday_date():
    # Calculate yesterday's date
    yesterday_date = datetime.today() - timedelta(days=1)
    # Format yesterday's date as yyyy-mm-dd
    yesterday_date_str = yesterday_date.strftime('%Y-%m-%d')
    return yesterday_date_str


# Define the DAG
with DAG('nhl_data_pipeline', default_args=default_args, description='Pipeline to stream NHL data and send it to Kafka', schedule_interval=timedelta(days=1)) as dag:

    # Task to calculate yesterday's date
    get_yesterday_task = PythonOperator(
        task_id='get_yesterday_date',
        python_callable=get_yesterday_date,
    )

    # Task to scrape data
    scrape_task = PythonOperator(
        task_id='scrape_data',
        python_callable=stream_data,
        op_kwargs={'date': '{{ ti.xcom_pull(task_ids="get_yesterday_date") }}'},  # Use yesterday's date
    )

    # Task to send data to Kafka
    kafka_task = PythonOperator(
        task_id='send_to_kafka',
        python_callable=send_data_to_kafka,
        op_kwargs={'kafka_topic': 'nhl_data'},  # Pass the Kafka topic name
    )

    # Define task dependencies
    get_yesterday_task >> scrape_task >> kafka_task