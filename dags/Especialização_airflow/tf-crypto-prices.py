"""
Crypto Price Pipeline
DAG that fetches, processes and loads crypto prices using TaskFlow API.
"""

# TODO import libraries
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime
from typing import Dict


# TODO declare DAG
@dag(
    dag_id='tf-crypto-prices',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['crypto', 'api', 'especialização']
)
def crypto_pipeline():

    # TODO declare tasks
    @task(task_id="fetch-api-crypto", retries=2)
    def get_crypto_price() -> Dict:
        """Fetch crypto prices from CoinGecko API"""

        url = Variable.get("api-espec-bitcoin")
        params = {
            "ids": "bitcoin,ethereum",
            "vs_currencies": "usd"
        }
        response = requests.get(url, params=params)
        return response.json()

    @task()
    def process_price(data: Dict) -> Dict:
        """Process the crypto prices"""

        return {
            'timestamp': datetime.now().isoformat(),
            'prices': {
                'BTC': data['bitcoin']['usd'],
                'ETH': data['ethereum']['usd']
            }
        }

    @task()
    def load_price(data: Dict):
        """Load the processed prices"""

        print(f"Timestamp: {data['timestamp']}")
        print(f"Crypto Prices: {data['prices']}")

    # TODO define task dependencies
    load_price(process_price(get_crypto_price()))


# TODO create DAG
dag = crypto_pipeline()