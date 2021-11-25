import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

# Загружаю датасет:

def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def top_10_frequent_domains():

    all_domains_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['domain'])

    all_domains_df['domain_zone'] = all_domains_df.domain \
                                                  .apply(lambda x: x.split('.')[-1])
    top_10_domains_by_frequency = all_domains_df.groupby('domain_zone', as_index=False) \
                                                .count() \
                                                .sort_values('domain', ascending=False) \
                                                .head(10)

    return top_10_domains_by_frequency

    # with open('top_10_domains_by_frequency.csv', 'w') as t:
    #   t.write(top_10_domains_by_frequency.to_csv(index=False, header=False))


def longest_domain():

    domains_only_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['domain'])

    # создаю в датафрейме колонку с доменами:

    domains_only_df['true_domain'] = domains_only_df.domain \
                                                  .apply(lambda x: x.split('.')[-2])

    # нахожу самыe длинные доменные имена:

    domains_only_df['domain_length'] = domains_only_df.true_domain \
                                                    .apply(lambda x: len(x))

    max_length = domains_only_df.domain_length.max()

    longest_domain = sorted(domains_only_df.query('domain_length == @max_length') \
                         .true_domain.to_list())
    return longest_domain


def airflow_domain_rank():

    all_domains_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['domain'])

    try:
        airflow_rank = all_domains_df.loc[all_domains_df['domain'] == "airflow.com"].index.item()
    except:
        airflow_rank = 'Ooops, seems to be that airflow.com is not in 1M most popular domains now ... Stay tuned! :)'

    return airflow_rank


default_args = {
    'owner': 's.oshemkov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2021, 11, 11),
    'schedule_interval': '2 11 * * *'
}

dag = DAG('2nd_lesson_DAG', default_args=default_args)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_10_frequent_domains',
                    python_callable=top_10_frequent_domains,
                    dag=dag)

t3 = PythonOperator(task_id='longest_domain',
                    python_callable=longest_domain,
                    dag=dag)

t4 = PythonOperator(task_id='airflow_domain_rank',
                    python_callable=airflow_domain_rank,
                    dag=dag)

t1 >> [t2, t3, t4]