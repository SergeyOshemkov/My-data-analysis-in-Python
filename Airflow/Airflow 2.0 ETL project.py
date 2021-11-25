import requests
from io import StringIO
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

# Задаю дефолтные аргументы для DAG:

default_args = {
    'owner': 's.oshemkov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 11, 11),
    'schedule_interval': '0 12 * * *'

}

# Задаю параметры для отправки сообщений в телеграм:

CHAT_ID = '75622129'

try:
    BOT_TOKEN = Variable.get('telegram_secret')
except:
    BOT_TOKEN = ''


def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id

    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=f'Dag {dag_id} is completed on {date}')
    else:
        pass


### Часть DAG-a:

@dag(default_args=default_args, catchup=False)
def dag_oshemkov():
    @task(retries=1)
    def get_data():
        # Т.к. в консоли репозиторий не клонируется - fatal: unable to access с этим буду разбираться отдельно, инструкции со стака у меня не работают...   # Пришлось закачать датасет на гугл-диск и с него читать csv...

        # Считываю датасет:

        orig_url = 'https://drive.google.com/file/d/1oP-jkxFBJESwaQISJSGU4I5Tx4eF0AOJ/view?usp=sharing'

        file_id = orig_url.split('/')[-2]
        dwn_url = 'https://drive.google.com/uc?export=download&id=' + file_id
        url = requests.get(dwn_url).text
        csv_raw = StringIO(url)
        dfsdf = pd.read_csv(csv_raw)

        # Подготавливаю датасет для дальнейшей работы:

        dfsdf.Year = dfsdf.Year.fillna(0)  # Выберу только конкретный год, поэтому обратно менять не буду.
        dfsdf.Year = dfsdf.Year.astype(int)

        # Выбираю нужный год:

        year = 1994 + hash(f's.oshemkov') % 23

        df = dfsdf.query('Year == @year')

        # Сохраняю файл в рабочую директорию:

        return df

    # Какая игра была самой продаваемой в этом году во всем мире? Насколько понял из задания - по объему продаж.

    @task(retries=1, retry_delay=timedelta(18))
    def top_sales_world(df):
        top_selling_game = df.groupby('Name', as_index=False) \
            .agg({'Global_Sales': 'sum'}) \
            .sort_values('Global_Sales', ascending=False) \
            .rename(columns={'Global_Sales': 'overall_sales'}) \
            .head(1) \
            .Name \
            .item()

        return top_selling_game

    # # Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько:

    @task(retries=1, retry_delay=timedelta(18))
    def top_selling_genre_EU(df):
        sales_x_genre = df.groupby('Genre', as_index=False) \
            .agg({'EU_Sales': 'sum'}) \
            .sort_values('EU_Sales', ascending=False)

        max_sales = sales_x_genre.EU_Sales.max()

        best_seller_EU = sales_x_genre.query('EU_Sales == @max_sales').Genre.item()

        return best_seller_EU

    # У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько

    @task(retries=1, retry_delay=timedelta(18))
    def top_avg_sales_JP(df):
        jp_sales_publisher = df.groupby('Publisher', as_index=False) \
            .agg({'JP_Sales': 'mean'}) \
            .sort_values('JP_Sales', ascending=False) \
            .rename(columns={'JP_Sales': 'mean_sales'})

        max_avg_jp_sales = jp_sales_publisher.mean_sales.max()

        top_publishers_jp = jp_sales_publisher.query('mean_sales == @max_avg_jp_sales') \
            .Publisher \
            .item()

        return top_publishers_jp

    # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной 
    # Америке? Перечислить все, если их несколько

    # Комментарий: в выбранной анализируемой части датасета всего df.shape[0] = 1202 записей. Поэтому
    # игр, проданных милионным тиражом в NA не будет. Исходя из этого я понял что необходимо найти игры,
    # объем продаж которых в Северной Америке составил > 1 M.

    @task(retries=1, retry_delay=timedelta(18))
    def platform(df):
        df1 = df[['Name', 'Platform', 'NA_Sales']]

        # Нахожу объем продаж по каждой игре:

        names_sold = df1.groupby('Name', as_index=False) \
            .agg({'NA_Sales': 'sum'}) \
            .query('NA_Sales > 1.0')

        # Делаю левый джоин с изначальной базой данных для того чтобы выбрать платформу(ы):

        merged = df1.merge(names_sold, on='Name', how='left') \
            .dropna()

        max_sales = merged.NA_Sales_y.max()
        best_platforms = merged.query('NA_Sales_y == @max_sales').Platform.to_list()

        return best_platforms

    # Сколько игр продались лучше в Европе, чем в Японии?

    @task(retries=1, retry_delay=timedelta(18))
    def number_games(df):
        eu_jp_df = df[['Name', 'EU_Sales', 'JP_Sales']]

        num_games = eu_jp_df.groupby('Name') \
            .sum() \
            .query('EU_Sales > JP_Sales') \
            .shape[0]

        return num_games

    #  Отправка сообщений в телеграм:

    @task(on_success_callback=send_message)
    def print_data(top_selling_game,
                   best_seller_EU,
                   top_publishers_jp,
                   best_platforms,
                   num_games
                   ):
        context = get_current_context()
        date = context['ds']

        return print(f'The top selling game for {date} is {top_selling_game}',
                     f'The best selling genre in EU for the {date} is(are) {best_seller_EU}',
                     f'The publisher(s) with the highest sales in JP for {date} is {top_publishers_jp}',
                     f'Platforms with sales > 1M in NA for {date} are {best_platforms}',
                     f'The number of games sold in EU more than in JP for the {date} is {num_games}'
                     )

    # Задаю последовательность тасков:

    df = get_data()
    top_selling_game = top_sales_world(df)
    best_seller_EU = top_selling_genre_EU(df)
    best_platforms = platform(df)
    top_publishers_jp = top_avg_sales_JP(df)
    num_games = number_games(df)
    print_data(top_selling_game,
               best_seller_EU,
               top_publishers_jp,
               best_platforms,
               num_games
               )


# Запускаю DAG:

dag_oshemkov = dag_oshemkov()
