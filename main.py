import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import sqlalchemy
import re
from dataclasses import dataclass
import math
from credo import api_key, db_config

@dataclass
class YandexGeocoder:
    api_key: str
    geocoder_url: str = 'https://geocode-maps.yandex.ru/1.x'

    def adress_to_geopoint(self, address: str) -> str:

        # Address conversion to DataLens format geo-coordinates

        response = requests.get(self.geocoder_url, params={
            'apikey': self.api_key,
            'geocode': address,
            'format': 'json',
        })
        response.raise_for_status()

        result = response.json()['response']['GeoObjectCollection']['featureMember']
        if not result:
            return None

        lat, lon = result[0]['GeoObject']['Point']['pos'].split(' ')
        return self._to_datalens_format(lon, lat)

    def _to_datalens_format(self, lon, lat):
        return f'[{lon},{lat}]'


class CarParser:
    def __init__(self, base_url, params, db_config, api_key):
        self.base_url = base_url
        self.params = params
        self.engine = sqlalchemy.create_engine(f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['db_name']}")
        self.geocoder = YandexGeocoder(api_key=api_key)
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }

    def get_total_pages(self):
        response = requests.get(f"{self.base_url}?{self.params}", headers=self.headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        small_elements = soup.find_all('small')
        for element in small_elements:
            if element.text.startswith("Prikazano od"):
                total_ads_text = element.text
                break
        total_ads = int(re.search(r'\d+', total_ads_text.split('ukupno')[1]).group())
        return math.ceil(total_ads / 25)

    def parse_cars(self, url):
        response = requests.get(url, headers=self.headers)
        if response.status_code != 200:
            print("Ошибка при загрузке страницы")
            return pd.DataFrame()

        soup = BeautifulSoup(response.text, 'html.parser')
        car_elements = soup.find_all(lambda tag: tag.name == 'article' and 'classified ad' in ' '.join(tag.get('class', [])))
        cars_data = []
        for car_el in car_elements:
            set_info_elements = car_el.find_all('div', class_='setInfo')

            price = car_el.find('div', class_='price')
            classified_id = car_el.get('data-classifiedid', None)
            # Исходная цена
            original_price = price.text.strip() if price else None

            # Очистка данных о цене
            clean_price = None
            if price:
                price_text = price.text.strip().replace('.', '').replace('€', '').replace(',', '.').strip()
                matches = re.findall(r'\d+\.?\d*', price_text)
                clean_price = float(matches[-1]) if matches else None

            city = car_el.find('div', class_='city')
            set_info1_parts = set_info_elements[0].text.strip().split('\n') if len(set_info_elements) > 0 else [None,
                                                                                                                None]
            set_info2_parts = set_info_elements[1].text.strip().split('\n') if len(set_info_elements) > 0 else [None,
                                                                                                                None]

            city_name = city.text.strip() if city else 'Belgrade'
            coords = self.get_or_request_city_coords(city_name)
            car_data = {

            'car_id': classified_id,
            'RequestDateTime': datetime.now(),
            'cabin_type': set_info1_parts[0],
            'engine_type': set_info1_parts[1] if len(set_info1_parts) > 1 else None,
            'mileage': set_info2_parts[0],
            'engine_power': set_info2_parts[1] if len(set_info2_parts) > 1 else None,
            'Price': original_price,
            'Clean_price': clean_price,
            'City': city.text.strip() if city else None,
            'coord': coords
            }
            cars_data.append(car_data)
        return pd.DataFrame(cars_data)

    def get_or_request_city_coords(self, city_name):
        try:
            query = f"SELECT * FROM coords WHERE city = '{city_name}'"
            existing_coords = pd.read_sql(query, self.engine)
        except:
            existing_coords = pd.DataFrame()

        if not existing_coords.empty:
            coords = existing_coords.iloc[0]['coord']
        else:
            coords = self.geocoder.adress_to_geopoint(city_name)
            if coords:
                new_entry = pd.DataFrame({'city': [city_name], 'coord': [coords]})
                new_entry.to_sql('coords', self.engine, if_exists='append', index=False)
        return coords

    def parse_all_pages(self):
        total_pages = self.get_total_pages()
        all_cars_dfs = []
        for page in range(1, total_pages + 1):
            url = f"{self.base_url}?page={page}&{self.params}"
            cars_df = self.parse_cars(url)
            if not cars_df.empty:
                all_cars_dfs.append(cars_df)
        return pd.concat(all_cars_dfs, ignore_index=True) if all_cars_dfs else pd.DataFrame()


    def save_to_database(self, df, table_name):
            if not df.empty:
                df.to_sql(table_name, self.engine, if_exists='append', index=False)
            else:
                print("DataFrame пуст, нет данных для записи в базу данных.")



car_parser = CarParser(
    base_url="https://www.polovniautomobili.com/auto-oglasi/pretraga",
    params="sort=basic&brand=peugeot&model%5B0%5D=308&chassis%5B0%5D=2634&city_distance=0&showOldNew=all&without_price=1",
    db_config=db_config,
    api_key=api_key
)

car_df = car_parser.parse_all_pages()
car_df = car_df.dropna(subset=['car_id'])
print(car_df)
car_parser.save_to_database(car_df, '308_prices')
