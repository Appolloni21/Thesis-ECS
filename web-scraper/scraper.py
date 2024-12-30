import requests
import json
from bs4 import BeautifulSoup
from tqdm import tqdm 

URL_ROOT = 'https://www.auto-data.net'
URL_ALLBRANDS = 'https://www.auto-data.net/en/allbrands'

def save_json(filename, data):
    with open(filename, 'w') as f:
        json.dump(data, f)

def open_json(filename):
    with open(filename, 'r') as f:
        data = json.load(f)
        return data

def get_links(urls, html_selector):
    links = []
    for url in tqdm(urls):
        r = requests.get(url)
        soup = BeautifulSoup(r.content, 'html.parser')
        try:
            table = soup.select(html_selector)
            for row in table:
                link = row.get('href').strip()
                full_link = URL_ROOT + link
                links.append(full_link)
        except TypeError:
            pass
    return links

def get_cars(urls):
    #In this dict I will put all the veichle spec we are interested
    cars = dict()
    id=0
    for url in tqdm(urls):
        r = requests.get(url)
        soup = BeautifulSoup(r.content, 'html.parser')
        try:
            table = soup.select('.cardetailsout.car2 tr')
            car = dict()
            for row in table:
                spec_label = row.select_one('th').text.strip()
                if row.find(class_= False) and spec_label != '':    
                    spec_elem = row.select_one('td').text.strip()
                    car[spec_label] = spec_elem     
            cars[id] = car
            id+=1
        except:
            pass
    return cars

#---------------------------
#   MAIN    
#---------------------------
def main():
    brand_pages = get_links([URL_ALLBRANDS],'.marki_blok')                                   
    #print(len(brand_pages))    #335 brands

    model_pages = get_links(brand_pages, '.modeli')
    #print(len(model_pages))    #3300 different models

    gen_pages = get_links(model_pages, '#generr a[title]')
    #print(len(gen_pages))      #9586 different gen                                               

    config_pages = get_links(gen_pages, '.carlist a[title]')
    #print(len(config_pages))    #52487 different configurations

    #for testing purposes
    #temp=['https://www.auto-data.net/en/abarth-124-gt-1.4-multiair-170hp-automatic-35172', 'https://www.auto-data.net/en/abarth-124-gt-1.4-multiair-170hp-35171', 'https://www.auto-data.net/en/abarth-124-spider-1.4-multiair-170hp-automatic-24535', 'https://www.auto-data.net/en/abarth-124-spider-1.4-multiair-170hp-25192']

    car_data = get_cars(config_pages)
    save_json('cars.json', car_data)

if __name__ == '__main__':
  main()