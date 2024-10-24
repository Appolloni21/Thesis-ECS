import requests
import json
from bs4 import BeautifulSoup
from copy import deepcopy
from tqdm import tqdm 

URL_ROOT_1 = 'https://www.auto-data.net'

def save_html(html, path):
    with open(path, 'wb') as f:
        f.write(html)

def open_html(path):
    with open(path, 'rb') as f:
        return f.read()

def save_json(filename, data):
    with open(filename, 'w') as f:
        json.dump(data, f)

def open_json(filename):
    with open(filename, 'r') as f:
        data = json.load(f)
        return data

def get_brands(url):
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html.parser')
    
    brands = []
    table = soup.select('.marki_blok')
    i=0
    for row in table:
        link1 = row.get('href').strip()
        #link2 = link1[4:]
        brand_page = URL_ROOT_1 + link1
        brands.append(brand_page)

    return brands

def get_models(urls):
    all_models = []
    for url in tqdm(urls):
        
        r = requests.get(url)
        soup = BeautifulSoup(r.content, 'html.parser')

        try:
            table = soup.select('.modeli')
            for row in table:
                link1 = row.get('href').strip()
                model_page = URL_ROOT_1 + link1
                all_models.append(model_page)

        except TypeError:
            pass

    return all_models

def get_gen(urls):
    all_gen = []
    for url in tqdm(urls):
        r = requests.get(url)
        soup = BeautifulSoup(r.content, 'html.parser')
        try:
            table = soup.select_one('#generr')
            references = table.select('a[title]')
            for ref in references:
                link = ref.get('href').strip()
                gen_page = URL_ROOT_1 + link
                all_gen.append(gen_page)
            
        except TypeError:
            pass

    return all_gen

def get_configurations(urls):
    configs = []
    for url in tqdm(urls):
        r = requests.get(url)
        soup = BeautifulSoup(r.content, 'html.parser')
        try:
            table = soup.select_one('.carlist')
            references = table.select('a[title]')
            print(references)
            for ref in references:
                link = ref.get('href').strip()
                config_page = URL_ROOT_1 + link
                configs.append(config_page)
            
        except:
            pass
    
    return configs

def car_spec_(urls):
    #in questa lista andremo a mettere tutte le specifiche che ci interessa sapere di un auto
    cars = []
    for url in tqdm(urls):
        r = requests.get(url)
        soup = BeautifulSoup(r.content, 'html.parser')
        table = soup.select('.cardetailsout.car2 tr')

        #lista che contiene i label delle spec che siamo interessati ad estrarre dalla tabella  
        labels = ['Brand', 'Model', 'Body type', 'Powertrain Architecture','CO2 emissions (NEDC)', 'Fuel Type', 'Emission standard', 'Power', 'Engine displacement', 'Kerb Weight']           

        for row in table:
            spec_label = row.select_one('th').text.strip()
            if spec_label in labels:
                spec_elem = row.select_one('td').text.strip()
                d = dict()
                d[spec_label] = spec_elem
                cars.append(d)

    return cars

def car_spec(urls):
    #in questo dizionario andremo a mettere tutte le specifiche che ci interessa sapere di un auto
    cars = dict()
    id=0
    for url in tqdm(urls):
        r = requests.get(url)
        soup = BeautifulSoup(r.content, 'html.parser')
        try:
            table = soup.select('.cardetailsout.car2 tr')

            #lista che contiene i label delle spec che siamo interessati ad estrarre dalla tabella  
            labels = ['Brand', 'Model', 'Body type', 'Powertrain Architecture','CO2 emissions','CO2 emissions (NEDC)','CO2 emissions (WLTP)','Fuel Type', 'Emission standard', 'Power', 'Electric motor power', 'System power','Engine displacement', 'Kerb Weight']           
            car = dict()
            for row in table:
                spec_label = row.select_one('th').text.strip()
                if spec_label in labels:
                    spec_elem = row.select_one('td').text.strip()
                    car[spec_label] = spec_elem
            cars[id] = car
            id+=1
        except:
            pass

    return cars

def get_cars(urls):
    #in questo dizionario andremo a mettere tutte le specifiche che ci interessa sapere di un auto
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

url_root = 'https://www.auto-data.net/en/allbrands'

#brand_pages = get_brands(url_root)                                                     #ci sono 335 brands

#model_pages = get_models(brand_pages)
#model_pages = open_json('model_pages.json')                                             #ci sono poco più di 3300 modelli di auto

#gen_pages = get_gen(model_pages)
#save_json('gen_pages.json', gen_pages)
#gen_pages = open_json('gen_pages.json')                                                 #ci sono 9586 configurazioni in totale


#config_pages = get_configurations(gen_pages)       
#save_json('config_pages.json', config_pages)
config_pages = open_json('config_pages.json')                                           #ci sono 52487 configurazioni
#config_pages = ["https://www.auto-data.net/en/ford-capri-ii-gecp-2.0-90hp-7850"] 


car_data = get_cars(config_pages)
save_json('cars.json', car_data)
#print(car_data)