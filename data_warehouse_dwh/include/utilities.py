import requests, zipfile, io, csv, os

AIRFLOW_PATH = "/usr/local/airflow/"
AIRFLOW_RESOURCE_PATH = AIRFLOW_PATH + "resources/"
AIRFLOW_PREPOC_PATH = AIRFLOW_RESOURCE_PATH + "pre_processed/"      #directory containing prepocessed version of raw files

DATASETS_2019_DIR = AIRFLOW_PATH + "include/datasets_2019/"

REGIONS_A = ["Abruzzo", "Basailicata", "Calabria", "Molise", "Sicilia"]

URLS = [
    "https://dati.mit.gov.it/hfs/parco_circolante_Abruzzo.csv.zip",
    "https://dati.mit.gov.it/hfs/parco_circolante_Basilicata.csv.zip",
    "https://dati.mit.gov.it/hfs/parco_circolante_Calabria.csv.zip",
    "https://dati.mit.gov.it/hfs/parco_circolante_Campania.csv.zip",
    "https://dati.mit.gov.it/hfs/parco_circolante_EmiliaRomagna.csv.zip",
    "https://dati.mit.gov.it/hfs/parco_circolante_FriuliVeneziaGiulia.csv.zip",
    "https://dati.mit.gov.it/hfs/parco_circolante_Lazio.csv.zip",
    "https://dati.mit.gov.it/hfs/parco_circolante_Liguria.csv.zip",
    "https://dati.mit.gov.it/hfs/parco_circolante_Lombardia.csv.zip",
    "https://dati.mit.gov.it/hfs/parco_circolante_Marche.csv.zip",
    "https://dati.mit.gov.it/hfs/parco_circolante_Molise.csv.zip",
    "https://dati.mit.gov.it/hfs/parco_circolante_Piemonte.csv.zip",
    "https://dati.mit.gov.it/hfs/parco_circolante_Puglia.csv.zip",
    "https://dati.mit.gov.it/hfs/parco_circolante_Sardegna.csv.zip",
    "https://dati.mit.gov.it/hfs/parco_circolante_Sicilia.csv.zip",
    "https://dati.mit.gov.it/hfs/parco_circolante_Toscana.csv.zip",
    "https://dati.mit.gov.it/hfs/parco_circolante_TrentinoAltoAdige.csv.zip",
    "https://dati.mit.gov.it/hfs/parco_circolante_Umbria.csv.zip",
    "https://dati.mit.gov.it/hfs/parco_circolante_ValleAosta.csv.zip",
    "https://dati.mit.gov.it/hfs/parco_circolante_Veneto.csv.zip",
]


def get_data():

    # URL del file ZIP
    # url = "https://dati.mit.gov.it/hfs/parco_circolante_Abruzzo.csv.zip"

    # Percorso dove salvare il file CSV
    # output_csv_path = "resources/parco_circolante_Abruzzo.csv"
    for url in URLS:
        try:
            # Scarica il file ZIP
            response = requests.get(url)
            response.raise_for_status()  # Controlla errori HTTP

            # Crea un file ZIP in memoria
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                # Stampa i nomi dei file contenuti nello ZIP
                print("File presenti nello ZIP:", z.namelist())

                # Cerca il file CSV
                for file_name in z.namelist():
                    if file_name.endswith(".csv"):
                        # Estrai e salva il file CSV
                        with z.open(file_name) as csv_file:
                            # Percorso dove salvare il file CSV
                            output_csv_path = "resources/" + str(file_name)
                            with open(output_csv_path, "wb") as output_file:
                                output_file.write(csv_file.read())
                        print(f"File CSV salvato come: {output_csv_path}")
                        file_name_str = str(file_name)
                        result = file_name_str.rsplit(".", 1)[0]
                        # return result
                        break
                    else:
                        print("Nessun file CSV trovato nello ZIP.")

        except requests.exceptions.RequestException as e:
            print(f"Errore durante il download del file: {e}")
        except zipfile.BadZipFile:
            print("Il file scaricato non è un file ZIP valido.")
        except Exception as e:
            print(f"Errore imprevisto: {e}")


def pre_processing():

    for file_name in os.listdir(AIRFLOW_RESOURCE_PATH):
        # Controlla se il file ha estensione .csv
        if file_name.endswith(".csv"):
            file_path = os.path.join(AIRFLOW_RESOURCE_PATH, file_name)
            #print(f"Extended Path: {file_path}")

            #If region is not in that list, csv associated file must be pre processed 
            if not (get_region_name(file_name) in REGIONS_A):
                print(f"Prepocessing in corso: {file_name}")

                output_file = AIRFLOW_PREPOC_PATH + "pp_" + file_name 

                # Apertura del file originale in lettura e del nuovo file in scrittura
                with open(file_path, "r", encoding="utf-8") as infile, open(
                    output_file, "w", encoding="utf-8", newline=""
                ) as outfile:
                    # Lettura e scrittura del file CSV
                    reader = csv.reader(
                        infile, quotechar='"', delimiter=",", quoting=csv.QUOTE_MINIMAL
                    )
                    writer = csv.writer(
                        outfile, quotechar='"', delimiter=",", quoting=csv.QUOTE_MINIMAL
                    )

                    #Friuli csv file is the only one containing the header and it must be removed
                    if(get_region_name(file_name) == "Friuli"):
                        # Salta la prima riga (header o altre informazioni)
                        next(reader, None)

                    for row in reader:
                        # Rimozione degli apostrofi da ogni campo della riga
                        new_row = [field.replace("'", " ") for field in row]
                        writer.writerow(new_row)

                    print(f"Prepocessed file created: {output_file}")
                


def get_region_name(file_name):
    l1 = file_name.split(".")
    l2 = l1[0].split("_")

    region_name = l2[1]
    return region_name




def postgreSQL_importing(query, conn, data_path):
    print(f"Loading: " + f"{data_path}")
    cur = conn.cursor()
    try:
        with open(data_path, "r") as file: 
            cur.copy_expert(query,file)
        conn.commit()
    except Exception as e:
        print(f"Errore durante il caricamento: {e}")