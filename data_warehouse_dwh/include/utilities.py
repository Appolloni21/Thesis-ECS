import requests, zipfile, io


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

DATA_PATHS = [
    "resources/Circolante_Abruzzo.csv",
    "resources/Circolante_Basailicata.csv",
    "resources/Circolante_Calabria.csv",
    "resources/Circolante_Molise.csv",
    "resources/Circolante_Sicilia.csv",
]

DATA_PATHS_2 = [
    "resources/Circolante_Campania.csv",
    "resources/Circolante_Emilia.csv",
    "resources/Circolante_Friuli.csv",
    "resources/Circolante_Lazio.csv",
    "resources/Circolante_Liguria.csv",
    "resources/Circolante_Lombardia.csv",
    "resources/Circolante_Marche.csv",
    "resources/Circolante_Piemonte.csv",
    "resources/Circolante_Puglia.csv",
    "resources/Circolante_Sardegna.csv",
    "resources/Circolante_Toscana.csv",
    "resources/Circolante_Trentino.csv",
    "resources/Circolante_Umbria.csv",
    "resources/Circolante_Valle_Aosta.csv",
    "resources/Circolante_Veneto.csv",
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
