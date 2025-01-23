INSERT INTO raw_car_circulating
SELECT  tipo_veicolo, destinazione, uso, provincia, marca, cilindrata, alimentazione, potenza, immatricolazione, classe, emissioni, peso
FROM raw_car_temp;
DROP TABLE raw_car_temp;