-- Ejecuta esta consulta en Hive View (Query Editor)

CREATE EXTERNAL TABLE IF NOT EXISTS TIENDAS_RANKING (
    canal                  STRING,
    posicion               INT,
    jefe                   STRING,
    ejecutivo              STRING,
    monto                  DOUBLE,
    desembolsado           INT,
    volumen_cambiado       DOUBLE,
    utilidad               DOUBLE
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA INPATH '/user/maria_dev/sin/tiendas_ranking.csv' 
OVERWRITE INTO TABLE TIENDAS_RANKING;
