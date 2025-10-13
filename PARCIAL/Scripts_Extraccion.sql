-- SCRIPTS PARA CREAR LAS TABLAS Y CARGAR LOS DATOS DEL CSV EN HIVE

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

-------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS TLV_RANKING (
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

LOAD DATA INPATH '/user/maria_dev/sin/tlv_ranking.csv' 
OVERWRITE INTO TABLE TLV_RANKING;

--------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS SEGMENTOS (
    periodo                INT,
    codigo_unico           BIGINT, 
    ruc                    BIGINT, 
    jefe                   STRING,
    ejecutivo              STRING,
    segmento_fx            STRING,
    tipo_cuenta            STRING,
    logeo                  INT,
    num_logeos             INT,
    flg_dig                INT
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA INPATH '/user/maria_dev/sin/segmentos_repeated.csv' 
OVERWRITE INTO TABLE SEGMENTOS;

---------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS REGISTRO_COMUNICACIONES (
    tipo_cliente           STRING,
    especificacion         STRING,
    cantidad               INT,
    dia                    STRING,
    estado                 STRING,
    card                   STRING
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA INPATH '/user/maria_dev/sin/registro_comunicaciones.csv' 
OVERWRITE INTO TABLE REGISTRO_COMUNICACIONES;

------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS VIRTUAL_RANKING (
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

LOAD DATA INPATH '/user/maria_dev/sin/virtual_ranking.csv' 
OVERWRITE INTO TABLE VIRTUAL_RANKING;

--------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS CAMBIOS_CLIENTES (
    fecha                  STRING,
    codigo_unico           INT,
    ruc                    BIGINT, 
    nombre                 STRING,
    tipo                   STRING,
    utilidad               DOUBLE,
    vol_usd                DOUBLE
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA INPATH '/user/maria_dev/sin/cambios_clientes.csv' 
OVERWRITE INTO TABLE CAMBIOS_CLIENTES;
