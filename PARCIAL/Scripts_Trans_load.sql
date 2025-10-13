-- CREACIÓN DE TABLA DE DIMENSIONES

--DIMENSIÓN CANAL
CREATE EXTERNAL TABLE IF NOT EXISTS DIM_CANAL (
    id_canal             INT, 
    nombre_canal         STRING
)
STORED AS ORC; -- Usamos ORC para eficiencia.

--DIMENSIÓN EJECUTIVO
CREATE EXTERNAL TABLE IF NOT EXISTS DIM_EJECUTIVO (
    id_ejecutivo         BIGINT, -- Clave Primaria (Sustituta)
    nombre_ejecutivo     STRING,
    nombre_jefe          STRING
)
STORED AS ORC;

--DIMENSIÓN CLIENTE
CREATE EXTERNAL TABLE IF NOT EXISTS DIM_CLIENTE (
    id_cliente           BIGINT, -- Clave Primaria (Sustituta)
    codigo_unico         BIGINT,
    ruc                  BIGINT,
    nombre_cliente       STRING,
    segmento_fx          STRING,
    tipo_cuenta          STRING,
    flag_fuga            INT,  
    
    -- Atributos de comportamiento del cliente
    logeo                INT, 
    num_logeos           INT,
    flg_dig              INT
)
STORED AS ORC;

--DIMENSIÓN COMUNICACION

CREATE EXTERNAL TABLE IF NOT EXISTS DIM_COMUNICACION (
    id_comunicacion      INT, -- Clave Primaria (Sustituta)
    tipo_cliente         STRING,
    especificacion       STRING, 
    estado               STRING,
    card                 STRING,
    flag_presupuesto     INT 
)
STORED AS ORC;

--DIMENSION FECHA
CREATE EXTERNAL TABLE IF NOT EXISTS DIM_FECHA (
    id_fecha             INT, 
    fecha                DATE,
    periodo              INT, 
    dia_util_flag        INT, 
    dia_final_util       INT, 
    anio                 INT,
    mes_num              INT,
    nombre_dia_semana    STRING
)
STORED AS ORC;

--TABLA DE HECHOS
CREATE EXTERNAL TABLE IF NOT EXISTS FACT_UTILIDAD_TRADING (
    id_hecho             BIGINT, -- Clave Primaria Sustituta
    
    -- Claves Foráneas (FKs)
    id_fecha             INT,
    id_ejecutivo         BIGINT,
    id_cliente           BIGINT,
    id_canal             INT,
    id_comunicacion      INT,
    
    -- Métricas (Medidas de Utilidad)
    monto                DOUBLE,
    desembolsado         INT,
    volumen_cambiado     DOUBLE,
    utilidad             DOUBLE 
)
STORED AS ORC;

-- INSERCCION Y CARGA DE LOS DATOS

--DIMENSION CANAL
INSERT OVERWRITE TABLE DIM_CANAL
SELECT
    ROW_NUMBER() OVER (ORDER BY canal) AS id_canal,
    canal AS nombre_canal
FROM (
    SELECT DISTINCT canal FROM TLV_RANKING
    UNION ALL
    SELECT DISTINCT canal FROM VIRTUAL_RANKING
    UNION ALL
    SELECT DISTINCT canal FROM TIENDAS_RANKING
) t;

--DIMENSION EJECUTIVO
INSERT OVERWRITE TABLE DIM_EJECUTIVO
SELECT
    ROW_NUMBER() OVER (ORDER BY t.ejecutivo, t.jefe) AS id_ejecutivo,
    t.ejecutivo AS nombre_ejecutivo,
    t.jefe AS nombre_jefe
FROM (
    -- Combina la lista única de Ejecutivos y Jefes de todas las fuentes
    SELECT DISTINCT ejecutivo, jefe FROM TLV_RANKING
    UNION
    SELECT DISTINCT ejecutivo, jefe FROM VIRTUAL_RANKING
    UNION
    SELECT DISTINCT ejecutivo, jefe FROM TIENDAS_RANKING
) AS t;

--DIMENSION FECHA
INSERT OVERWRITE TABLE DIM_FECHA
SELECT
    CAST(REGEXP_REPLACE(t.fecha_raw, '-', '') AS INT) AS id_fecha,
    CAST(t.fecha_raw AS DATE) AS fecha,
    CAST(SUBSTR(REGEXP_REPLACE(t.fecha_raw, '-', ''), 1, 6) AS INT) AS periodo,
    
    -- Campos de enriquecimiento
    1 AS dia_util_flag, 
    0 AS dia_final_util,
    CAST(SUBSTR(REGEXP_REPLACE(t.fecha_raw, '-', ''), 1, 4) AS INT) AS anio,
    CAST(SUBSTR(REGEXP_REPLACE(t.fecha_raw, '-', ''), 5, 2) AS INT) AS mes_num,
    'DESCONOCIDO' AS nombre_dia_semana
FROM 
    (
        -- 1. Consolidar todas las fechas únicas de las fuentes relevantes
        SELECT DISTINCT dia AS fecha_raw FROM REGISTRO_COMUNICACIONES
        UNION 
        SELECT DISTINCT SUBSTR(fecha, 1, 10) AS fecha_raw FROM CAMBIOS_CLIENTES
    ) AS t;

--DIMENSION CLIENTE
-- 1. Crear la tabla temporal para obtener los atributos de cliente más recientes
DROP TABLE IF EXISTS temp_cambios_clientes_reciente;

CREATE TABLE temp_cambios_clientes_reciente AS
SELECT  *,
    ROW_NUMBER() OVER (PARTITION BY codigo_unico ORDER BY fecha DESC) as rn 
FROM
    cambios_clientes;

INSERT OVERWRITE TABLE DIM_CLIENTE
SELECT
    -- Genera ID_CLIENTE para cada Codigo Unico
    ROW_NUMBER() OVER (ORDER BY u.codigo_unico) AS id_cliente,
    
    -- Claves Naturales
    u.codigo_unico AS codigo_unico,
    COALESCE(s.ruc, c.ruc) AS ruc,
    c.nombre AS nombre_cliente,
    
    -- Atributos de Segmentación y Logeo (desde SEGMENTOS)
    s.segmento_fx,
    s.tipo_cuenta,
    s.logeo,
    s.num_logeos,
    s.flg_dig,
    
    -- Derivación: Flag Fuga (Basado en la utilidad más reciente)
    CASE 
        WHEN c.utilidad < 500 AND c.codigo_unico IS NOT NULL THEN 1 
        ELSE 0 
    END AS flag_fuga 
FROM 
    (
        -- Lista Unificada de Clientes (Clientes únicos)
        SELECT DISTINCT codigo_unico, ruc FROM SEGMENTOS
        UNION
        SELECT DISTINCT codigo_unico, ruc FROM CAMBIOS_CLIENTES
    ) AS u
LEFT JOIN SEGMENTOS s ON u.codigo_unico = s.codigo_unico
LEFT JOIN temp_cambios_clientes_reciente c ON u.codigo_unico = c.codigo_unico AND c.rn = 1;

-- DIMENSION COMUNICACION

INSERT OVERWRITE TABLE DIM_COMUNICACION
SELECT
    ROW_NUMBER() OVER (
        ORDER BY 
            tipo_cliente, especificacion, estado, card
    ) AS id_comunicacion,
    
    t.tipo_cliente,
    t.especificacion,
    t.estado,
    t.card,
    
    CASE 
        WHEN t.especificacion = 'SMS' AND t.cantidad > 10000 THEN 1 -- Bandera de limitación
        ELSE 0
    END AS flag_presupuesto
FROM (
    -- Obtener la combinación única de atributos de comunicación
    SELECT DISTINCT 
        tipo_cliente, especificacion, estado, card, cantidad
    FROM 
        REGISTRO_COMUNICACIONES
) AS t;

--INSERCCION A LA TABLA DE HECHO

INSERT OVERWRITE TABLE FACT_UTILIDAD_TRADING
SELECT
  
    ROW_NUMBER() OVER (ORDER BY t.monto) AS id_hecho,
    
    CAST(REGEXP_REPLACE(CAST(CURRENT_DATE() AS STRING), '-', '') AS INT) AS id_fecha, 
    de.id_ejecutivo,
    0 AS id_cliente,
    dc.id_canal,
    0 AS id_comunicacion,
    
    -- Métricas
    t.monto,
    t.desembolsado,
    t.volumen_cambiado,
    t.utilidad
FROM (
    SELECT * FROM TLV_RANKING
    UNION ALL
    SELECT * FROM VIRTUAL_RANKING
    UNION ALL
    SELECT * FROM TIENDAS_RANKING
) t
LEFT JOIN DIM_CANAL dc ON t.canal = dc.nombre_canal
LEFT JOIN DIM_EJECUTIVO de ON t.ejecutivo = de.nombre_ejecutivo;


