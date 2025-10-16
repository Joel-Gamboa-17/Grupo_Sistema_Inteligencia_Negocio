## EVIDENCIA DOCUMENTAL

### 1. OBJETIVO
Consolidar el flujo completo de datos desde las fuentes OLTP hasta el consumo analítico final en Power BI, evidenciando la trazabilidad de las etapas **Extract – Transform – Load (ETL)** implementadas en Hortonworks Sandbox y la construcción del modelo dimensional de análisis para el área de *Trading* de Interbank.

---

### 2. FLUJO DE DESARROLLO

**ETL general:**

Fuentes (CSV/OLTP) ➜ Ingesta HDFS ➜ Procesamiento en Hive (Raw / Curated) ➜
Modelado Dimensional (Star Schema) ➜ Cubo OLAP ➜ Dashboard Power BI

---

### 3. DESARROLLO TÉCNICO

#### 3.1 Extracción
- Archivos cargados al HDFS desde `/user/maria_dev/sin/`:
  - `cambios_clientes.csv` → Utilidades por cliente y volumen de operaciones.
  - `registro_comunicaciones.csv` → Registros de envíos y performance.
  - `segmentos_repeated.csv` → Segmentación de clientes y comportamiento digital.
  - `tlv_ranking.csv`, `virtual_ranking.csv`, `tiendas_ranking.csv` → Rankings por canal y ejecutivo.
- Creación de tablas externas en Hive con `Scripts_Extraccion.sql` bajo formato **TEXTFILE**, delimitado por comas.

#### 3.2 Transformación
- Aplicación de reglas de negocio en Hive (script `Scripts_Trans_load.sql`):
  - Generación de **claves sustitutas (ROW_NUMBER)** en todas las dimensiones.
  - Cálculo de campos derivados:
    - `flag_fuga`: identifica clientes con baja utilidad (< 500 USD).
    - `flag_presupuesto`: señala limitaciones de envío (SMS > 10K).
  - Integración de fuentes heterogéneas mediante **JOINs** por RUC y código único.
- Conversión de formato a **ORC** para optimización de consultas y almacenamiento eficiente.

#### 3.3 Carga
Tablas creadas y cargadas en la capa **Curated (Data Warehouse)**:

| Tipo | Tabla | Descripción | Formato |
|------|--------|--------------|----------|
| Dimensión | `DIM_CANAL` | Catálogo de canales de trading | ORC |
| Dimensión | `DIM_EJECUTIVO` | Jerarquía de jefes y ejecutivos | ORC |
| Dimensión | `DIM_CLIENTE` | Información del cliente y flag de fuga | ORC |
| Dimensión | `DIM_COMUNICACION` | Tipos de comunicación y presupuesto | ORC |
| Dimensión | `DIM_FECHA` | Calendario de análisis y días útiles | ORC |
| Hechos | `FACT_UTILIDAD_TRADING` | Métricas de utilidad, monto y volumen | ORC |

---

### 4. MODELO ESTRELLA (Star Schema)

**Tabla de Hechos:** `FACT_UTILIDAD_TRADING`  
Contiene los indicadores financieros principales y las claves foráneas hacia las dimensiones.

**Dimensiones:**
- `DIM_FECHA`: analiza la estacionalidad por periodo y días hábiles.
- `DIM_COMUNICACION`: evalúa performance y restricciones de campañas.
- `DIM_CLIENTE`: identifica fugas y comportamiento de clientes.
- `DIM_EJECUTIVO`: mide productividad individual.
- `DIM_CANAL`: analiza los canales más rentables.

---

### 5. CUBO OLAP Y DASHBOARD

#### Cubo OLAP
- Implementado en **Power BI**, conectado al **Hive LLAP**.
- Permite análisis multidimensional por canal, cliente, ejecutivo y periodo.

#### Dashboard Power BI
- **Vista acumulativa:** curva de crecimiento mensual de utilidades.  
- **Vista comparativa:** ranking de ejecutivos por utilidad promedio.  
- **KPI con semáforo:** tasa de fuga de clientes (51 % en rojo).  
- Archivo: `dashboard/Trading_Interbank.pbix`.

---

### 6. LIMITACIONES DETECTADAS
- Acceso restringido a datos históricos (solo desde periodo 202501).
- Versión desfasada de Hive en Ambari Sandbox (incompatibilidad en funciones).
- Dependencia de aprobación de datos sensibles del área comercial.

---

### 7. PROPUESTAS DE MEJORA
1. Calcular **Utilidad por Comunicación Enviada** para medir eficiencia de campañas.  
2. Analizar **tiempo medio entre envío y transacción** para optimizar el *timing* comercial.  
3. Crear **segmento Alto Valor** para priorizar clientes rentables.  
4. Implementar modelo predictivo de **Churn (fuga de clientes)** en Spark MLlib.  
5. Integrar procesamiento en tiempo real con **Kafka + Spark Streaming**.

---


