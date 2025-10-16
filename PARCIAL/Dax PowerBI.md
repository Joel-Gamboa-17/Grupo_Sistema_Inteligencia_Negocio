## Métricas DAX implementadas en Power BI

A continuación se listan las principales medidas creadas en Power BI para el análisis de desempeño y retención del área de *Trading* de Interbank.  
Estas fórmulas están basadas en las tablas del modelo dimensional cargado desde Hive.

---

### En la tabla `[dim_cliente]`

```DAX
Tasa de Fuga Clientes Recurrentes (%) = 
VAR ClientesRecurrentes =
    FILTER(
        dim_cliente,
        dim_cliente[segmento_fx] = "RECURRENTE"
    )
VAR ClientesTotales =
    COUNTROWS(ClientesRecurrentes)
VAR ClientesFugados =
    COUNTROWS(
        FILTER(ClientesRecurrentes, dim_cliente[flag_fuga] = 1)
    )
RETURN
DIVIDE(ClientesFugados, ClientesTotales, 0) * 100
```
```DAX
Color Semáforo Fuga = 
SWITCH(
    TRUE(),
    [Tasa de Fuga Clientes Recurrentes (%)] < 10, "#00B050",   -- Verde
    [Tasa de Fuga Clientes Recurrentes (%)] < 20, "#FFC000",   -- Amarillo
    "#FF0000"                                                  -- Rojo
)
```
### En la tabla `[dim_fecha]`
```DAX
% Días Hábiles Efectivos = 
VAR DiasHabiles = CALCULATE(COUNTROWS(dim_fecha), dim_fecha[dia_util_flag] = 1)
VAR TotalDias = COUNTROWS(dim_fecha)
RETURN
DIVIDE(DiasHabiles, TotalDias, 0) * 100
```

### En la tabla `[fact_utilidad_trading]`
```DAX
Utilidad Acumulada = 
CALCULATE(
    SUM(fact_utilidad_trading[utilidad]),
    FILTER(
        ALLSELECTED(dim_fecha),
        dim_fecha[fecha] <= MAX(dim_fecha[fecha])
    )
)
```
```DAX
Utilidad Promedio por Ejecutivo = 
DIVIDE(
    SUM(fact_utilidad_trading[utilidad]),
    DISTINCTCOUNT(fact_utilidad_trading[id_ejecutivo])
)

```




