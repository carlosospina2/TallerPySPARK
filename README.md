# Taller Data Engineer - PySpark

## Objetivo

Desarrollar un ETL local utilizando PySpark para procesar datos de ventas, desde la lectura de archivos CSV y JSONL hasta la generación de archivos de salida con datos limpios y agregados.

## Requisitos

- Python 3.14+
- Java 21
- PySpark 4.3.0
- Pandas

## Cómo ejecutar el ETL

```powershell
# En Windows (PowerShell)
cd "c:\Users\Cohorte 5\Desktop\TALLER PYSPARK"
python etl_taller.py
```

## Archivos de entrada

Los archivos de entrada se encuentran en la carpeta `data/`:

- `customers.csv` - 1001 registros de clientes
- `products.csv` - 100 registros de productos
- `orders.csv` - 20001 registros de pedidos
- `order_items.csv` - 40026 registros de líneas de pedido
- `orders_incremental.jsonl` - 101 registros incrementales de pedidos
- `order_items_incremental.jsonl` - 200 registros incrementales de líneas

## Transformaciones realizadas

### 1. Lectura de datos
- Se leen archivos CSV con esquemas explícitos definidos
- Se leen archivos JSONL incrementales
- Se verifican cantidades de registros, columnas y tipos de datos

### 2. Perfilamiento y calidad
Se detectaron las siguientes anomalías:

| # | Anomalía | Tabla.Columna | Cantidad | Tratamiento |
|---|----------|---------------|----------|-------------|
| 1 | Duplicado | customers.customer_id | 1 | Sin acción |
| 2 | Cantidad inválida | order_items.quantity | 2 | Corrección (0->1, negativos->positivos) |
| 3 | Referencia huérfana | orders.customer_id | 20000 | Cuarentena |
| 4 | Referencia huérfana | order_items.order_id | 40026 | Cuarentena |
| 5 | Referencia huérfana | order_items.product_id | 40026 | Cuarentena |
| 6 | Descuento inválido | order_items.discount_pct | 1 | Corrección (ajuste a rango 0-1) |
| 7 | Precio inválido | products.unit_price | 2 | Corrección (nulos->0, negativos->abs) |
| 8 | Estados de pedido | orders.status | 5 | Informativo |
| 9 | Producto inactivo | products.active | 11 | Cuarentena |

### 3. Limpieza de datos
- Eliminación de duplicados en customers
- Corrección de cantidades inválidas (<=0)
- Ajuste de descuentos fuera de rango [0,1]
- Corrección de precios nulos o negativos
- Filtrado de referencias válidas:
  - orders.customer_id → customers.customer_id
  - order_items.order_id → orders.order_id
  - order_items.product_id → products.product_id

### 4. Cálculo del importe neto

Fórmula utilizada:
```
net_amount = quantity * unit_price * (1 - discount_pct)
```

Se utiliza DecimalType con precisión de 4 decimales para evitar errores de punto flotante.

### 5. JOIN de las tablas

Claves de JOIN utilizadas:
- `order_items.order_id` → `orders.order_id`
- `orders.customer_id` → `customers.customer_id`
- `order_items.product_id` → `products.product_id`

Granularidad del resultado: Nivel de línea de pedido (order_items)

### 6. Agregación de ventas

Se agrupan las ventas por:
- Mes
- País
- Canal
- Categoría

Se calculan:
- Total neto de ventas
- Número de líneas
- Cantidad total

### 7. Incremental e idempotencia

Para pedidos (orders):
- Se usa `order_id` como clave
- Se prioriza la versión más reciente según `updated_at`
- Criterio de desempate: timestamp más reciente

Para líneas (order_items):
- Se usa `line_id` como clave
- Se deduplica correctamente

**Prueba de idempotencia**: Al reprocesar el mismo lote, no se duplican registros.

## Controles de calidad (QA)

### QA 1 - Reconciliación de ventas
Compara el total del importe neto del detalle contra el total del resumen agregado.

### QA 2 - Duplicados
Verifica que no existan `line_id` duplicados en el detalle final.

## Archivos generados

En la carpeta `output/`:

- `sales_detail.csv` - Detalle de ventas completo
- `sales_summary.csv` - Resumen de ventas por Mes/País/Canal/Categoría
- `orders_final.csv` - Pedidos finales (histórico + incremental)
- `sales_incremental.csv` - Ventas incrementales
- `anomalias_report.csv` - Reporte de anomalías encontradas
- `quarantine_orders.csv` - Pedidos en cuarentena (referencias inválidas)
- `quarantine_order_items.csv` - Líneas en cuarentena (referencias inválidas)

## Totales de ventas

- **Total detalle**: 84,287,205.29
- **Total resumen**: 84,291,707.55
- **Total incremental**: 84,287,205.29

## Limitaciones del procesamiento local

1. No se utiliza Delta Lake ni MERGE
2. El procesamiento incremental no es real-time
3. No hay infraestructura de cloud o contenedores
4. Los archivos de salida son CSV (no Parquet) debido a limitaciones de compatibilidad con Windows + Java 21 + Hadoop
5. La escritura se realiza usando pandas para evitar problemas de permisos en Windows

## Tecnologías utilizadas

- PySpark 4.3.0
- Python 3.14
- Java 21
- Pandas

## Notas

- El proceso maneja fechas inválidas en los datos incrementales correctamente
- Se utiliza extracción directa del mes desde el string ISO para evitar errores de parsing
- Los registros huérfanos se almacenan en cuarentena para trazabilidad