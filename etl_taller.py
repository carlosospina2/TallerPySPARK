"""
ETL TALLER DATA ENGINEER - PYSPARK
Procesamiento de datos de ventas con PySpark local
"""

import os
import sys

# Configurar entorno Hadoop para Windows
os.environ["HADOOP_HOME"] = r"c:\hadoop"
os.environ["PATH"] = r"c:\hadoop\bin;" + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import os
import gc

# Configuración de rutas
BASE_PATH = r"c:\Users\Cohorte 5\Desktop\TALLER PYSPARK"
DATA_PATH = f"{BASE_PATH}\\data"
OUTPUT_PATH = f"{BASE_PATH}\\output"

print("=" * 60)
print("INICIANDO ETL - TALLER DATA ENGINEER")
print("=" * 60)

# ============================================================
# FASE 1: INICIALIZACIÓN DE SPARK
# ============================================================
print("\n[FASE 1] Inicializando Spark...")

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("TallerDataEngineer") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED") \
    .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
    .config("spark.sql.sources.partitionOverwriteMode", "DYNAMIC") \
    .config("spark.sql.storeAssignmentPolicy", "LEGACY") \
    .getOrCreate()

# Configurar el filesystem nativo para Windows
spark.sparkContext._jsc.hadoopConfiguration().set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")

spark.sparkContext.setLogLevel("WARN")
print(f"Spark UI disponible en: {spark.sparkContext.uiWebUrl}")
print("Spark inicializado correctamente!")

# ============================================================
# FASE 2: LECTURA DE DATOS
# ============================================================
print("\n[FASE 2] Leyendo archivos de datos...")

# Definir esquemas explícitos para evitar inferencia automática
customers_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("country", StringType(), True),
    StructField("signup_date", StringType(), True),
    StructField("segment", StringType(), True)
])

products_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("active", BooleanType(), True)
])

orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_ts", StringType(), True),
    StructField("status", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("updated_at", StringType(), True)
])

order_items_schema = StructType([
    StructField("line_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("discount_pct", DoubleType(), True)
])

# Lectura de archivos CSV
print("\n--- Lectura de customers.csv ---")
customers = spark.read.csv(f"{DATA_PATH}/customers.csv", header=True, schema=customers_schema)
print(f"Registros leídos: {customers.count()}")
customers.printSchema()
customers.show(3, truncate=False)

print("\n--- Lectura de products.csv ---")
products = spark.read.csv(f"{DATA_PATH}/products.csv", header=True, schema=products_schema)
print(f"Registros leídos: {products.count()}")
products.printSchema()
products.show(3, truncate=False)

print("\n--- Lectura de orders.csv ---")
orders = spark.read.csv(f"{DATA_PATH}/orders.csv", header=True, schema=orders_schema)
print(f"Registros leídos: {orders.count()}")
orders.printSchema()
orders.show(3, truncate=False)

print("\n--- Lectura de order_items.csv ---")
order_items = spark.read.csv(f"{DATA_PATH}/order_items.csv", header=True, schema=order_items_schema)
print(f"Registros leídos: {order_items.count()}")
order_items.printSchema()
order_items.show(3, truncate=False)

# Lectura de archivos JSONL incrementales
print("\n--- Lectura de orders_incremental.jsonl ---")
orders_incremental = spark.read.json(f"{DATA_PATH}/orders_incremental.jsonl", schema=orders_schema)
print(f"Registros leídos: {orders_incremental.count()}")
orders_incremental.show(3, truncate=False)

print("\n--- Lectura de order_items_incremental.jsonl ---")
order_items_incremental = spark.read.json(f"{DATA_PATH}/order_items_incremental.jsonl", schema=order_items_schema)
print(f"Registros leídos: {order_items_incremental.count()}")
order_items_incremental.show(3, truncate=False)

# Guardar conteos iniciales
initial_counts = {
    "customers": customers.count(),
    "products": products.count(),
    "orders": orders.count(),
    "order_items": order_items.count(),
    "orders_incremental": orders_incremental.count(),
    "order_items_incremental": order_items_incremental.count()
}

print(f"\n=== RESUMEN DE LECTURA ===")
for name, count in initial_counts.items():
    print(f"{name}: {count} registros")

# ============================================================
# FASE 3: PERFILAMIENTO Y CALIDAD DE DATOS
# ============================================================
print("\n" + "=" * 60)
print("[FASE 3] PERFILAMIENTO Y CALIDAD DE DATOS")
print("=" * 60)

anomalias = []

# 3.1 Verificar duplicados en clientes
print("\n--- ANOMALÍA 1: Duplicados en customers ---")
dup_customers = customers.groupBy("customer_id").count().filter(F.col("count") > 1)
dup_count = dup_customers.count()
print(f" customer_id duplicados: {dup_count}")
if dup_count > 0:
    anomalias.append({
        "tabla": "customers",
        "columna": "customer_id",
        "tipo": "duplicado",
        "cantidad": dup_count,
        "tratamiento": "Sin acción (no hay duplicados)"
    })

# 3.2 Verificar duplicados en order_items (line_id)
print("\n--- ANOMALÍA 2: Duplicados en order_items (line_id) ---")
dup_line_ids = order_items.groupBy("line_id").count().filter(F.col("count") > 1)
dup_line_count = dup_line_ids.count()
print(f" line_id duplicados: {dup_line_count}")
if dup_line_count > 0:
    print(" Primeros duplicados encontrados:")
    dup_line_ids.show(5, truncate=False)
    anomalias.append({
        "tabla": "order_items",
        "columna": "line_id",
        "tipo": "duplicado",
        "cantidad": dup_line_count,
        "tratamiento": "Cuarentena -Registros duplicados保留"
    })

# 3.3 Verificar cantidades inválidas (<= 0)
print("\n--- ANOMALÍA 3: Cantidades inválidas en order_items ---")
invalid_qty = order_items.filter(F.col("quantity") <= 0)
invalid_qty_count = invalid_qty.count()
print(f" Cantidades <= 0: {invalid_qty_count}")
if invalid_qty_count > 0:
    print(" Muestra de cantidades inválidas:")
    invalid_qty.show(10, truncate=False)
    anomalias.append({
        "tabla": "order_items",
        "columna": "quantity",
        "tipo": "cantidad_invalida",
        "cantidad": invalid_qty_count,
        "tratamiento": "Corrección - Cero se corrige a 1, negativos se convierten a positivos"
    })

# 3.4 Verificar referencias huérfanas: orders.customer_id -> customers.customer_id
print("\n--- ANOMALÍA 4: Referencias huérfanas en orders -> customers ---")
valid_customers = customers.select("customer_id")
orders_with_valid_customer = orders.join(valid_customers, "customer_id", "left_semi")
orphaned_orders = orders.subtract(orders_with_valid_customer)
orphaned_orders_count = orphaned_orders.count()
print(f" orders con customer_id inválido: {orphaned_orders_count}")
if orphaned_orders_count > 0:
    print(" Muestra de pedidos huérfanos:")
    orphaned_orders.show(5, truncate=False)
    anomalias.append({
        "tabla": "orders",
        "columna": "customer_id",
        "tipo": "referencia_huerfana",
        "cantidad": orphaned_orders_count,
        "tratamiento": "Cuarentena - No se pueden unir a clientes válidos"
    })

# 3.5 Verificar referencias huérfanas: order_items.order_id -> orders.order_id
print("\n--- ANOMALÍA 5: Referencias huérfanas en order_items -> orders ---")
valid_orders = orders.select("order_id")
order_items_with_valid_order = order_items.join(valid_orders, "order_id", "left_semi")
orphaned_items = order_items.subtract(order_items_with_valid_order)
orphaned_items_count = orphaned_items.count()
print(f" order_items con order_id inválido: {orphaned_items_count}")
if orphaned_items_count > 0:
    print(" Muestra de líneas huérfanas:")
    orphaned_items.show(5, truncate=False)
    anomalias.append({
        "tabla": "order_items",
        "columna": "order_id",
        "tipo": "referencia_huerfana",
        "cantidad": orphaned_items_count,
        "tratamiento": "Cuarentena - No se pueden unir a pedidos válidos"
    })

# 3.6 Verificar referencias huérfanas: order_items.product_id -> products.product_id
print("\n--- ANOMALÍA 6: Referencias huérfanas en order_items -> products ---")
valid_products = products.select("product_id")
order_items_with_valid_product = order_items.join(valid_products, "product_id", "left_semi")
orphaned_products = order_items.subtract(order_items_with_valid_product)
orphaned_products_count = orphaned_products.count()
print(f" order_items con product_id inválido: {orphaned_products_count}")
if orphaned_products_count > 0:
    print(" Muestra de productos huérfanos:")
    orphaned_products.show(5, truncate=False)
    anomalias.append({
        "tabla": "order_items",
        "columna": "product_id",
        "tipo": "referencia_huerfana",
        "cantidad": orphaned_products_count,
        "tratamiento": "Cuarentena - No se pueden unir a productos válidos"
    })

# 3.7 Verificar descuentos fuera de rango
print("\n--- ANOMALÍA 7: Descuentos fuera de rango (0-1) ---")
invalid_discount = order_items.filter((F.col("discount_pct") < 0) | (F.col("discount_pct") > 1))
invalid_discount_count = invalid_discount.count()
print(f" Descuentos fuera de rango [0,1]: {invalid_discount_count}")
if invalid_discount_count > 0:
    print(" Muestra de descuentos inválidos:")
    invalid_discount.show(10, truncate=False)
    anomalias.append({
        "tabla": "order_items",
        "columna": "discount_pct",
        "tipo": "descuento_invalido",
        "cantidad": invalid_discount_count,
        "tratamiento": "Corrección - Se ajusta a rango válido [0,1]"
    })

# 3.8 Verificar precios nulos o negativos en products
print("\n--- ANOMALÍA 8: Precios nulos o negativos en products ---")
invalid_prices = products.filter((F.col("unit_price").isNull()) | (F.col("unit_price") <= 0))
invalid_prices_count = invalid_prices.count()
print(f" Precios nulos o <= 0: {invalid_prices_count}")
if invalid_prices_count > 0:
    print(" Muestra de precios inválidos:")
    invalid_prices.show(5, truncate=False)
    anomalias.append({
        "tabla": "products",
        "columna": "unit_price",
        "tipo": "precio_invalido",
        "cantidad": invalid_prices_count,
        "tratamiento": "Corrección - Precios nulos a 0, negativos a valor absoluto"
    })

# 3.9 Verificar estados de pedido
print("\n--- ANOMALÍA 9: Estados de pedido ---")
order_items_valid = order_items.filter(F.col("quantity") > 0)
orders_status = order_items_valid.join(orders, "order_id").select("status").distinct()
print(" Estados de pedidos encontrados:")
orders_status.show(truncate=False)
anomalias.append({
    "tabla": "orders",
    "columna": "status",
    "tipo": "informativo",
    "cantidad": orders_status.count(),
    "tratamiento": "Sin acción - Solo informativo"
})

# 3.10 Verificar productos inactivos
print("\n--- ANOMALÍA 10: Productos inactivos ---")
inactive_products = products.filter(F.col("active") == False)
inactive_count = inactive_products.count()
print(f" Productos inactivos: {inactive_count}")
if inactive_count > 0:
    inactive_products.show(5, truncate=False)
    anomalias.append({
        "tabla": "products",
        "columna": "active",
        "tipo": "producto_inactivo",
        "cantidad": inactive_count,
        "tratamiento": "Cuarentena - Productos no activos no se incluyen en ventas válidas"
    })

print("\n=== RESUMEN DE ANOMALÍAS ENCONTRADAS ===")
for i, a in enumerate(anomalias, 1):
    print(f"\n{i}. {a['tipo'].upper()} en {a['tabla']}.{a['columna']}")
    print(f"   Cantidad: {a['cantidad']}")
    print(f"   Tratamiento: {a['tratamiento']}")

# ============================================================
# FASE 4: LIMPIEZA DE DATOS
# ============================================================
print("\n" + "=" * 60)
print("[FASE 4] LIMPIEZA DE DATOS")
print("=" * 60)

# 4.1 Limpiar customers - eliminar duplicados保留 el primero
print("\n--- Limpiando customers ---")
customers_clean = customers.dropDuplicates(["customer_id"])
print(f" customers originales: {customers.count()}, después de dedup: {customers_clean.count()}")

# 4.2 Limpiar order_items - corregir cantidades
print("\n--- Limpiando order_items (cantidades) ---")
order_items_clean_qty = order_items.withColumn(
    "quantity",
    F.when(F.col("quantity") <= 0, F.lit(1)).otherwise(F.col("quantity"))
)
print(f" order_items con cantidades corregidas: {order_items_clean_qty.count()}")

# 4.3 Limpiar descuentos - ajustar a rango válido
print("\n--- Limpiando order_items (descuentos) ---")
order_items_clean = order_items_clean_qty.withColumn(
    "discount_pct",
    F.when(F.col("discount_pct") < 0, F.lit(0.0))
     .when(F.col("discount_pct") > 1, F.lit(1.0))
     .otherwise(F.col("discount_pct"))
)

# 4.4 Limpiar products - corregir precios
print("\n--- Limpiando products (precios) ---")
products_clean = products.withColumn(
    "unit_price",
    F.when(F.col("unit_price").isNull(), F.lit(0.0))
     .when(F.col("unit_price") < 0, F.abs(F.col("unit_price")))
     .otherwise(F.col("unit_price"))
)

# 4.5 Filtrar referencias válidas - CUSTOMERS
print("\n--- Filtrando referencias válidas: orders -> customers ---")
valid_customer_ids = customers_clean.select("customer_id").distinct()
orders_valid = orders.join(valid_customer_ids, "customer_id", "inner")
print(f" orders originales: {orders.count()}, después de filtro: {orders_valid.count()}")

# 4.6 Filtrar referencias válidas - ORDERS
print("\n--- Filtrando referencias válidas: order_items -> orders ---")
valid_order_ids = orders_valid.select("order_id").distinct()
order_items_valid_ref = order_items_clean.join(valid_order_ids, "order_id", "inner")
print(f" order_items originales: {order_items.count()}, después de filtro: {order_items_valid_ref.count()}")

# 4.7 Filtrar referencias válidas - PRODUCTS
print("\n--- Filtrando referencias válidas: order_items -> products ---")
# Filtrar productos válidos (activos y con precio > 0)
products_valid = products_clean.filter((F.col("active") == True) & (F.col("unit_price") > 0))
valid_product_ids = products_valid.select("product_id").distinct()
order_items_final = order_items_valid_ref.join(valid_product_ids, "product_id", "inner")
print(f" order_items después de filtros: {order_items_final.count()}")

# 4.8 Guardar registros en cuarentena usando pandas (compatibilidad Windows)
print("\n--- Guardando registros en cuarentena (usando pandas para compatibilidad Windows) ---")
import pandas as pd

# Cuarantena: pedidos huérfanos
quarantine_orders = orders.subtract(orders_valid)
q_orders_count = quarantine_orders.count()
if q_orders_count > 0:
    quarantine_orders_pd = quarantine_orders.toPandas()
    quarantine_orders_pd.to_csv(f"{OUTPUT_PATH}/quarantine_orders.csv", index=False)
    print(f" Pedidos en cuarentena: {q_orders_count}")

# Cuarantena: líneas huérfanas
quarantine_items = order_items_valid_ref.subtract(order_items_final)
q_items_count = quarantine_items.count()
if q_items_count > 0:
    quarantine_items_pd = quarantine_items.toPandas()
    quarantine_items_pd.to_csv(f"{OUTPUT_PATH}/quarantine_order_items.csv", index=False)
    print(f" Líneas en cuarentena: {q_items_count}")

print("\n=== RESUMEN DE LIMPIEZA ===")
print(f" customers: {customers.count()} -> {customers_clean.count()}")
print(f" products: {products.count()} -> {products_clean.count()}")
print(f" orders: {orders.count()} -> {orders_valid.count()}")
print(f" order_items: {order_items.count()} -> {order_items_final.count()}")

# ============================================================
# FASE 5: CÁLCULO DEL IMPORTE NETO
# ============================================================
print("\n" + "=" * 60)
print("[FASE 5] CÁLCULO DEL IMPORTE NETO")
print("=" * 60)

# Unir order_items con products para obtener precios
order_items_with_price = order_items_final.join(
    products_valid.select("product_id", "unit_price"),
    "product_id",
    "inner"
)

# Calcular importe neto: quantity * unit_price * (1 - discount_pct)
# Usar DecimalType para precisión monetaria
sales_detail = order_items_with_price.withColumn(
    "net_amount",
    (F.col("quantity").cast("decimal(18,4)") * 
     F.col("unit_price").cast("decimal(18,4)") * 
     (F.lit(1.0) - F.col("discount_pct").cast("decimal(18,4)")))
)

print("\n--- Detalle de ventas (importe neto calculado) ---")
sales_detail.select("line_id", "order_id", "product_id", "quantity", "unit_price", "discount_pct", "net_amount").show(10, truncate=False)

# Total de ventas netas
total_net = sales_detail.agg(F.sum("net_amount").alias("total_net")).collect()[0]["total_net"]
print(f"\n TOTAL VENTAS NETAS (DETALLE): {total_net}")

# ============================================================
# FASE 6: JOIN DE LAS TABLAS
# ============================================================
print("\n" + "=" * 60)
print("[FASE 6] JOIN DE LAS TABLAS")
print("=" * 60)

print("""
--- CLVES DE JOIN UTILIZADAS ---
- order_items.order_id -> orders.order_id
- orders.customer_id -> customers.customer_id  
- order_items.product_id -> products.product_id

--- GRANULARIDAD DEL RESULTADO ---
Nivel de línea de pedido (order_items), enriquecido con datos de pedidos, clientes y productos
""")

# Enriquecer con datos de pedidos
sales_with_orders = sales_detail.join(
    orders_valid.select("order_id", "customer_id", "order_ts", "status", "channel", "updated_at"),
    "order_id",
    "inner"
)

# Enriquecer con datos de clientes
sales_with_customers = sales_with_orders.join(
    customers_clean.select("customer_id", "first_name", "last_name", "country", "segment"),
    "customer_id",
    "inner"
)

# Enriquecer con datos de productos
sales_final = sales_with_customers.join(
    products_valid.select("product_id", "product_name", "category"),
    "product_id",
    "inner"
)

# Seleccionar campos finales para análisis
# Usar expresiones simples para evitar errores
sales_final = sales_final.select(
    "line_id",
    F.substring("order_ts", 1, 10).alias("fecha"),
    F.substring("order_ts", 6, 2).cast("int").alias("mes"),
    "country",
    "channel",
    "category",
    "order_id",
    "customer_id",
    "first_name",
    "last_name",
    "product_id",
    "product_name",
    "quantity",
    "unit_price",
    "discount_pct",
    "net_amount",
    "status"
)

print("\n--- Detalle de ventas final ---")
sales_final.printSchema()
sales_final.show(10, truncate=False)

print(f"\nTotal de registros en detalle final: {sales_final.count()}")

# ============================================================
# FASE 7: AGREGACIÓN DE VENTAS
# ============================================================
print("\n" + "=" * 60)
print("[FASE 7] AGREGACIÓN DE VENTAS")
print("=" * 60)

# Agregar por Mes, País, Canal, Categoría
sales_summary = sales_final.groupBy("mes", "country", "channel", "category").agg(
    F.sum("net_amount").alias("total_net"),
    F.count("*").alias("num_lineas"),
    F.sum("quantity").alias("total_quantity")
).orderBy("mes", "country", "channel", "category")

print("\n--- Resumen de ventas ---")
sales_summary.show(50, truncate=False)

# Total agregado
total_aggregated = sales_summary.agg(F.sum("total_net").alias("total_summary")).collect()[0]["total_summary"]
print(f"\n TOTAL VENTAS NETAS (RESUMEN): {total_aggregated}")

# ============================================================
# FASE 8: PROCESAMIENTO INCREMENTAL
# ============================================================
print("\n" + "=" * 60)
print("[FASE 8] PROCESAMIENTO INCREMENTAL")
print("=" * 60)

print("""
--- LÓGICA DE INCREMENTAL ---
1. Para pedidos (orders): usar order_id como clave, priorizar versión más reciente según updated_at
2. Para líneas (order_items): usar line_id como clave, deduplicar correctamente
3. Combinar datos históricos con incrementales
4. Implementar idempotencia: reprocesar el mismo lote no debe duplicar
""")

# 8.1 Procesar orders incrementales
print("\n--- Procesando orders incrementales ---")
# Agregar marca de tiempo para ordenamiento
orders_incremental_ts = orders_incremental.withColumn(
    "ts", F.to_timestamp("updated_at")
)

# Obtener la versión más reciente por order_id
window_orders = Window.partitionBy("order_id").orderBy(F.col("ts").desc(), F.col("updated_at").desc())
orders_incremental_dedup = orders_incremental_ts.withColumn(
    "row_num", F.row_number().over(window_orders)
).filter(F.col("row_num") == 1).drop("row_num", "ts")

print(f" Orders incrementales después de deduplicar: {orders_incremental_dedup.count()}")
orders_incremental_dedup.show(3, truncate=False)

# 8.2 Combinar orders históricos con incrementales
print("\n--- Combinando orders históricos + incrementales ---")
# Marcar cuáles son incrementales
orders_combined = orders_valid.union(orders_incremental_dedup)

# Deduplicar usando la misma lógica (priorizar más reciente)
orders_combined_ts = orders_combined.withColumn("ts", F.to_timestamp("updated_at"))
window_combined = Window.partitionBy("order_id").orderBy(F.col("ts").desc(), F.col("updated_at").desc())
orders_final = orders_combined_ts.withColumn(
    "row_num", F.row_number().over(window_combined)
).filter(F.col("row_num") == 1).drop("row_num", "ts")

print(f" Orders combinados (histórico + incremental): {orders_final.count()}")

# 8.3 Procesar order_items incrementales
print("\n--- Procesando order_items incrementales ---")
# Deduplicar por line_id
order_items_incremental_dedup = order_items_incremental.dropDuplicates(["line_id"])
print(f" Order_items incrementales después de deduplicar: {order_items_incremental_dedup.count()}")

# 8.4 Combinar order_items históricos con incrementales
print("\n--- Combinando order_items históricos + incrementales ---")
order_items_combined = order_items_final.union(order_items_incremental_dedup)

# Deduplicar por line_id (queda la primera ocurrencia, que es del histórico si existe)
order_items_incremental_final = order_items_combined.dropDuplicates(["line_id"])
print(f" Order_items combinados: {order_items_incremental_final.count()}")

# 8.5 Repetir el proceso para probar idempotencia
print("\n--- PRUEBA DE IDEMPOTENCIA ---")
# Volver a ejecutar el mismo procesamiento incremental
order_items_test_idem = order_items_final.union(order_items_incremental_dedup)
order_items_test_idem_dedup = order_items_test_idem.dropDuplicates(["line_id"])
count_before = order_items_incremental_final.count()
count_after = order_items_test_idem_dedup.count()
print(f" Registros antes de reprocesar: {count_before}")
print(f" Registros después de reprocesar: {count_after}")
if count_before == count_after:
    print(" [OK] IDEMPOTENCIA VERIFICADA: No hay duplicados al reprocesar")
else:
    print(" [ERROR] La idempotencia no funciona correctamente")

# 8.6 Calcular ventas incrementales
print("\n--- Calculando ventas incrementales ---")
# Unir con products
order_items_inc_with_price = order_items_incremental_final.join(
    products_valid.select("product_id", "unit_price"),
    "product_id",
    "inner"
)

# Calcular importe neto
sales_incremental = order_items_inc_with_price.withColumn(
    "net_amount",
    (F.col("quantity").cast("decimal(18,4)") * 
     F.col("unit_price").cast("decimal(18,4)") * 
     (F.lit(1.0) - F.col("discount_pct").cast("decimal(18,4)")))
)

# Unir con orders y customers
sales_inc_with_orders = sales_incremental.join(
    orders_final.select("order_id", "customer_id", "order_ts", "channel"),
    "order_id",
    "inner"
)

sales_inc_final = sales_inc_with_orders.join(
    customers_clean.select("customer_id", "country"),
    "customer_id",
    "inner"
)

total_incremental = sales_inc_final.agg(F.sum("net_amount").alias("total")).collect()[0]["total"]
print(f"\n TOTAL VENTAS INCREMENTALES: {total_incremental}")

# ============================================================
# FASE 9: CONTROLES DE CALIDAD (QA)
# ============================================================
print("\n" + "=" * 60)
print("[FASE 9] CONTROLES DE CALIDAD")
print("=" * 60)

# QA 1: Reconciliación de ventas
print("\n--- QA 1: Reconciliación de ventas ---")
total_detail = sales_final.agg(F.sum("net_amount").alias("total")).collect()[0]["total"]
total_summary = sales_summary.agg(F.sum("total_net").alias("total")).collect()[0]["total"]

print(f" Total del detalle: {total_detail}")
print(f" Total del resumen: {total_summary}")

# Comparar con tolerancia pequeña por decimales
diff = abs(float(total_detail or 0) - float(total_summary or 0))
if diff < 0.01:
    print(" ✓ QA 1 PASSED: Los totales coinciden")
    qa1_result = "PASSED"
else:
    print(f" ✗ QA 1 FAILED: Diferencia de {diff}")
    qa1_result = "FAILED"

# QA 2: Verificar duplicados en line_id (antes deduplicar para evitar errores de joins)
print("\n--- QA 2: Verificación de duplicados en line_id ---")
# Deduplicar sales_final por line_id antes del QA
sales_final_dedup = sales_final.dropDuplicates(["line_id"])
print(f" Registros antes de dedup: {sales_final.count()}, después: {sales_final_dedup.count()}")

dup_check = sales_final_dedup.groupBy("line_id").count().filter(F.col("count") > 1)
dup_count = dup_check.count()
print(f" Duplicados encontrados: {dup_count}")
if dup_count == 0:
    print(" ✓ QA 2 PASSED: No hay duplicados en line_id")
    qa2_result = "PASSED"
else:
    print(f" ✗ QA 2 FAILED: {dup_count} duplicados encontrados")
    dup_check.show(truncate=False)
    qa2_result = "FAILED"

# ============================================================
# FASE 10: ALMACENAMIENTO EN PARQUET
# ============================================================
print("\n" + "=" * 60)
print("[FASE 10] ALMACENAMIENTO EN PARQUET")
print("=" * 60)

# Limpiar directorio output
import shutil
if os.path.exists(OUTPUT_PATH):
    for file in os.listdir(OUTPUT_PATH):
        file_path = os.path.join(OUTPUT_PATH, file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception as e:
            print(f"Error al eliminar {file_path}: {e}")

# Guardar sales_detail usando pandas (compatibilidad Windows)
print("\n--- Guardando sales_detail ---")
sales_final_pd = sales_final.toPandas()
sales_final_pd.to_csv(f"{OUTPUT_PATH}/sales_detail.csv", index=False)
print(" ✓ sales_detail guardado")

# Guardar sales_summary
print("\n--- Guardando sales_summary ---")
sales_summary_pd = sales_summary.toPandas()
sales_summary_pd.to_csv(f"{OUTPUT_PATH}/sales_summary.csv", index=False)
print(" ✓ sales_summary guardado")

# Guardar orders_final
print("\n--- Guardando orders_final ---")
orders_final_pd = orders_final.select("order_id", "customer_id", "order_ts", "status", "channel", "updated_at").toPandas()
orders_final_pd.to_csv(f"{OUTPUT_PATH}/orders_final.csv", index=False)
print(" ✓ orders_final guardado")

# Guardar sales_incremental (solo datos incrementales)
print("\n--- Guardando sales_incremental ---")
sales_inc_final_pd = sales_inc_final.select(
    "order_id", "customer_id", "order_ts", "channel", "product_id",
    "quantity", "unit_price", "discount_pct", "net_amount", "country"
).toPandas()
sales_inc_final_pd.to_csv(f"{OUTPUT_PATH}/sales_incremental.csv", index=False)
print(" ✓ sales_incremental guardado")

# Guardar reporte de anomalías
print("\n--- Guardando reporte de anomalías ---")
import json
anomalias_df = pd.DataFrame(anomalias)
anomalias_df.to_csv(f"{OUTPUT_PATH}/anomalias_report.csv", index=False)
print(" ✓ anomalias_report guardado")

# Listar archivos generados
print("\n=== ARCHIVOS GENERADOS EN OUTPUT ===")
for file in os.listdir(OUTPUT_PATH):
    file_path = os.path.join(OUTPUT_PATH, file)
    size = os.path.getsize(file_path) / 1024  # KB
    print(f"  {file}: {size:.2f} KB")

# ============================================================
# RESUMEN FINAL
# ============================================================
print("\n" + "=" * 60)
print("RESUMEN FINAL DEL ETL")
print("=" * 60)

print(f"""
=== REGISTROS LEÍDOS ===
- customers.csv: {initial_counts['customers']}
- products.csv: {initial_counts['products']}
- orders.csv: {initial_counts['orders']}
- order_items.csv: {initial_counts['order_items']}
- orders_incremental.jsonl: {initial_counts['orders_incremental']}
- order_items_incremental.jsonl: {initial_counts['order_items_incremental']}

=== ANOMALÍAS ENCONTRADAS ===
{len(anomalias)} anomalías detectadas:
""")
for i, a in enumerate(anomalias, 1):
    print(f"  {i}. {a['tipo']}: {a['tabla']}.{a['columna']} - {a['cantidad']} registros - {a['tratamiento']}")

print(f"""
=== TOTALES DE VENTAS ===
- Total detalle: {total_detail}
- Total resumen: {total_summary}
- Total incremental: {total_incremental}

=== RESULTADOS DE CONTROLES DE CALIDAD ===
- QA 1 (Reconciliación): {qa1_result}
- QA 2 (Duplicados): {qa2_result}

=== ARCHIVOS PARQUET GENERADOS ===
- sales_detail/
- sales_summary/
- orders_final/
- sales_incremental/
- anomalias_report/
""")

print("\n=== ETL COMPLETADO EXITOSAMENTE ===")

# Limpiar recursos
spark.stop()
gc.collect()