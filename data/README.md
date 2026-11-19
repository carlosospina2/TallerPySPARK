# Datos del Taller PySpark

## Estructura de datos

Los archivos en esta carpeta fueron generados a partir del Excel `taller_data_engineer.xlsx` y contienen:

### Archivos CSV
- `customers.csv` - 1001 registros de clientes
- `products.csv` - 100 registros de productos  
- `orders.csv` - 20001 registros de pedidos
- `order_items.csv` - 40026 registros de líneas de pedido

### Archivos JSONL (incrementales)
- `orders_incremental.jsonl` - 101 registros de pedidos incrementales
- `order_items_incremental.jsonl` - 200 registros de líneas incrementales

## Notas importantes

1. Los datos contienen **referencias intencionalmente inválidas** para pruebas de calidad:
   - `customer_id` en orders incluye valores fuera del rango de customers
   - `product_id` en order_items incluye valores fuera del rango de products
   - `order_id` en order_items incluye valores fuera del rango de orders

2. Esto es por diseño para probar la detección de anomalías en el ETL.

3. Los archivos originales del ZIP `taller_data_local.zip` no están disponibles en este repositorio.

## Esquemas

### customers
- `customer_id` (string): ID único del cliente (ej: C0001)
- `first_name` (string): Nombre
- `last_name` (string): Apellido
- `email` (string): Correo electrónico
- `country` (string): País (CL, CO, MX, PE)
- `signup_date` (string): Fecha de registro (YYYY-MM-DD)
- `segment` (string): Segmento del cliente (standard, silver, gold)

### products
- `product_id` (string): ID único del producto (ej: P001)
- `product_name` (string): Nombre del producto
- `category` (string): Categoría (electronics, home, sports, books)
- `unit_price` (float): Precio unitario
- `active` (boolean): Producto activo (true/false)

### orders
- `order_id` (string): ID único del pedido (ej: O00001)
- `customer_id` (string): ID del cliente
- `order_ts` (string): Timestamp del pedido (ISO 8601)
- `status` (string): Estado (delivered, pending, shipped, cancelled)
- `channel` (string): Canal (web, store, mobile)
- `updated_at` (string): Timestamp de actualización (ISO 8601)

### order_items
- `line_id` (string): ID único de línea (ej: L0000001)
- `order_id` (string): ID del pedido
- `product_id` (string): ID del producto
- `quantity` (int): Cantidad
- `discount_pct` (float): Porcentaje de descuento (0-1)