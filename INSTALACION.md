# Guía de Instalación - Taller PySpark

## Requisitos Previos

### Software Necesario
- **Python**: 3.8 o superior (recomendado 3.10+)
- **Java**: JDK 11 o JDK 17 (JDK 21 tiene problemas con Hadoop en Windows)
- **Git**: Para clonar el repositorio

### Verificar Instalaciones
```powershell
# Verificar Python
python --version

# Verificar Java
java -version

# Verificar Git
git --version
```

## Instalación Paso a Paso

### 1. Clonar el Repositorio
```powershell
git clone https://github.com/carlosospina2/TallerPySPARK.git
cd TallerPySPARK
```

### 2. Crear Entorno Virtual (Recomendado)
```powershell
# Crear entorno virtual
python -m venv venv

# Activar entorno virtual
# En Windows PowerShell:
.\venv\Scripts\Activate.ps1

# En Windows CMD:
.\venv\Scripts\activate.bat
```

### 3. Instalar Dependencias
```powershell
pip install -r requirements.txt
```

### 4. Configuración para Windows

#### Opción A: Usar Java 11 (Recomendado)
Si tienes problemas con Parquet en Windows:
1. Instalar Java 11 o 17
2. Configurar JAVA_HOME apuntando a Java 11/17
3. El script usará CSV automáticamente si hay problemas

#### Opción B: Configurar Hadoop (Avanzado)
```powershell
# Descargar winutils.exe compatible
# Colocarlo en c:\hadoop\bin\
# Configurar variables de entorno
$env:HADOOP_HOME = "c:\hadoop"
$env:PATH = "c:\hadoop\bin;$env:PATH"
```

### 5. Preparar Datos

Los datos se encuentran en el archivo Excel `taller_data_engineer.xlsx`. El ETL los exportará automáticamente a CSV/JSONL en la primera ejecución.

## Ejecución del ETL

### Ejecución Básica
```powershell
python etl_taller.py
```

### Salida Esperada
El script creará:
- `data/` - Archivos CSV y JSONL exportados
- `output/` - Resultados del ETL:
  - `sales_detail.csv` - Detalle de ventas
  - `sales_summary.csv` - Resumen agregado
  - `orders_final.csv` - Pedidos procesados
  - `sales_incremental.csv` - Datos incrementales
  - `anomalias_report.csv` - Reporte de anomalías
  - `quarantine_*.csv` - Registros en cuarentena

## Solución de Problemas

### Problema: "No module named 'pyspark'"
**Solución**: Instalar dependencias
```powershell
pip install pyspark
```

### Problema: Error de Java/Hadoop en Windows
**Solución**: 
1. Usar Java 11 o 17 en lugar de Java 21
2. O dejar que el script use CSV en lugar de Parquet (comportamiento por defecto)

### Problema: "Unable to load native-hadoop library"
**Solución**: Este es solo un WARNING, puede ignorarse. El script funciona correctamente.

### Problema: Archivos de salida no se generan
**Solución**: 
1. Verificar que el directorio `output/` exista
2. Verificar permisos de escritura
3. Revisar logs del ETL

## Estructura del Proyecto

```
TallerPySPARK/
├── README.md              # Documentación principal
├── INSTALACION.md         # Esta guía
├── requirements.txt       # Dependencias Python
├── etl_taller.py         # Script ETL principal
├── taller_data_engineer.xlsx  # Datos fuente
├── Taller_Spark_.pdf     # Consigna del taller
├── data/                 # Datos exportados (generados)
│   ├── README.md
│   ├── customers.csv
│   ├── products.csv
│   ├── orders.csv
│   ├── order_items.csv
│   ├── orders_incremental.jsonl
│   └── order_items_incremental.jsonl
├── output/               # Resultados del ETL (generados)
│   ├── sales_detail.csv
│   ├── sales_summary.csv
│   └── ...
└── docs/                 # Documentación técnica
    ├── ANALISIS_TECNICO.md
    └── README_PROYECTO.md
```

## Verificación de la Instalación

Para verificar que todo está correctamente instalado:

```powershell
# Ejecutar un test rápido de PySpark
python -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.master('local[*]').getOrCreate(); print('¡PySpark funciona!'); spark.stop()"
```

Si ves el mensaje "¡PySpark funciona!", la instalación es correcta.

## Próximos Pasos

1. Leer el `README.md` para entender el proyecto
2. Revisar `docs/ANALISIS_TECNICO.md` para detalles de implementación
3. Ejecutar `python etl_taller.py` para procesar los datos
4. Revisar los archivos en `output/` para ver los resultados

## Soporte

Para problemas o preguntas:
- Revisar la documentación en `docs/`
- Consultar el README principal
- Revisar los logs del ETL en `etl_output.txt` (si existe)