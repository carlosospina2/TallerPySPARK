# 🚀 Taller Data Engineer - PySpark

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-4.3.0-orange.svg)](https://spark.apache.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## 📋 Descripción

ETL completo desarrollado con PySpark para procesamiento de datos de ventas con enfoque en calidad de datos, procesamiento incremental e idempotencia.

### Características Principales
- ✅ Detección automática de 9 tipos de anomalías
- ✅ Procesamiento incremental con idempotencia garantizada
- ✅ Controles de calidad automatizados (QA)
- ✅ Cuarentena inteligente de registros problemáticos
- ✅ Reconciliación automática de ventas

## ⚡ Inicio Rápido

```bash
# 1. Clonar repositorio
git clone https://github.com/carlosospina2/TallerPySPARK.git
cd TallerPySPARK

# 2. Instalar dependencias
pip install -r requirements.txt

# 3. Ejecutar ETL
python etl_taller.py
```

📖 Ver **[INSTALACION.md](INSTALACION.md)** para instrucciones detalladas

## 📊 Resultados

| Archivo | Descripción | Registros |
|---------|-------------|-----------|
| `sales_detail.csv` | Detalle de ventas válidas | 35,354 |
| `sales_summary.csv` | Resumen agregado | 147 |
| `orders_final.csv` | Pedidos procesados | 20,099 |
| `anomalias_report.csv` | Reporte de anomalías | 9 tipos |
| `quarantine_*.csv` | Registros en cuarentena | 24,672 |

📈 Ver **[RESULTADOS_EJEMPLO.md](RESULTADOS_EJEMPLO.md)** para métricas detalladas

## 🎯 Flujo del ETL

```mermaid
graph LR
    A[CSV/JSONL] --> B[Lectura]
    B --> C[Perfilamiento]
    C --> D[Limpieza]
    D --> E[Cálculo]
    E --> F[JOINs]
    F --> G[Agregación]
    G --> H[QA]
    H --> I[Output CSV]
```

### Fases del Proceso

1. **Lectura**: CSV con esquemas explícitos + JSONL incrementales
2. **Perfilamiento**: Detección de 9 tipos de anomalías
3. **Limpieza**: Corrección + filtrado + cuarentena
4. **Cálculo**: `net_amount = quantity × unit_price × (1 - discount_pct)`
5. **JOINs**: Relaciones customers/orders/products
6. **Agregación**: Por Mes/País/Canal/Categoría
7. **Incremental**: Merge por clave + timestamp
8. **QA**: Reconciliación + duplicados
9. **Output**: Archivos CSV con resultados

## 🔍 Anomalías Detectadas

El ETL detecta y maneja automáticamente:

| Tipo | Registros | Tratamiento |
|------|-----------|-------------|
| Referencias huérfanas | 20,000+ | Cuarentena |
| Cantidades inválidas | 2 | Corrección |
| Descuentos fuera de rango | 1 | Corrección |
| Precios inválidos | 2 | Corrección |
| Productos inactivos | 11 | Cuarentena |
| Duplicados | 1 | Deduplicación |

## 🔧 Requisitos

- **Python**: 3.8+ (recomendado 3.10+)
- **Java**: JDK 11 o 17 ⚠️ *JDK 21 tiene problemas con Hadoop en Windows*
- **Librerías**: PySpark 4.3.0, Pandas 2.x

## 📁 Estructura del Proyecto

```
TallerPySPARK/
├── 📄 README.md                 # Este archivo
├── 📄 INSTALACION.md            # Guía de instalación
├── 📄 RESULTADOS_EJEMPLO.md     # Ejemplos de resultados
├── 🐍 etl_taller.py            # Script ETL principal (997 líneas)
├── 📊 taller_data_engineer.xlsx # Datos fuente
├── 📋 Taller_Spark_.pdf        # Consigna del taller
├── 📂 data/                     # Datos CSV/JSONL
├── 📂 output/                   # Resultados generados
└── 📂 docs/                     # Documentación técnica
```

## 💡 Controles de Calidad

### QA 1: Reconciliación de Ventas
```
✓ PASSED
  Total detalle:  $84,291,707.55
  Total resumen:  $84,291,707.55
  Diferencia:     $0.00
```

### QA 2: Verificación de Duplicados
```
✓ PASSED
  Duplicados en line_id: 0
```

### Idempotencia
```
✓ VERIFICADA
  Reprocesar no duplica registros
```

## 🚀 Tecnologías

- **PySpark 4.3.0**: Motor de procesamiento distribuido
- **Python 3.14**: Lenguaje de programación
- **Pandas**: Manipulación de datos
- **Git**: Control de versiones

## 📖 Documentación

- **[INSTALACION.md](INSTALACION.md)**: Guía completa de instalación
- **[RESULTADOS_EJEMPLO.md](RESULTADOS_EJEMPLO.md)**: Ejemplos de ejecución
- **[docs/ANALISIS_TECNICO.md](docs/ANALISIS_TECNICO.md)**: Análisis técnico detallado
- **[data/README.md](data/README.md)**: Esquemas de datos

## ⚠️ Limitaciones Conocidas

1. **Parquet no disponible**: Por problemas de compatibilidad Windows + Java 21 + Hadoop
2. **Fuente de datos**: Exportaciones de Excel (falta ZIP original `taller_data_local.zip`)
3. **Procesamiento local**: No distribuido (requiere ~4GB RAM)

## 📝 Licencia

Este proyecto está bajo la licencia MIT. Ver [LICENSE](LICENSE) para más detalles.

## 👤 Autor

**Carlos Ospina**
- GitHub: [@carlosospina2](https://github.com/carlosospina2)
- Email: caospina4@gmail.com

## 🤝 Contribuciones

Las contribuciones son bienvenidas. Por favor:
1. Fork el proyecto
2. Crea una rama para tu feature (`git checkout -b feature/AmazingFeature`)
3. Commit tus cambios (`git commit -m 'Add AmazingFeature'`)
4. Push a la rama (`git push origin feature/AmazingFeature`)
5. Abre un Pull Request

---

⭐ Si este proyecto te fue útil, considera darle una estrella en GitHub