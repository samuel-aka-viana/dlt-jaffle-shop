# Jaffle Shop Data Pipeline

A high-performance, scalable data pipeline for extracting, transforming, and analyzing data from the Jaffle Shop API using `dlt` (data load tool) and DuckDB.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Data Schema](#data-schema)
- [Available Analytics](#available-analytics)
- [Performance](#performance)
- [Logging](#logging)
- [Troubleshooting](#troubleshooting)

## 🎯 Overview

The Jaffle Shop Pipeline is a comprehensive data ingestion and analytics solution for e-commerce data, designed to demonstrate best practices in data engineering:

- **Parallel extraction** from multiple API endpoints
- **Optimized loading** with chunking and buffering
- **Automated analytics** for products, customers, and supply chain
- **Structured logging** for monitoring and debugging

## 🏗️ Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────┐
│  Jaffle Shop    │     │   ETL Pipeline   │     │   DuckDB    │
│      API        │────▶│                  │────▶│  Database   │
│                 │     │  • Parallelism   │     │             │
│ • /orders       │     │  • Chunking      │     │ • orders    │
│ • /customers    │     │  • Buffering     │     │ • customers │
│ • /items        │     │  • Error handling│     │ • items     │
│ • /supplies     │     │                  │     │ • supplies  │
│ • /stores       │     └──────────────────┘     │ • stores    │
└─────────────────┘                              └─────────────┘
                                                         │
                                                         ▼
                                                 ┌─────────────┐
                                                 │  Analytics  │
                                                 │             │
                                                 │ • Top SKUs  │
                                                 │ • Revenue   │
                                                 │ • Margins   │
                                                 └─────────────┘
```

### Key Components

1. **Data Extraction Layer**
   - Concurrent API requests using ThreadPoolExecutor
   - Intelligent pagination with empty page detection
   - Configurable batch sizes and parallelism

2. **Data Transformation Layer**
   - Streaming data processing with chunking
   - Type conversion and data cleaning
   - Optimized buffer management

3. **Data Loading Layer**
   - Incremental loading with merge strategy
   - DuckDB as analytical database
   - Normalized table structure

4. **Analytics Layer**
   - Pre-built queries for common metrics
   - Product performance analysis
   - Supply chain optimization insights

## ✨ Features

### Performance Optimizations
- **Parallel Processing**: 8 concurrent threads by default
- **Chunking**: 1,000 records per chunk for optimal memory usage
- **Buffering**: 50,000 items max buffer with 10,000 items per file
- **Smart Pagination**: Stops fetching when encountering empty pages

### Data Quality
- **Primary Key Enforcement**: Ensures data integrity
- **Merge Strategy**: Handles updates gracefully
- **Error Handling**: Robust error recovery with detailed logging

### Analytics Capabilities
- **Product Analysis**: Most purchased items, revenue by SKU
- **Customer Insights**: Purchase patterns, customer segmentation
- **Supply Chain**: Cost analysis, perishable vs non-perishable
- **Store Performance**: Revenue by location, order volume

## 🚀 Installation

### Prerequisites
- Python 3.8+
- pip package manager

### Setup

```bash
# Clone the repository
git clone [<repository-url>](https://github.com/samuel-aka-viana/dlt-jaffle-shop.git)
cd dlt-jaffle-shop

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
run pip install -r requirements.txt
```

## 📖 Usage

### Basic Usage

```python
# Run the complete pipeline
python main.py
```

### Advanced Usage

```python
# Enable file logging
from jaffle_shop_pipeline import setup_file_logging
setup_file_logging()

# Run specific components
from jaffle_shop_pipeline import (
    run_complete_pipeline,
    analyze_most_purchased_product,
    run_supply_chain_analysis
)

# Extract and load data
pipeline = run_complete_pipeline()

# Run analytics
analyze_most_purchased_product()
run_supply_chain_analysis()
```

### Custom Queries

```python
# Access the database directly
pipeline = dlt.pipeline(
    pipeline_name="jaffle_shop_complete",
    destination="duckdb",
    dataset_name="jaffle_shop"
)

with pipeline.sql_client() as client:
    result = client.execute_sql("SELECT * FROM orders LIMIT 10")
    for row in result:
        print(row)
```

## ⚙️ Configuration

### Environment Variables

```python
# Performance tuning
os.environ["DATA_WRITER__BUFFER_MAX_ITEMS"] = "50000"
os.environ["DATA_WRITER__FILE_MAX_ITEMS"] = "10000"
os.environ["NORMALIZE__WORKERS"] = str(max(2, os.cpu_count()))
os.environ["EXTRACT__WORKERS"] = str(max(2, os.cpu_count() * 1))
```

### Pipeline Configuration

```python
# API settings
BASE_URL = "https://jaffle-shop.scalevector.ai/api/v1"
PAGE_SIZE = 100        # Records per API page
CHUNK_SIZE = 1000      # Records per processing chunk
THREADS = 8            # Concurrent API threads
BATCH_SIZE = 20        # Pages per batch

# Endpoint configurations
ENDPOINTS = {
    "orders": {
        "path": "/orders",
        "primary_key": "id",
        "max_pages": 100
    },
    # ... other endpoints
}
```

## 📊 Data Schema

### Tables

#### orders
| Column | Type | Description |
|--------|------|-------------|
| id | VARCHAR | Unique order identifier |
| customer_id | VARCHAR | Reference to customers table |
| store_id | VARCHAR | Reference to stores table |
| ordered_at | TIMESTAMP | Order timestamp |
| order_total | VARCHAR | Total order value (includes $) |

#### customers
| Column | Type | Description |
|--------|------|-------------|
| id | VARCHAR | Unique customer identifier |
| name | VARCHAR | Customer name |

#### items
| Column | Type | Description |
|--------|------|-------------|
| id | VARCHAR | Unique item identifier |
| order_id | VARCHAR | Reference to orders table |
| sku | VARCHAR | Stock keeping unit |

#### supplies
| Column | Type | Description |
|--------|------|-------------|
| id | VARCHAR | Unique supply identifier |
| name | VARCHAR | Supply name |
| sku | VARCHAR | Stock keeping unit |
| cost | VARCHAR | Unit cost (includes $) |
| perishable | BOOLEAN | Perishability flag |

#### stores
| Column | Type | Description |
|--------|------|-------------|
| id | VARCHAR | Unique store identifier |
| name | VARCHAR | Store name |
| opened_at | TIMESTAMP | Store opening date |
| tax_rate | VARCHAR | Store tax rate |

## 📈 Available Analytics

### 1. Most Purchased Products
- Top 20 products by sales volume
- Revenue and profit margin analysis
- Customer reach metrics

### 2. Category Analysis
- Sales breakdown by product category (BEV, JAF, etc.)
- Category revenue comparison

### 3. Supply Chain Performance
- Unit cost analysis
- Perishable vs non-perishable performance
- Supply utilization rates

### 4. Store Performance
- Revenue by store location
- Order volume comparison

## 🚄 Performance

### Metrics
- **Throughput**: ~390 records/second
- **Total Processing Time**: ~50-60 seconds for full pipeline
- **Memory Usage**: Optimized with chunking to handle large datasets

### Optimization Tips
1. **Adjust thread count** based on your CPU cores
2. **Modify chunk size** for memory constraints
3. **Use `full_refresh=True` for clean runs
4. **Enable debug logging** for performance profiling

## 📝 Logging

### Log Levels
- **INFO**: Pipeline progress and key metrics
- **DEBUG**: Detailed execution information
- **WARNING**: Non-critical issues
- **ERROR**: Failures with stack traces

### Log Structure
```
2024-01-15 10:30:45 - JaffleShop - INFO - JAFFLE SHOP COMPLETE PIPELINE
2024-01-15 10:30:45 - JaffleShop.orders - INFO - Starting extraction (pages 1 to 100)
2024-01-15 10:30:47 - JaffleShop.Stats - INFO - Orders: 10,000 records
```

### File Logging
```python
# Enable rotating file logs
setup_file_logging(log_dir="logs")
# Logs saved to: logs/jaffle_shop_YYYYMMDD.log
```

## 🔧 Troubleshooting

### Common Issues

1. **API Timeouts**
   - Reduce `THREADS` or `BATCH_SIZE`
   - Increase timeout in `fetch_page()`

2. **Memory Issues**
   - Decrease `CHUNK_SIZE`
   - Reduce `DATA_WRITER__BUFFER_MAX_ITEMS`

3. **Database Errors**
   - Check DuckDB file permissions
   - Use `full_refresh=True` to reset

4. **Empty Results**
   - Verify API connectivity
   - Check endpoint configurations
   - Review logs for extraction errors

### Debug Mode
```python
# Enable debug logging
logging.getLogger('JaffleShop').setLevel(logging.DEBUG)
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- [dlt](https://dlthub.com/) - Data load tool
- [DuckDB](https://duckdb.org/) - Analytical database
- [Jaffle Shop API](https://jaffle-shop.scalevector.ai/) - Sample e-commerce API
