import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import List, Dict, Optional

import dlt
import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger('JaffleShop')

dlt_logger = logging.getLogger('dlt')
dlt_logger.setLevel(logging.WARNING)

os.environ["DATA_WRITER__BUFFER_MAX_ITEMS"] = "50000"
os.environ["DATA_WRITER__FILE_MAX_ITEMS"] = "10000"
os.environ["NORMALIZE__WORKERS"] = str(max(2, os.cpu_count()))
os.environ["EXTRACT__WORKERS"] = str(max(2, os.cpu_count() * 1))

BASE_URL = "https://jaffle-shop.scalevector.ai/api/v1"
PAGE_SIZE = 100
CHUNK_SIZE = 1000
THREADS = 8
BATCH_SIZE = 20
FULL_REFRESH = False

ENDPOINTS = {
    "orders": {
        "path": "/orders",
        "primary_key": "id",
        "max_pages": 100
    },
    "customers": {
        "path": "/customers",
        "primary_key": "id",
        "max_pages": 50
    },
    "items": {
        "path": "/items",
        "primary_key": "id",
        "max_pages": 100
    },
    "supplies": {
        "path": "/supplies",
        "primary_key": "id",
        "max_pages": 20
    },
    "stores": {
        "path": "/stores",
        "primary_key": "id",
        "max_pages": 5
    }
}


def fetch_page(endpoint: str, page: int) -> List[Dict]:
    endpoint_logger = logging.getLogger(f'JaffleShop.{endpoint}')

    try:
        url = f"{BASE_URL}{ENDPOINTS[endpoint]['path']}"
        response = requests.get(
            url,
            params={"page": page, "per_page": PAGE_SIZE},
            timeout=10
        )
        response.raise_for_status()
        data = response.json() or []

        if data:
            endpoint_logger.debug(f"Page {page}: fetched {len(data)} records")

        return data
    except Exception as e:
        endpoint_logger.error(f"Error fetching page {page}: {e}")
        return []


def create_resource(endpoint_name: str, max_pages: Optional[int] = None):
    endpoint_logger = logging.getLogger(f'JaffleShop.{endpoint_name}')

    @dlt.resource(
        name=endpoint_name,
        write_disposition="merge",
        primary_key=ENDPOINTS[endpoint_name]["primary_key"]
    )
    def generic_resource(start_page: int = 1):
        endpoint_max_pages = max_pages or ENDPOINTS[endpoint_name]["max_pages"]

        with ThreadPoolExecutor(max_workers=THREADS) as executor:
            chunk_buffer = []
            current_page = start_page
            empty_pages_count = 0

            endpoint_logger.info(f"Starting extraction (pages {start_page} to {endpoint_max_pages})")

            while current_page <= endpoint_max_pages and empty_pages_count < 3:
                batch_end = min(current_page + BATCH_SIZE, endpoint_max_pages + 1)
                batch_pages = list(range(current_page, batch_end))

                endpoint_logger.info(f"Processing batch: pages {current_page}-{batch_end - 1}")

                page_futures = {
                    executor.submit(fetch_page, endpoint_name, page): page
                    for page in batch_pages
                }

                batch_empty = True
                for future in as_completed(page_futures):
                    page_num = page_futures[future]
                    page_data = future.result()

                    if page_data:
                        endpoint_logger.debug(f"Page {page_num}: {len(page_data)} records")
                        chunk_buffer.extend(page_data)
                        batch_empty = False
                        empty_pages_count = 0

                        if len(chunk_buffer) >= CHUNK_SIZE:
                            endpoint_logger.info(f"Yielding chunk of {len(chunk_buffer)} records")
                            yield chunk_buffer
                            chunk_buffer = []
                    else:
                        endpoint_logger.debug(f"Page {page_num}: empty")

                if batch_empty:
                    empty_pages_count += 1
                    endpoint_logger.debug(f"Empty batch detected, count: {empty_pages_count}")

                current_page = batch_end

            if chunk_buffer:
                endpoint_logger.info(f"Yielding final chunk: {len(chunk_buffer)} records")
                yield chunk_buffer

            endpoint_logger.info(f"Extraction completed!")

    return generic_resource


def run_complete_pipeline():
    logger.info("=" * 60)
    logger.info("JAFFLE SHOP COMPLETE PIPELINE")
    logger.info("=" * 60)
    logger.info(f"System: {os.cpu_count()} CPU cores")
    logger.info(f"Config: {THREADS} threads, {CHUNK_SIZE} chunk size")
    logger.info(f"Endpoints: {', '.join(ENDPOINTS.keys())}")

    pipeline = dlt.pipeline(
        pipeline_name="jaffle_shop_complete",
        destination="duckdb",
        dataset_name="jaffle_shop",
        full_refresh=FULL_REFRESH,
    )

    start_time = time.time()

    try:
        resources = []
        for endpoint in ENDPOINTS.keys():
            resource = create_resource(endpoint)
            resources.append(resource())

        logger.info("Running extraction for all endpoints...")
        load_info = pipeline.run(resources)

        end_time = time.time()
        duration = end_time - start_time

        logger.info("PIPELINE COMPLETED!")
        logger.info("=" * 60)
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.debug(f"Load info: {load_info}")

        show_pipeline_stats(pipeline, duration)

        return pipeline

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise


def show_pipeline_stats(pipeline, duration):
    stats_logger = logging.getLogger('JaffleShop.Stats')

    try:
        with pipeline.sql_client() as client:
            stats_logger.info("PIPELINE STATISTICS")
            stats_logger.info("-" * 40)

            total_records = 0

            for endpoint in ENDPOINTS.keys():
                try:
                    result = client.execute_sql(f"SELECT COUNT(*) as count FROM {endpoint}")
                    rows = list(result)
                    if rows:
                        count = rows[0][0]
                        total_records += count
                        stats_logger.info(f"{endpoint.capitalize()}: {count:,} records")
                except Exception as e:
                    stats_logger.warning(f"Could not get stats for {endpoint}: {e}")

            stats_logger.info(f"Total records: {total_records:,}")
            if duration > 0:
                stats_logger.info(f"Throughput: {total_records / duration:.0f} records/second")

    except Exception as e:
        stats_logger.error(f"Could not get statistics: {e}")


def analyze_most_purchased_product():
    analysis_logger = logging.getLogger('JaffleShop.Analysis')

    analysis_logger.info("=" * 60)
    analysis_logger.info("ANALYZING MOST PURCHASED PRODUCT")
    analysis_logger.info("=" * 60)

    pipeline = dlt.pipeline(
        pipeline_name="jaffle_shop_complete",
        destination="duckdb",
        dataset_name="jaffle_shop"
    )

    try:
        with pipeline.sql_client() as client:
            query = """
                    WITH product_sales AS (SELECT i.sku, \
                                                  COUNT(*)                                                                       as total_sales, \
                                                  COUNT(DISTINCT o.customer_id)                                                  as unique_customers, \
                                                  COUNT(DISTINCT o.store_id)                                                     as stores_sold_in, \
                                                  SUM(CAST(REPLACE(REPLACE(o.order_total, '$', ''), ',', '') AS DECIMAL(10, 2))) as total_revenue \
                                           FROM items i \
                                                    JOIN orders o ON i.order_id = o.id \
                                           GROUP BY i.sku),
                         product_info AS (SELECT ps.*, \
                                                 s.name                                                             as product_name, \
                                                 CAST(REPLACE(REPLACE(s.cost, '$', ''), ',', '') AS DECIMAL(10, 2)) as supply_cost \
                                          FROM product_sales ps \
                                                   LEFT JOIN supplies s ON ps.sku = s.sku)
                    SELECT COALESCE(product_name, sku)                                          as product, \
                           sku, \
                           total_sales, \
                           unique_customers, \
                           stores_sold_in, \
                           total_revenue, \
                           ROUND(total_revenue / total_sales, 2)                                as avg_revenue_per_sale, \
                           supply_cost, \
                           ROUND((total_revenue - (total_sales * COALESCE(supply_cost, 0))), 2) as gross_profit, \
                           ROUND(((total_revenue - (total_sales * COALESCE(supply_cost, 0))) / total_revenue * 100), \
                                 2)                                                             as gross_margin_pct
                    FROM product_info
                    ORDER BY total_sales DESC LIMIT 20 \
                    """

            analysis_logger.debug("Executing main product analysis query...")
            result = client.execute_sql(query)
            rows = list(result)

            if rows:
                analysis_logger.info("\nTOP 20 MOST PURCHASED PRODUCTS:")
                analysis_logger.info("-" * 140)

                header = f"{'Product':<30} {'SKU':<10} {'Sales':<8} {'Customers':<10} {'Revenue':<12} {'Cost/Unit':<10} {'Gross Profit':<12} {'Margin %':<10}"
                analysis_logger.info(header)
                analysis_logger.info("-" * 140)

                for row in rows:
                    product = row[0] if row[0] else "Unknown"
                    sku = row[1]
                    sales = row[2]
                    customers = row[3]
                    revenue = row[5] if row[5] else 0
                    cost = row[7] if row[7] else 0
                    profit = row[8] if row[8] else 0
                    margin = row[9] if row[9] else 0

                    analysis_logger.info(
                        f"{product:<30} {sku:<10} {sales:<8} {customers:<10} ${revenue:<11,.2f} ${cost:<9.2f} ${profit:<11,.2f} {margin:<9.1f}%"
                    )

                if rows:
                    winner = rows[0]
                    analysis_logger.info("\n" + "=" * 60)
                    analysis_logger.info("🥇 MOST PURCHASED PRODUCT:")
                    analysis_logger.info("=" * 60)
                    analysis_logger.info(f"Product: {winner[0] if winner[0] else 'SKU: ' + winner[1]}")
                    analysis_logger.info(f"SKU: {winner[1]}")
                    analysis_logger.info(f"Total Sales: {winner[2]:,}")
                    analysis_logger.info(f"Unique Customers: {winner[3]:,}")
                    analysis_logger.info(f"Sold in {winner[4]} stores")
                    analysis_logger.info(f"Total Revenue: ${winner[5]:,.2f}")
                    analysis_logger.info(f"Average Revenue per Sale: ${winner[6]:.2f}")
                    if winner[7]:
                        analysis_logger.info(f"Unit Cost: ${winner[7]:.2f}")
                        analysis_logger.info(f"Total Gross Profit: ${winner[8]:,.2f}")
                        analysis_logger.info(f"Profit Margin: {winner[9]:.1f}%")

                analysis_logger.info("\nANALYSIS BY CATEGORY:")
                result = client.execute_sql("""
                                            SELECT SUBSTR(sku, 1, 3)   as category,
                                                   COUNT(DISTINCT sku) as unique_products,
                                                   SUM(total_sales)    as total_category_sales,
                                                   SUM(total_revenue)  as total_category_revenue
                                            FROM (SELECT i.sku,
                                                         COUNT(*)                                                                       as total_sales,
                                                         SUM(CAST(REPLACE(REPLACE(o.order_total, '$', ''), ',', '') AS DECIMAL(10, 2))) as total_revenue
                                                  FROM items i
                                                           JOIN orders o ON i.order_id = o.id
                                                  GROUP BY i.sku) sku_sales
                                            GROUP BY SUBSTR(sku, 1, 3)
                                            ORDER BY total_category_sales DESC
                                            """)

                rows = list(result)
                if rows:
                    analysis_logger.info("-" * 80)
                    analysis_logger.info(f"{'Category':<15} {'Products':<15} {'Total Sales':<15} {'Total Revenue':<20}")
                    analysis_logger.info("-" * 80)
                    for row in rows:
                        cat, prods, sales, revenue = row
                        analysis_logger.info(f"{cat:<15} {prods:<15} {sales:<15,} ${revenue:<19,.2f}")

            else:
                analysis_logger.warning("No data found. Make sure the pipeline has run successfully.")

    except Exception as e:
        analysis_logger.error(f"Analysis failed: {e}", exc_info=True)
        try:
            with pipeline.sql_client() as client:
                result = client.execute_sql("""
                                            SELECT sku, COUNT(*) as count
                                            FROM items
                                            GROUP BY sku
                                            ORDER BY count DESC
                                                LIMIT 10
                                            """)

                rows = list(result)
                if rows:
                    analysis_logger.info("\nSIMPLE COUNT BY SKU:")
                    analysis_logger.info("-" * 40)
                    for i, row in enumerate(rows, 1):
                        analysis_logger.info(f"{i}. SKU {row[0]}: {row[1]:,} sales")

        except Exception as fallback_e:
            analysis_logger.error(f"Fallback query also failed: {fallback_e}")


def run_supply_chain_analysis():
    supply_logger = logging.getLogger('JaffleShop.SupplyChain')

    supply_logger.info("=" * 50)
    supply_logger.info("SUPPLY CHAIN ANALYSIS")
    supply_logger.info("=" * 50)

    pipeline = dlt.pipeline(
        pipeline_name="jaffle_shop_complete",
        destination="duckdb",
        dataset_name="jaffle_shop"
    )

    try:
        with pipeline.sql_client() as client:
            query = """
                    WITH sku_performance AS (SELECT i.sku, \
                                                    COUNT(*)                      as units_sold, \
                                                    COUNT(DISTINCT o.customer_id) as unique_customers \
                                             FROM items i \
                                                      JOIN orders o ON i.order_id = o.id \
                                             GROUP BY i.sku),
                         supply_analysis AS (SELECT s.name                           as supply_name, \
                                                    s.sku, \
                                                    s.cost, \
                                                    s.perishable, \
                                                    COALESCE(sp.units_sold, 0)       as units_sold, \
                                                    COALESCE(sp.unique_customers, 0) as customers_reached \
                                             FROM supplies s \
                                                      LEFT JOIN sku_performance sp ON s.sku = sp.sku)
                    SELECT supply_name, \
                           sku, \
                           CAST(REPLACE(cost, '$', '') AS DECIMAL(10, 2))                        as unit_cost, \
                           perishable, \
                           units_sold, \
                           customers_reached, \
                           ROUND(units_sold * CAST(REPLACE(cost, '$', '') AS DECIMAL(10, 2)), 2) as total_cost
                    FROM supply_analysis
                    ORDER BY units_sold DESC LIMIT 15 \
                    """

            result = client.execute_sql(query)
            rows = list(result)

            if rows:
                supply_logger.info("\nSUPPLY CHAIN PERFORMANCE:")
                supply_logger.info("-" * 100)
                supply_logger.info(
                    f"{'Supply Name':<30} {'SKU':<15} {'Cost':<10} {'Perishable':<12} {'Units Sold':<12} {'Customers':<12}"
                )
                supply_logger.info("-" * 100)

                for row in rows:
                    name, sku, cost, perishable, units, customers, total = row
                    perish = "Yes" if perishable else "No"
                    supply_logger.info(
                        f"{name:<30} {sku:<15} ${cost:<9.2f} {perish:<12} {units:<12} {customers:<12}"
                    )

    except Exception as e:
        supply_logger.error(f"Supply chain analysis failed: {e}", exc_info=True)


def setup_file_logging(log_dir: str = "logs"):
    import os
    from logging.handlers import RotatingFileHandler

    os.makedirs(log_dir, exist_ok=True)

    file_handler = RotatingFileHandler(
        filename=os.path.join(log_dir, f'jaffle_shop_{datetime.now().strftime("%Y%m%d")}.log'),
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5
    )

    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    file_handler.setFormatter(file_formatter)

    logging.getLogger().addHandler(file_handler)

    logger.info(f"File logging enabled. Logs will be saved to {log_dir}/")


if __name__ == "__main__":
    logger.info("JAFFLE SHOP COMPLETE PIPELINE - FINAL VERSION")
    logger.info("=" * 60)

    try:
        pipeline = run_complete_pipeline()
        logger.info("\nSUCCESS! Complete pipeline finished.")

        analyze_most_purchased_product()
        run_supply_chain_analysis()

        logger.info("\nPIPELINE COMPLETED SUCCESSFULLY!")
        logger.info("All data has been loaded and analyzed.")

    except Exception as e:
        logger.error(f"\nPipeline failed with error: {e}", exc_info=True)
        logger.error("Check the error messages above for details.")
