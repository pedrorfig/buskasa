import logging
import os
import time
from contextlib import contextmanager
from typing import Optional

from dotenv import load_dotenv

import pandas as pd
from sqlalchemy import create_engine, Engine, text
from sqlalchemy.pool import QueuePool

# Configure logging
logging.basicConfig(
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

load_dotenv()

class DatabaseManager:
    """Centralized database connection manager with connection pooling"""
    
    def __init__(self):
        self._engine = self._create_engine()
    
    def _create_engine(self) -> Engine:
        """Create a SQLAlchemy engine with connection pooling"""
        user = os.environ["DB_USER"]
        password = os.environ["DB_PASS"]
        port = 6543
        
        assert isinstance(port, int), "Port must be numeric"
        assert user is not None, "Username is empty"
        assert password is not None, "Password is empty"
        
        db_uri = f"postgresql+psycopg2://{user}:{password}@aws-0-sa-east-1.pooler.supabase.com:{port}/postgres"
        
        # Configure connection pooling for better performance
        engine = create_engine(
            db_uri, 
            future=True,
            poolclass=QueuePool,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            pool_recycle=3600
        )
        return engine
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = self._engine.connect()
        try:
            yield conn
        finally:
            conn.close()
    
    @contextmanager
    def get_transaction(self):
        """Context manager for database transactions"""
        with self._engine.begin() as conn:
            yield conn
    
    def get_engine(self) -> Engine:
        """Get the database engine"""
        return self._engine
    
    def dispose(self):
        """Dispose of the engine and close all connections"""
        if self._engine:
            self._engine.dispose()
            self._engine = None


class DataCache:
    """Centralized data cache to reduce database I/O"""
    
    def __init__(self):
        self._cache = {}
        self._cache_ttl = {}
        self.default_ttl = 3600  # 1 hour in seconds
    
    def get(self, key: str):
        """Get data from cache if it exists and is not expired"""
        if key in self._cache:
            if time.time() < self._cache_ttl.get(key, 0):
                logger.info(f"Cache hit for {key}")
                return self._cache[key]
            else:
                # Cache expired, remove it
                del self._cache[key]
                if key in self._cache_ttl:
                    del self._cache_ttl[key]
        return None
    
    def set(self, key: str, value, ttl: Optional[int] = None):
        """Set data in cache with optional TTL"""
        self._cache[key] = value
        ttl = ttl or self.default_ttl
        self._cache_ttl[key] = time.time() + ttl
        logger.info(f"Cached {key} for {ttl} seconds")
    
    def clear(self):
        """Clear all cached data"""
        self._cache.clear()
        self._cache_ttl.clear()
        logger.info("Cache cleared")


class BulkDataOperations:
    """Optimized bulk operations for database interactions"""
    
    def __init__(self, db_manager, data_cache):
        self.db_manager = db_manager
        self.data_cache = data_cache
    
    def bulk_exists_check(self, table: str, id_column: str, ids: list) -> set:
        """Check which IDs already exist in the database"""
        cache_key = f"exists_{table}_{id_column}"
        cached_ids = self.data_cache.get(cache_key)
        
        if cached_ids is None:
            with self.db_manager.get_connection() as conn:
                query = f"""
                SELECT DISTINCT {id_column} 
                FROM {table}
                WHERE {id_column} = ANY(%(ids)s)
                """
                result = conn.execute(text(query), {"ids": ids})
                existing_ids = {row[0] for row in result}
                self.data_cache.set(cache_key, existing_ids, ttl=300)  # 5 minutes
                return existing_ids
        else:
            return cached_ids.intersection(set(ids))
    
    def bulk_get_data(self, table: str, columns: list, filter_column: str, filter_values: list) -> pd.DataFrame:
        """Get bulk data from database with caching"""
        
        cache_key = f"bulk_{table}_{filter_column}"
        cached_data = self.data_cache.get(cache_key)
        
        if cached_data is None:
            columns_str = ", ".join(columns)
            with self.db_manager.get_connection() as conn:
                query = f"""
                SELECT {columns_str}
                FROM {table}
                WHERE {filter_column} = ANY(%(values)s)
                """
                result = pd.read_sql(query, conn, params={"values": filter_values})
                self.data_cache.set(cache_key, result)
                return result
        else:
            return cached_data[cached_data[filter_column].isin(filter_values)]

    def bulk_get_existing_data(self, neighborhood_filters: list) -> dict:
        """Get existing data for multiple neighborhoods in a single query"""
        cache_key = f"bulk_existing_data_{hash(str(neighborhood_filters))}"
        cached_result = self.data_cache.get(cache_key)
        if cached_result is not None:
            return cached_result
        
        with self.db_manager.get_connection() as conn:
            # Create parameterized query for multiple neighborhoods
            filter_placeholders = []
            all_params = {}
            
            for i, filters in enumerate(neighborhood_filters):
                filter_placeholders.append(f"""
                    (city = :city_{i} AND neighborhood = :neighborhood_{i} 
                     AND business_type = :business_type_{i})
                """)
                all_params.update({
                    f"city_{i}": filters["city"],
                    f"neighborhood_{i}": filters["neighborhood"],
                    f"business_type_{i}": filters["business_type"]
                })
            
            query = f"""
                SELECT listing_id, city, neighborhood, business_type
                FROM fact_listings
                WHERE {' OR '.join(filter_placeholders)}
            """
            
            result = pd.read_sql(query, con=conn, params=all_params)
            
        # Cache the result
        self.data_cache.set(cache_key, {"existing_listings": result})
        return {"existing_listings": result}
    
    def bulk_get_zip_codes(self) -> pd.DataFrame:
        """Get all zip codes in a single query with caching"""
        cache_key = "all_zip_codes"
        cached_result = self.data_cache.get(cache_key)
        if cached_result is not None:
            return cached_result
            
        with self.db_manager.get_connection() as conn:
            result = pd.read_sql(
                "SELECT * FROM dim_zip_code", 
                con=conn, 
                index_col="zip_code"
            )
        
        # Cache for longer since zip codes don't change often
        self.data_cache.set(cache_key, result, ttl=7200)  # 2 hours
        return result
    
    def bulk_get_analysis_data(self) -> dict:
        """Get image and traffic analysis data in a single operation"""
        cache_key = "analysis_data"
        cached_result = self.data_cache.get(cache_key)
        if cached_result is not None:
            return cached_result
            
        with self.db_manager.get_connection() as conn:
            image_analysis = pd.read_sql(
                "SELECT * FROM fact_image_analysis", 
                con=conn, 
                index_col="id"
            )
            traffic_analysis = pd.read_sql(
                "SELECT * FROM fact_traffic_analysis", 
                con=conn, 
                index_col="id"
            )
        
        result = {
            "image_analysis": image_analysis,
            "traffic_analysis": traffic_analysis
        }
        
        self.data_cache.set(cache_key, result)
        return result


# Create global instances
db_manager = DatabaseManager()
data_cache = DataCache()
bulk_ops = BulkDataOperations(db_manager, data_cache)
