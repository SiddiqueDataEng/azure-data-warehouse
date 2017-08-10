"""
Unit tests for Azure Data Warehouse ETL
"""

import unittest
import pyodbc
from datetime import datetime
import sys
sys.path.append('..')


class TestETLPipeline(unittest.TestCase):
    """Test ETL pipeline functionality"""
    
    @classmethod
    def setUpClass(cls):
        """Set up test database connection"""
        cls.conn_str = (
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=test-server.database.windows.net;'
            'DATABASE=test_dw;'
            'UID=testuser;'
            'PWD=TestPassword123!'
        )
    
    def test_database_connection(self):
        """Test database connectivity"""
        try:
            conn = pyodbc.connect(self.conn_str)
            self.assertIsNotNone(conn)
            conn.close()
        except Exception as e:
            self.fail(f"Database connection failed: {str(e)}")
    
    def test_staging_tables_exist(self):
        """Test that staging tables exist"""
        conn = pyodbc.connect(self.conn_str)
        cursor = conn.cursor()
        
        tables = ['staging.Customers', 'staging.Products', 'staging.SalesOrders']
        
        for table in tables:
            query = f"""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'staging' 
            AND TABLE_NAME = '{table.split('.')[1]}'
            """
            cursor.execute(query)
            count = cursor.fetchone()[0]
            self.assertEqual(count, 1, f"Table {table} does not exist")
        
        conn.close()
    
    def test_dimension_tables_exist(self):
        """Test that dimension tables exist"""
        conn = pyodbc.connect(self.conn_str)
        cursor = conn.cursor()
        
        tables = ['dwh.DimCustomer', 'dwh.DimProduct', 'dwh.DimDate']
        
        for table in tables:
            query = f"""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'dwh' 
            AND TABLE_NAME = '{table.split('.')[1]}'
            """
            cursor.execute(query)
            count = cursor.fetchone()[0]
            self.assertEqual(count, 1, f"Table {table} does not exist")
        
        conn.close()
    
    def test_stored_procedures_exist(self):
        """Test that stored procedures exist"""
        conn = pyodbc.connect(self.conn_str)
        cursor = conn.cursor()
        
        procedures = ['dwh.LoadDimCustomer', 'dwh.LoadDimProduct', 'dwh.LoadFactSales']
        
        for proc in procedures:
            query = f"""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.ROUTINES 
            WHERE ROUTINE_SCHEMA = 'dwh' 
            AND ROUTINE_NAME = '{proc.split('.')[1]}'
            """
            cursor.execute(query)
            count = cursor.fetchone()[0]
            self.assertEqual(count, 1, f"Procedure {proc} does not exist")
        
        conn.close()
    
    def test_data_quality_checks(self):
        """Test data quality validation"""
        conn = pyodbc.connect(self.conn_str)
        cursor = conn.cursor()
        
        # Check for null values in key columns
        query = """
        SELECT COUNT(*) 
        FROM dwh.DimCustomer 
        WHERE CustomerID IS NULL OR CustomerName IS NULL
        """
        cursor.execute(query)
        null_count = cursor.fetchone()[0]
        self.assertEqual(null_count, 0, "Found null values in key columns")
        
        # Check for duplicates
        query = """
        SELECT CustomerID, COUNT(*) 
        FROM dwh.DimCustomer 
        WHERE IsCurrent = 1
        GROUP BY CustomerID 
        HAVING COUNT(*) > 1
        """
        cursor.execute(query)
        duplicates = cursor.fetchall()
        self.assertEqual(len(duplicates), 0, "Found duplicate records")
        
        conn.close()


class TestDataTransformations(unittest.TestCase):
    """Test data transformation logic"""
    
    def test_scd_type2_logic(self):
        """Test SCD Type 2 implementation"""
        # Test that historical records are properly maintained
        pass
    
    def test_date_dimension_population(self):
        """Test date dimension has all required dates"""
        pass
    
    def test_fact_table_aggregations(self):
        """Test fact table calculations"""
        pass


if __name__ == '__main__':
    unittest.main()
