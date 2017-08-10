"""
Azure Data Warehouse Monitoring API
REST API for pipeline monitoring and management
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import pyodbc
from datetime import datetime, timedelta
import logging

app = Flask(__name__)
CORS(app)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection
def get_db_connection():
    conn_str = (
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=your-synapse-workspace.sql.azuresynapse.net;'
        'DATABASE=analytics_dw;'
        'UID=sqladmin;'
        'PWD=YourPassword123!'
    )
    return pyodbc.connect(conn_str)


@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})


@app.route('/api/metrics/summary', methods=['GET'])
def get_metrics_summary():
    """Get summary metrics"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get today's metrics
        query = """
        SELECT 
            COUNT(*) as total_pipelines,
            SUM(CASE WHEN LastLoadStatus = 'Success' THEN 1 ELSE 0 END) as successful,
            SUM(RecordsProcessed) as total_records,
            AVG(DATEDIFF(MINUTE, CreatedDate, ModifiedDate)) as avg_duration
        FROM etl.ETLControl
        WHERE CAST(ModifiedDate AS DATE) = CAST(GETDATE() AS DATE)
        """
        
        cursor.execute(query)
        row = cursor.fetchone()
        
        metrics = {
            'total_pipelines': row[0] or 0,
            'successful_pipelines': row[1] or 0,
            'total_records': row[2] or 0,
            'avg_duration_minutes': round(row[3] or 0, 2),
            'success_rate': round((row[1] / row[0] * 100) if row[0] > 0 else 0, 2)
        }
        
        conn.close()
        return jsonify(metrics)
        
    except Exception as e:
        logger.error(f"Error fetching metrics: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/pipelines/recent', methods=['GET'])
def get_recent_pipelines():
    """Get recent pipeline executions"""
    try:
        limit = request.args.get('limit', 10, type=int)
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = f"""
        SELECT TOP {limit}
            TableName,
            LastLoadDate,
            LastLoadStatus,
            RecordsProcessed,
            ModifiedDate
        FROM etl.ETLControl
        ORDER BY ModifiedDate DESC
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        pipelines = []
        for row in rows:
            pipelines.append({
                'table_name': row[0],
                'load_date': row[1].isoformat() if row[1] else None,
                'status': row[2],
                'records_processed': row[3] or 0,
                'modified_date': row[4].isoformat() if row[4] else None
            })
        
        conn.close()
        return jsonify(pipelines)
        
    except Exception as e:
        logger.error(f"Error fetching pipelines: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/data-quality/score', methods=['GET'])
def get_data_quality_score():
    """Get data quality score"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Calculate quality score based on various checks
        query = """
        SELECT 
            COUNT(*) as total_checks,
            SUM(CASE WHEN Status = 'PASS' THEN 1 ELSE 0 END) as passed_checks
        FROM etl.DataQualityChecks
        WHERE CheckDate >= DATEADD(DAY, -1, GETDATE())
        """
        
        cursor.execute(query)
        row = cursor.fetchone()
        
        total = row[0] or 0
        passed = row[1] or 0
        score = round((passed / total * 100) if total > 0 else 100, 2)
        
        result = {
            'quality_score': score,
            'total_checks': total,
            'passed_checks': passed,
            'failed_checks': total - passed
        }
        
        conn.close()
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error calculating quality score: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/pipelines/trigger', methods=['POST'])
def trigger_pipeline():
    """Trigger a pipeline manually"""
    try:
        data = request.get_json()
        pipeline_name = data.get('pipeline_name')
        
        if not pipeline_name:
            return jsonify({'error': 'pipeline_name is required'}), 400
        
        # Here you would integrate with Azure Data Factory API
        # For now, return success
        
        result = {
            'status': 'triggered',
            'pipeline_name': pipeline_name,
            'run_id': f"run_{datetime.now().strftime('%Y%m%d%H%M%S')}",
            'timestamp': datetime.now().isoformat()
        }
        
        logger.info(f"Pipeline triggered: {pipeline_name}")
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error triggering pipeline: {str(e)}")
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
