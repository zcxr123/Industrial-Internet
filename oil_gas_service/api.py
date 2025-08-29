from flask import Flask, jsonify, request, g
import logging
from database import Database
from base_station_manager import BaseStationManager
from sensor_data_collector import SensorDataCollector
from alarm_processor import AlarmProcessor
import json

app = Flask(__name__)

# 配置日志
logger = logging.getLogger('oil_gas_api')
logger.setLevel(logging.INFO)

# 数据库连接
def get_db():
    if 'db' not in g:
        g.db = Database(
            host="localhost",
            port=3306,
            user="oil_gas_user",
            password="Oil_Gas_2023",
            database="oil_gas_db"
        )
    return g.db

@app.teardown_appcontext
def close_db(e=None):
    db = g.pop('db', None)
    if db is not None:
        db.close()

# 健康检查接口
@app.route('/health', methods=['GET'])
def health_check():
    try:
        db = get_db()
        if db.test_connection():
            return jsonify({"status": "healthy", "message": "Service is running normally"}), 200
        else:
            return jsonify({"status": "unhealthy", "message": "Database connection failed"}), 500
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# 基站管理接口
@app.route('/api/stations', methods=['GET'])
def get_stations():
    try:
        status = request.args.get('status')
        manager = BaseStationManager(get_db())
        
        if status:
            stations = manager.get_all_stations(status)
        else:
            stations = manager.get_all_stations()
            
        return jsonify({"stations": stations}), 200
    except Exception as e:
        logger.error(f"Error getting stations: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/stations', methods=['POST'])
def create_station():
    try:
        data = request.get_json()
        required_fields = ['name', 'geolocation', 'oil_field_block', 'status']
        
        # 验证必填字段
        for field in required_fields:
            if field not in data:
                return jsonify({"error": f"Missing required field: {field}"}), 400
        
        manager = BaseStationManager(get_db())
        station_id = manager.create_station(
            name=data['name'],
            geolocation=data['geolocation'],
            oil_field_block=data['oil_field_block'],
            status=data['status']
        )
        
        if station_id:
            return jsonify({"message": "Station created", "station_id": station_id}), 201
        else:
            return jsonify({"error": "Failed to create station"}), 500
    except Exception as e:
        logger.error(f"Error creating station: {str(e)}")
        return jsonify({"error": str(e)}), 500

# 传感器数据接口
@app.route('/api/stations/<int:station_id>/sensors', methods=['GET'])
def get_sensors(station_id):
    try:
        collector = SensorDataCollector(get_db())
        sensors = collector.get_sensors_for_station(station_id)
        return jsonify({"station_id": station_id, "sensors": sensors}), 200
    except Exception as e:
        logger.error(f"Error getting sensors: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/sensors/<int:sensor_id>/data', methods=['GET'])
def get_sensor_data(sensor_id):
    try:
        # 获取查询参数
        limit = request.args.get('limit', 100, type=int)
        start_time = request.args.get('start_time')
        end_time = request.args.get('end_time')
        
        db = get_db()
        query = """
        SELECT * FROM sensor_data_record 
        WHERE device_id = %s
        """
        params = [sensor_id]
        
        # 添加时间范围过滤
        if start_time and end_time:
            query += " AND timestamp BETWEEN %s AND %s"
            params.extend([start_time, end_time])
        elif start_time:
            query += " AND timestamp >= %s"
            params.append(start_time)
        elif end_time:
            query += " AND timestamp <= %s"
            params.append(end_time)
            
        # 添加排序和限制
        query += " ORDER BY timestamp DESC LIMIT %s"
        params.append(limit)
        
        data = db.execute_query(query, params, fetch=True)
        return jsonify({"sensor_id": sensor_id, "data": data}), 200
    except Exception as e:
        logger.error(f"Error getting sensor data: {str(e)}")
        return jsonify({"error": str(e)}), 500

# 报警接口
@app.route('/api/alarms', methods=['GET'])
def get_alarms():
    try:
        # 可选参数：是否只获取未处理的报警
        unprocessed_only = request.args.get('unprocessed', 'false').lower() == 'true'
        
        processor = AlarmProcessor(get_db())
        
        if unprocessed_only:
            alarms = processor.get_unprocessed_alarms()
        else:
            # 获取所有报警
            db = get_db()
            alarms = db.execute_query(
                "SELECT * FROM alarm_event ORDER BY triggered_timestamp DESC",
                fetch=True
            )
            
        return jsonify({"alarms": alarms}), 200
    except Exception as e:
        logger.error(f"Error getting alarms: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/alarms/<int:alarm_id>/acknowledge', methods=['POST'])
def acknowledge_alarm(alarm_id):
    try:
        data = request.get_json() or {}
        status = data.get('status', '已处理')
        
        processor = AlarmProcessor(get_db())
        result = processor.acknowledge_alarm(alarm_id, status)
        
        if result:
            return jsonify({"message": f"Alarm {alarm_id} acknowledged", "status": status}), 200
        else:
            return jsonify({"error": f"Failed to acknowledge alarm {alarm_id}"}), 404
    except Exception as e:
        logger.error(f"Error acknowledging alarm: {str(e)}")
        return jsonify({"error": str(e)}), 500

# 钻井进度接口
@app.route('/api/stations/<int:station_id>/drilling-progress', methods=['GET'])
def get_drilling_progress(station_id):
    try:
        limit = request.args.get('limit', 100, type=int)
        db = get_db()
        
        query = """
        SELECT * FROM drilling_progress 
        WHERE station_id = %s
        ORDER BY timestamp DESC
        LIMIT %s
        """
        progress = db.execute_query(query, (station_id, limit), fetch=True)
        
        return jsonify({
            "station_id": station_id,
            "drilling_progress": progress
        }), 200
    except Exception as e:
        logger.error(f"Error getting drilling progress: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    # 生产环境中应使用合适的WSGI服务器如Gunicorn
    app.run(host='0.0.0.0', port=5000, debug=True)
