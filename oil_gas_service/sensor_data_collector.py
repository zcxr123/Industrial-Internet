import random
from datetime import datetime
import logging

class SensorDataCollector:
    def __init__(self, db, logger=None):
        self.db = db
        self.logger = logger or logging.getLogger('oil_gas_industry_iot')
        # 预定义的传感器类型和范围
        self.sensor_types = {
            1: {"name": "温度传感器", "quantity": "温度", "unit": "°C", "min": -20, "max": 80},
            2: {"name": "压力传感器", "quantity": "压力", "unit": "MPa", "min": 0, "max": 10},
            3: {"name": "流量传感器", "quantity": "流量", "unit": "m³/h", "min": 0, "max": 100},
            4: {"name": "振动传感器", "quantity": "振动频率", "unit": "Hz", "min": 0, "max": 500}
        }
        # 确保存在基础传感器类型数据
        self._initialize_sensor_types()
        
    def _initialize_sensor_types(self):
        """初始化传感器类型数据（如果不存在）"""
        query = "SELECT COUNT(*) as count FROM sensor_type"
        result = self.db.execute_query(query, fetch=True)
        
        if result and result[0]['count'] == 0:
            self.logger.info("Initializing sensor types...")
            for type_id, info in self.sensor_types.items():
                query = """
                INSERT INTO sensor_type 
                (type_id, type_name, measured_quantity, measurement_unit, typical_accuracy)
                VALUES (%s, %s, %s, %s, %s)
                """
                params = (
                    type_id,
                    info["name"],
                    info["quantity"],
                    info["unit"],
                    f"±0.5{info['unit']}"
                )
                self.db.execute_query(query, params)
    
    def get_sensors_for_station(self, station_id):
        """获取指定基站的所有传感器"""
        query = "SELECT * FROM sensor_device WHERE station_id = %s"
        return self.db.execute_query(query, (station_id,), fetch=True)
    
    def ensure_sensors_exist(self, station_id, num_sensors=5):
        """确保基站有足够的传感器，如果没有则创建"""
        existing_sensors = self.get_sensors_for_station(station_id)
        
        if len(existing_sensors) < num_sensors:
            needed = num_sensors - len(existing_sensors)
            self.logger.info(f"Adding {needed} sensors to station {station_id}")
            
            for i in range(needed):
                # 随机选择传感器类型
                type_id = random.choice(list(self.sensor_types.keys()))
                install_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                query = """
                INSERT INTO sensor_device 
                (sensor_type_id, station_id, installation_date, calibration_data, output_signal_type)
                VALUES (%s, %s, %s, %s, %s)
                """
                params = (
                    type_id,
                    station_id,
                    install_date,
                    f"Calibrated on {install_date}",
                    random.choice(["数字信号", "模拟信号", "脉冲信号"])
                )
                
                self.db.execute_query(query, params)
    
    def generate_sensor_data(self, sensor_id, sensor_type_id):
        """生成模拟的传感器数据"""
        sensor_info = self.sensor_types.get(sensor_type_id, {})
        if not sensor_info:
            return None
        
        # 生成在传感器范围内的随机值
        min_val = sensor_info["min"]
        max_val = sensor_info["max"]
        value = round(random.uniform(min_val, max_val), 2)
        
        # 随机生成数据质量标识（大部分正常，偶尔异常）
        quality = "正常"
        if random.random() < 0.05:  # 5%的概率数据异常
            quality = "异常"
        
        return {
            "sensor_id": sensor_id,
            "value": value,
            "quality": quality,
            "unit": sensor_info["unit"],
            "quantity": sensor_info["quantity"]
        }
    
    def collect_and_store_data(self, station_id):
        """采集并存储传感器数据"""
        try:
            # 确保基站有传感器
            self.ensure_sensors_exist(station_id)
            
            # 获取基站的传感器
            sensors = self.get_sensors_for_station(station_id)
            if not sensors:
                self.logger.warning(f"No sensors found for station {station_id}")
                return
            
            # 获取该基站的录井仪（如果没有则创建一个）
            logging_unit_id = self._get_or_create_logging_unit(station_id)
            
            # 为每个传感器生成并存储数据
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for sensor in sensors:
                data = self.generate_sensor_data(sensor['device_id'], sensor['sensor_type_id'])
                if data:
                    self._store_sensor_data(
                        sensor_id=data['sensor_id'],
                        logging_unit_id=logging_unit_id,
                        timestamp=timestamp,
                        value=data['value'],
                        quality=data['quality']
                    )
            
            # 更新钻井进度
            self._update_drilling_progress(station_id, timestamp)
            
            self.logger.info(f"Collected data for {len(sensors)} sensors at station {station_id}")
            
        except Exception as e:
            self.logger.error(f"Error collecting sensor data: {str(e)}")
    
    def _get_or_create_logging_unit(self, station_id):
        """获取或创建录井仪"""
        query = "SELECT unit_id FROM integrated_logging_unit WHERE station_id = %s LIMIT 1"
        result = self.db.execute_query(query, (station_id,), fetch=True)
        
        if result and len(result) > 0:
            return result[0]['unit_id']
        
        # 创建新的录井仪
        query = """
        INSERT INTO integrated_logging_unit 
        (station_id, model_number, data_acquisition_frequency_hz, processing_capabilities_description)
        VALUES (%s, %s, %s, %s)
        """
        params = (
            station_id,
            2023,  # 型号
            "10Hz",  # 采集频率
            "支持多类型传感器数据采集与实时处理"
        )
        
        self.db.execute_query(query, params)
        return self.db.get_last_insert_id()
    
    def _store_sensor_data(self, sensor_id, logging_unit_id, timestamp, value, quality):
        """存储传感器数据到数据库"""
        query = """
        INSERT INTO sensor_data_record 
        (device_id, logging_unit_id, timestamp, measured_value, data_quality_flag)
        VALUES (%s, %s, %s, %s, %s)
        """
        params = (
            sensor_id,
            logging_unit_id,
            timestamp,
            str(value),
            quality
        )
        
        self.db.execute_query(query, params)
    
    def _update_drilling_progress(self, station_id, timestamp):
        """更新钻井进度数据"""
        # 获取最新的钻井进度
        query = """
        SELECT current_hole_depth, bit_position 
        FROM drilling_progress 
        WHERE station_id = %s 
        ORDER BY timestamp DESC 
        LIMIT 1
        """
        result = self.db.execute_query(query, (station_id,), fetch=True)
        
        # 计算新的井深（模拟缓慢增加）
        last_depth = 0
        last_position = "0,0,0"
        
        if result and len(result) > 0:
            try:
                last_depth = float(result[0]['current_hole_depth'])
                last_position = result[0]['bit_position']
            except:
                pass
        
        # 模拟钻速（每5秒增加0.1-0.5米）
        new_depth = round(last_depth + random.uniform(0.1, 0.5), 2)
        
        # 模拟钻头位置变化
        pos_parts = list(map(float, last_position.split(',')))
        new_pos = f"{pos_parts[0] + random.uniform(-0.05, 0.05):.2f}," \
                  f"{pos_parts[1] + random.uniform(-0.05, 0.05):.2f}," \
                  f"{new_depth:.2f}"
        
        # 计算钻速
        penetration_rate = round((new_depth - last_depth) / 5 * 3600, 2)  # 转换为每小时米数
        
        # 存储新的钻井进度
        query = """
        INSERT INTO drilling_progress 
        (station_id, timestamp, current_hole_depth, bit_position, rate_of_penetration)
        VALUES (%s, %s, %s, %s, %s)
        """
        params = (
            station_id,
            timestamp,
            str(new_depth),
            new_pos,
            f"{penetration_rate} m/h"
        )
        
        self.db.execute_query(query, params)
