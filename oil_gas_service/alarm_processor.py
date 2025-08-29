import logging
from datetime import datetime

class AlarmProcessor:
    def __init__(self, db, logger=None):
        self.db = db
        self.logger = logger or logging.getLogger('oil_gas_industry_iot')
        # 传感器报警阈值配置
        self.alarm_thresholds = {
            1: {"high": 60, "low": -10},  # 温度传感器：高于60或低于-10报警
            2: {"high": 8, "low": 1},     # 压力传感器：高于8或低于1报警
            3: {"high": 80, "low": 5},    # 流量传感器：高于80或低于5报警
            4: {"high": 400}              # 振动传感器：高于400报警
        }
    
    def check_and_process_alerts(self):
        """检查最新的传感器数据并处理报警"""
        try:
            # 获取最近5分钟内的传感器数据
            query = """
            SELECT sdr.*, sd.sensor_type_id 
            FROM sensor_data_record sdr
            JOIN sensor_device sd ON sdr.device_id = sd.device_id
            WHERE sdr.timestamp >= NOW() - INTERVAL 5 MINUTE
            AND sdr.data_quality_flag = '异常'
            ORDER BY sdr.timestamp DESC
            """
            
            abnormal_data = self.db.execute_query(query, fetch=True)
            
            if not abnormal_data:
                return
            
            # 处理每个异常数据点
            for data in abnormal_data:
                self._process_abnormal_data(data)
                
        except Exception as e:
            self.logger.error(f"Error processing alerts: {str(e)}")
    
    def _process_abnormal_data(self, data):
        """处理异常数据并生成报警（如果需要）"""
        device_id = data['device_id']
        sensor_type_id = data['sensor_type_id']
        value = float(data['measured_value'])
        timestamp = data['timestamp']
        
        # 检查是否已为此数据创建报警
        query = """
        SELECT alarm_id FROM alarm_event 
        WHERE device_id = %s AND triggered_timestamp = %s
        """
        existing = self.db.execute_query(query, (device_id, timestamp), fetch=True)
        
        if existing and len(existing) > 0:
            return  # 已存在报警，不再重复创建
        
        # 确定报警类型和级别
        thresholds = self.alarm_thresholds.get(sensor_type_id, {})
        alarm_type = None
        severity = "低危"
        
        if "high" in thresholds and value > thresholds["high"]:
            alarm_type = f"高于阈值({thresholds['high']})"
            severity = "高危" if value > thresholds["high"] * 1.2 else "中危"
        elif "low" in thresholds and value < thresholds["low"]:
            alarm_type = f"低于阈值({thresholds['low']})"
            severity = "高危" if value < thresholds["low"] * 0.8 else "中危"
        
        # 如果有确定的报警类型，则创建报警
        if alarm_type:
            self._create_alarm_event(
                device_id=device_id,
                alarm_type=alarm_type,
                triggered_time=timestamp,
                severity=severity,
                message=f"传感器值异常: {value}, {alarm_type}"
            )
    
    def _create_alarm_event(self, device_id, alarm_type, triggered_time, severity, message):
        """创建报警事件记录"""
        query = """
        INSERT INTO alarm_event 
        (device_id, alarm_type, triggered_timestamp, severity_level, 
         acknowledgment_status, alarm_message)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        params = (
            device_id,
            alarm_type,
            triggered_time,
            severity,
            "未处理",
            message
        )
        
        row_count = self.db.execute_query(query, params)
        
        if row_count and row_count > 0:
            alarm_id = self.db.get_last_insert_id()
            self.logger.warning(f"Created alarm {alarm_id}: {message} for device {device_id}")
            return alarm_id
        return None
    
    def get_unprocessed_alarms(self):
        """获取未处理的报警"""
        query = """
        SELECT ae.*, s.station_id, s.station_name, st.type_name 
        FROM alarm_event ae
        JOIN sensor_device sd ON ae.device_id = sd.device_id
        JOIN base_station s ON sd.station_id = s.station_id
        JOIN sensor_type st ON sd.sensor_type_id = st.type_id
        WHERE ae.acknowledgment_status = '未处理'
        ORDER BY 
            CASE ae.severity_level 
                WHEN '高危' THEN 1 
                WHEN '中危' THEN 2 
                WHEN '低危' THEN 3 
            END,
            ae.triggered_timestamp DESC
        """
        return self.db.execute_query(query, fetch=True)
    
    def acknowledge_alarm(self, alarm_id, status="已处理"):
        """标记报警为已处理"""
        query = """
        UPDATE alarm_event 
        SET acknowledgment_status = %s 
        WHERE alarm_id = %s
        """
        row_count = self.db.execute_query(query, (status, alarm_id))
        
        if row_count and row_count > 0:
            self.logger.info(f"Alarm {alarm_id} marked as {status}")
            return True
        return False
