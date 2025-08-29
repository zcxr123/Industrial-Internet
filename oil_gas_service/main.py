import logging
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler
from sensor_data_collector import SensorDataCollector
from base_station_manager import BaseStationManager
from alarm_processor import AlarmProcessor
from database import Database

# 配置日志
def setup_logger():
    logger = logging.getLogger('oil_gas_industry_iot')
    logger.setLevel(logging.INFO)
    
    # 确保日志目录存在
    import os
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    # 配置日志轮转
    handler = RotatingFileHandler(
        'logs/oil_gas_service.log',
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    
    logger.addHandler(handler)
    return logger

def main():
    # 初始化日志
    logger = setup_logger()
    logger.info("Starting Oil & Gas Industry IoT Service...")
    
    try:
        # 初始化数据库连接
        db = Database(
            host="localhost",
            port=3306,
            user="oil_gas_user",
            password="Oil_Gas_2023",
            database="oil_gas_db"
        )
        
        # 测试数据库连接
        if db.test_connection():
            logger.info("Database connection successful")
        else:
            logger.error("Database connection failed. Exiting...")
            return
        
        # 初始化各模块
        station_manager = BaseStationManager(db, logger)
        sensor_collector = SensorDataCollector(db, logger)
        alarm_processor = AlarmProcessor(db, logger)
        
        # 创建示例基站（如果不存在）
        if not station_manager.get_station_by_name("Test Station 1"):
            station_id = station_manager.create_station(
                name="Test Station 1",
                geolocation="30.1234, 120.5678",
                oil_field_block="East China Sea Block A",
                status="运行"
            )
            logger.info(f"Created test base station with ID: {station_id}")
        else:
            station = station_manager.get_station_by_name("Test Station 1")
            station_id = station['station_id']
            logger.info(f"Using existing test base station with ID: {station_id}")
        
        # 主循环：采集数据并处理报警
        logger.info("Entering main data collection loop...")
        while True:
            # 采集传感器数据
            sensor_collector.collect_and_store_data(station_id)
            
            # 处理报警
            alarm_processor.check_and_process_alerts()
            
            # 每5秒采集一次数据
            time.sleep(5)
            
    except Exception as e:
        logger.error(f"Critical error in main service: {str(e)}", exc_info=True)
    finally:
        if 'db' in locals():
            db.close()
        logger.info("Oil & Gas Industry IoT Service stopped")

if __name__ == "__main__":
    main()
