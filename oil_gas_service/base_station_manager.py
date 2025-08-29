from datetime import datetime
import logging

class BaseStationManager:
    def __init__(self, db, logger=None):
        self.db = db
        self.logger = logger or logging.getLogger('oil_gas_industry_iot')
    
    def create_station(self, name, geolocation, oil_field_block, status):
        """
        创建新基站
        :param name: 基站名称
        :param geolocation: 地理位置（经纬度）
        :param oil_field_block: 所属油田区块
        :param status: 状态（运行/维护/停机）
        :return: 新创建基站的ID
        """
        try:
            query = """
            INSERT INTO base_station 
            (station_name, geolocation, oil_field_block, status, created_time)
            VALUES (%s, %s, %s, %s, %s)
            """
            created_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            params = (name, geolocation, oil_field_block, status, created_time)
            
            row_count = self.db.execute_query(query, params)
            
            if row_count and row_count > 0:
                station_id = self.db.get_last_insert_id()
                self.logger.info(f"Created new base station with ID: {station_id}")
                return station_id
            else:
                self.logger.error("Failed to create base station")
                return None
        except Exception as e:
            self.logger.error(f"Error creating base station: {str(e)}")
            return None
    
    def get_station(self, station_id):
        """根据ID获取基站信息"""
        query = "SELECT * FROM base_station WHERE station_id = %s"
        result = self.db.execute_query(query, (station_id,), fetch=True)
        return result[0] if result else None
    
    def get_station_by_name(self, name):
        """根据名称获取基站信息"""
        query = "SELECT * FROM base_station WHERE station_name = %s"
        result = self.db.execute_query(query, (name,), fetch=True)
        return result[0] if result else None
    
    def get_all_stations(self, status=None):
        """获取所有基站信息，可按状态筛选"""
        if status:
            query = "SELECT * FROM base_station WHERE status = %s"
            result = self.db.execute_query(query, (status,), fetch=True)
        else:
            query = "SELECT * FROM base_station"
            result = self.db.execute_query(query, fetch=True)
        return result
    
    def update_station_status(self, station_id, status):
        """更新基站状态"""
        query = """
        UPDATE base_station 
        SET status = %s 
        WHERE station_id = %s
        """
        row_count = self.db.execute_query(query, (status, station_id))
        if row_count and row_count > 0:
            self.logger.info(f"Updated station {station_id} status to {status}")
            return True
        return False
    
    def delete_station(self, station_id):
        """删除基站"""
        query = "DELETE FROM base_station WHERE station_id = %s"
        row_count = self.db.execute_query(query, (station_id,))
        if row_count and row_count > 0:
            self.logger.info(f"Deleted station {station_id}")
            return True
        return False
