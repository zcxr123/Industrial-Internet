import mysql.connector
from mysql.connector import Error
import logging

class Database:
    def __init__(self, host, port, user, password, database, db_type="mysql"):
        """
        初始化数据库连接
        :param host: 数据库主机
        :param port: 数据库端口
        :param user: 用户名
        :param password: 密码
        :param database: 数据库名
        :param db_type: 数据库类型，支持"mysql"和"kingbase"
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.db_type = db_type
        self.connection = None
        self.logger = logging.getLogger('oil_gas_industry_iot')
        
    def connect(self):
        """建立数据库连接"""
        try:
            if self.db_type == "mysql":
                self.connection = mysql.connector.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.database,
                    charset='utf8mb4'
                )
            elif self.db_type == "kingbase":
                # KingBase使用与PostgreSQL兼容的驱动
                import psycopg2
                self.connection = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    dbname=self.database
                )
            if self.connection.is_connected():
                return True
        except Exception as e:
            self.logger.error(f"Database connection error: {str(e)}")
            self.connection = None
        return False
    
    def test_connection(self):
        """测试数据库连接"""
        if not self.connection or not self.connection.is_connected():
            return self.connect()
        return True
    
    def execute_query(self, query, params=None, fetch=False):
        """
        执行查询
        :param query: SQL查询语句
        :param params: 查询参数
        :param fetch: 是否返回结果
        :return: 查询结果或受影响的行数
        """
        if not self.test_connection():
            return None
        
        cursor = None
        try:
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute(query, params or ())
            
            if fetch:
                result = cursor.fetchall()
                return result
            else:
                self.connection.commit()
                return cursor.rowcount
        except Error as e:
            self.logger.error(f"Query error: {str(e)}, SQL: {query}, Params: {params}")
            self.connection.rollback()
            return None
        finally:
            if cursor:
                cursor.close()
    
    def get_last_insert_id(self):
        """获取最后插入记录的ID"""
        if self.db_type == "mysql":
            cursor = self.connection.cursor()
            cursor.execute("SELECT LAST_INSERT_ID()")
            result = cursor.fetchone()
            cursor.close()
            return result[0] if result else None
        elif self.db_type == "kingbase":
            cursor = self.connection.cursor()
            cursor.execute("SELECT CURRVAL(pg_get_serial_sequence('base_station', 'station_id'))")
            result = cursor.fetchone()
            cursor.close()
            return result[0] if result else None
    
    def close(self):
        """关闭数据库连接"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            self.logger.info("Database connection closed")
