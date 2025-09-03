import polars as pl
import os
from typing import Dict , Optional
from src.config import config
import logging

# Setup logging
logging.basicConfig(level=getattr(logging, config.LOG_LEVEL),
                    format='%(asctime)s - %(levelname)s - %(message)s'
                    )
logger = logging.getLogger(__name__)

class SrcChecker:
    """
    Class for checking the existence of source files
    """

    def __init__(self):
        self.config = config()

    def check_src_csv(self) -> bool:
        """
        Check if the source CSV files exist
        Returns:
        bool: True if all source files are found, False otherwise
        """
        logger.info("Checking source files...")


        missing_files = []

        for table_name, file_name in self.config.CSV_FILES.items():
            file_path = self.config.get_csv_path(table_name)
        if not os.path.exists(file_path):
                missing_files.append(file_path)

        if missing_files:
            logger.error("Missing CSV files:")
            for file_path in missing_files:
                logger.error(f" - {file_path}")
            return False

        logger.info("✅ All source files found!")

        return True
     
class DataExtractor:
    """
    Class for extracting data from CSV files
    """
    
    def __init__(self):
        self.config = config()
    
    def extract_csv(self,file_path: str, table_name: str) -> pl.DataFrame:
        """
        อ่านไฟล์ CSV ไฟล์เดียว และรีเทิร์นค่าเป็น Polars DataFrame
        Args:
            file_path (str): ที่อยู่ของไฟล์ CSV
            table_name (str): ชื่อของตารางที่ใช้ในการตั้งชื่อคอลัมน์
        Returns:
            pl.DataFrame: DataFrame ที่อ่านจากไฟล์ CSV
        """
        try:
            logger.info("Starting ETL process...")
            df = pl.read_csv(file_path,encoding="utf-8",
                    try_parse_dates=True,
                    null_values=["", "NULL", "null", "N/A", "n/a","\\N"])
                # try_parse_dates=True ช่วยให้ Polars พยายามแปลงคอลัมน์ที่เป็นวันที่ให้เป็นชนิดข้อมูล DateTime
            logging.info(f"Successfully extracted {len(df)} rows from {table_name}")
            return df
        except Exception as e:
            logging.error(f"Error reading {file_path}: {e}")
            return None

    def extract_data(self) -> dict:
        """
        อ่านข้อมูลจากไฟล์ CSV ทั้งหมดจากโฟลเดอร์ที่ระบุ
        Args:
            - None:
        Returns:
            dict: Dictionary ที่มีชื่อตารางเป็น key และ Polars DataFrame เป็น value
            
        """
        logger.info("📁 Reading the data from file CSVs...")
        try:
            # ตรวจสอบว่าโฟลเดอร์ข้อมูลมีอยู่
            config = self.config
            datasource_dir = config.RAW_DATA_PATH
            csv_files = config.CSV_FILES
            if not os.path.isdir(datasource_dir):
                logging.info(f"Error: Data folder does not exist '{datasource_dir}'")
                return None
            # ตรวจสอบว่าไฟล์ CSVs มีอยู่ในโฟลเดอร์ 
            paths = {}   
            for table_name, file_name in csv_files.items():
                # file_path = os.path.join(datasource_dir, file_name)
                file_path = config.get_csv_path(table_name)
                
                if os.path.exists(file_path):
                    paths[table_name] = file_path
                else:
                    logger.warning(f"Error: cannot find '{file_name}' in the folder '{datasource_dir}'")
                    return None
            dict_df = {}
            for name, path in paths.items():
                logger.info(f"Reading the data from {name} at {path}")
                pl_df = self.extract_csv(path,name)
                
                dict_df[name] =  pl_df
                
                    
            # dict_df = {name: extract_csv(path,name) for name, path in paths.items()}
            logger.info("✅ Completed reading all CSV files.")
            return  dict_df
        except Exception as e:
            logger.error(f"Technical error during extracting process: {e}")
            return None