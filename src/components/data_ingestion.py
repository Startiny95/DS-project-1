'''
Data ingestion is the process of collecting and importing raw data 
from various sources into a centralized storage system
(like a data warehouse, data lake, or database) where it can be stored, 
processed, and analyzed.
'''
import os
import sys
from src.exception import CustomException
from src.logger import logging
import pandas as pd
from sklearn.model_selection import train_test_split
from dataclasses import dataclass

@dataclass
class DataIngestionConfig:
    # creating new artifacts folder, to store all the outputs
    train_data_path: str = os.path.join('artifacts', 'train.csv')
    test_data_path: str = os.path.join('artifacts', 'test.csv')
    raw_data_path: str = os.path.join('artifacts', 'data.csv')

class DataIngestion:
    def __init__(self):
        self.ingestion_config = DataIngestionConfig()

    # initiate data ingestion will see if data is stored in database
    # read the dataset in easy way
    def initiate_data_ingestion(self):
        logging.info("Entered the data ingestion method ")
        # anytime as error will come:
        try:
            # read the dataset: (can also do it from mongodb)
            df = pd.read_csv(r'notebooks\data\StudentsPerformance.csv')
            logging.info('Read the dataset as dataframe')

            os.makedirs(os.path.dirname(self.ingestion_config.train_data_path), exist_ok = True)

            df.to_csv(self.ingestion_config.raw_data_path, index = False, header = True)

            logging.info("Train test split initiated")
            train_set, test_set = train_test_split(df, test_size=0.2, random_state=42)

            train_set.to_csv(self.ingestion_config.train_data_path, index = False, header = True)
            test_set.to_csv(self.ingestion_config.test_data_path, index = False, header = True)

            logging.info("Ingestion of the data is completed")

            return (
                self.ingestion_config.train_data.path,
                self.ingestion_config.test_data_path
            )
        except Exception as e:
            raise CustomException(e,sys)
        
if __name__ == "__main__":
    obj = DataIngestion()
    obj.initiate_data_ingestion()
