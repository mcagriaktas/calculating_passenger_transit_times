# Datamanipulations modules
import pandas as pd
import json

# Database modules
import pymongo
import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

# Debugging modules
import logging

class DatabaseController:
    def __init__(self, mongo_ip, mongo_port, mongo_authSource, mongo_username, mongo_password, mongo_database,
                 postgres_ip, postgres_port, postgres_database, postgres_username, postgres_password):
        
        self.mongo_ip = mongo_ip
        self.mongo_port = mongo_port
        self.mongo_authSource = mongo_authSource
        self.mongo_username = mongo_username
        self.mongo_password = mongo_password
        self.mongo_database = mongo_database
        
        self.postgres_ip = postgres_ip
        self.postgres_port = postgres_port
        self.postgres_database = postgres_database
        self.postgres_username = postgres_username
        self.postgres_password = postgres_password
        
        self.mongo_client = None
        self.mongo_db = None
        self.mongo_collection = None
        self.postgres_engine = None
        
    def connect_to_mongodb(self, collection_name):
        try: 
            logging.info(f"Connected mongo database: {self.mongo_database} and collection: {collection_name}.")
            self.mongo_client = pymongo.MongoClient(
                host=f"mongodb://{self.mongo_ip}:{self.mongo_port}/?authSource={self.mongo_authSource}", 
                username=self.mongo_username, 
                password=self.mongo_password)
            self.mongo_db = self.mongo_client[self.mongo_database]
            self.mongo_collection = self.mongo_db[collection_name] 
        except Exception as e:
            logging.error(f"Failed to connect to MongoDB: {e}")

    def fetch_data_from_mongo(self, start_datetime=None, end_datetime=None):
        query = {}
        if start_datetime and end_datetime:
            start_timestamp = int(start_datetime.timestamp())
            end_timestamp = int(end_datetime.timestamp())
            query['WINDOW_START'] = {'$gte': start_timestamp, '$lte': end_timestamp}
        fields = {"CLIMAC": 1, "WINDOW_START": 1, "POSITION": 1}
        x = self.mongo_collection.find(query, fields)
        df = pd.DataFrame(list(x))
        return df

    def fetch_from_postgres(self, columns):
        """
        Fetches data from a PostgreSQL table, handling multiple columns and truncating datetime fields.

        Args:
        wifi_main (str): The name of the PostgreSQL table to fetch data from.
        columns (list): List of column names to fetch, with datetime truncation applied if needed.

        Returns:
        DataFrame: A pandas DataFrame containing the fetched data.
        """
        trunc_columns = []
        for column in columns:
            if 'first_seen' in column:
                trunc_columns.append(f"DATE_TRUNC('hour', {column}) AS {column}")
        query = f"SELECT {', '.join(trunc_columns)} FROM wifi_main"
        return pd.read_sql(query, self.postgres_engine)
    
    def add_area_columns(self, num_areas):
        with self.postgres_engine.connect() as connection:
            metadata = sqlalchemy.MetaData()
            table = sqlalchemy.Table("wifi_main", metadata, autoload_with=self.postgres_engine)
            existing_columns = [c.name for c in table.columns]

            for i in range(2, num_areas + 1):  
                columns_to_add = [
                    f"area{i}_first_seen TIMESTAMP",
                    f"area{i}_last_seen TIMESTAMP",
                    f"area{i}_total INT",
                    f"area{i}_category VARCHAR(10)"
                ]
                for col in columns_to_add:
                    column_name, column_type = col.split()
                    if column_name not in existing_columns:
                        alter_cmd = sqlalchemy.schema.AddColumn("wifi_main", sqlalchemy.Column(column_name, eval('sqlalchemy.'+column_type)))
                        connection.execute(alter_cmd)

            logging.info(f"Table wifi_main updated with additional area columns.")

    def insert_and_return_case_id(self, case_description, origin_id, destination_ids, origin_entrance, origin_exit):
        """
        Insert a new record into the `odcase` table and return the generated case ID.
        """
        destination_ids_json = json.dumps(destination_ids)
        sql_insert = text(
            "INSERT INTO odcase (case_describe, origin_id, destination_id, origin_entrance, origin_exit) "
            "VALUES (:case_describe, :origin_id, :destination_id, :origin_entrance, :origin_exit) "
            "RETURNING id"
        )
        Session = sessionmaker(bind=self.postgres_engine)
        session = Session()
        try:
            result = session.execute(sql_insert, {
                'case_describe': case_description,
                'origin_id': origin_id,
                'destination_id': destination_ids_json,
                'origin_entrance': origin_entrance,
                'origin_exit': origin_exit
            })
            session.commit()
            case_id = result.fetchone()[0]  # Retrieve the newly created case ID
            logging.info(f"New case inserted with ID: {case_id}")
            return case_id
        except SQLAlchemyError as e:
            session.rollback()
            logging.error(f"An error occurred during data insertion: {e}")
            return None
        finally:
            session.close()
    
    def connect_to_postgres(self):
        try:
            db_connection_str = f"postgresql://{self.postgres_username}:{self.postgres_password}@{self.postgres_ip}:{self.postgres_port}/{self.postgres_database}"
            self.postgres_engine = create_engine(db_connection_str)
            logging.info(f"Connected to PostgreSQL database: {self.postgres_database}")
        except Exception as e:
            logging.error(f"Failed to connected to PostgreSQL database: {self.postgres_database}")

    def get_coordinates(self, area_ids):
        with self.postgres_engine.connect().execution_options(stream_results=True) as connection:
            sql_query = text("SELECT id, cordinate FROM area WHERE id = ANY(:area_ids)")
            result = connection.execute(sql_query, {'area_ids': area_ids})
            coordinates_map = {}
            for row in result.mappings():
                area_id = row['id']
                json_data = row['cordinate']
                if isinstance(json_data, str):
                    coordinates = json.loads(json_data)  
                else:
                    coordinates = json_data 
                formatted_coordinates = [(float(coord['lat']), float(coord['lng'])) for coord in coordinates]
                coordinates_map[area_id] = formatted_coordinates
            return coordinates_map

    def write_to_postgres(self, df):
        df.columns = [c.lower() for c in df.columns] 
        logging.info(f"Attempting to write to PostgreSQL, DataFrame: {df.head()}")
        Session = sessionmaker(bind=self.postgres_engine)
        session = Session()
        try:
            df.to_sql('wifi_main', self.postgres_engine, if_exists='append', index=False)
            session.commit()
            logging.info("Data written to PostgreSQL successfully.")
        except Exception as e:
            session.rollback()
            logging.error(f"Failed to write data to PostgreSQL: {e}")
        finally:
            session.close()


    def write_to_postgres_flexible(self, df, table_name):
        df.columns = [c.lower() for c in df.columns]  
        with self.postgres_engine.connect() as connection:
            if not connection.dialect.has_table(connection, table_name):
                df.to_sql(table_name, self.postgres_engine, index=False, if_exists='append')
            else:
                metadata = sqlalchemy.MetaData()
                table = sqlalchemy.Table(table_name, metadata, autoload_with=self.postgres_engine)
                existing_columns = [c.name for c in table.columns]
                new_columns = [col for col in df.columns if col not in existing_columns]

                for column in new_columns:
                    col_type = sqlalchemy.String()  
                    if 'first_seen' in column or 'last_seen' in column:
                        col_type = sqlalchemy.DateTime()
                    elif 'total' in column:
                        col_type = sqlalchemy.Integer()

                    alter_cmd = sqlalchemy.schema.AddColumn(table_name, sqlalchemy.Column(column, col_type))
                    connection.execute(alter_cmd)

                df.to_sql(table_name, self.postgres_engine, index=False, if_exists='append', method='multi')
            logging.info(f"Data written to Postgres successfully in the table: {table_name}")
