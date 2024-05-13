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
    """
    There are total 9 functions.
    functions names: 
        connect_to_mongodb
        connect_to_postgres
        fetch_data_from_mongo
        fetch_from_postgres
        add_area_columns
        insert_and_return_case_id
        get_coordinates
        write_to_postgres
        write_to_postgres_flexible
    """
    
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
        """
        Connect to a MongoDB database and access a specific collection.

        This method establishes a connection to the MongoDB server using the
        authentication details provided. It logs the connection details and assigns 
        the specified collection to an instance attribute for further operations.

        Args:
            collection_name (str): The name of the MongoDB collection to connect to.

        Returns:
            None: This method sets instance attributes for the MongoDB client, database,
            and collection, which can be used later in the program.
        """
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

    def connect_to_postgres(self):
        """
        Establish a connection to the PostgreSQL database using SQLAlchemy.

        This function creates a SQLAlchemy engine for connecting to a PostgreSQL database
        using the authentication details provided. It stores the created engine as an instance
        attribute `postgres_engine` for further use. If the connection attempt fails, an error
        message is logged.

        Returns:
            None: The function sets the `postgres_engine` instance attribute.
        """
        try:
            db_connection_str = f"postgresql://{self.postgres_username}:{self.postgres_password}@{self.postgres_ip}:{self.postgres_port}/{self.postgres_database}"
            self.postgres_engine = create_engine(db_connection_str)
            logging.info(f"Connected to PostgreSQL database: {self.postgres_database}")
        except Exception as e:
            logging.error(f"Failed to connected to PostgreSQL database: {self.postgres_database}")

    def fetch_data_from_mongo(self, start_datetime=None, end_datetime=None):
        """
        Fetch data from the MongoDB collection within a specific datetime range.

        This function queries the MongoDB collection for documents that fall within the given datetime range
        (using the `WINDOW_START` field) and returns them as a Pandas DataFrame. It also limits the output
        to only the required fields: "CLIMAC", "WINDOW_START", and "POSITION".

        Args:
            start_datetime (datetime, optional): The start of the datetime range to filter documents by.
            end_datetime (datetime, optional): The end of the datetime range to filter documents by.

        Returns:
            DataFrame: A Pandas DataFrame containing the filtered documents from the MongoDB collection.
        """
        query = {}
        if start_datetime and end_datetime:
            start_timestamp = int(start_datetime.timestamp())
            end_timestamp = int(end_datetime.timestamp())
            query['WINDOW_START'] = {'$gte': start_timestamp, '$lte': end_timestamp}
        fields = {"CLIMAC": 1, "WINDOW_START": 1, "POSITION": 1}
        x = self.mongo_collection.find(query, fields)
        df = pd.DataFrame(list(x))
        return df

    def fetch_from_postgres(self, columns, start_datetime, end_datetime):
        """
        Fetches data from a PostgreSQL table, handling multiple columns and truncating datetime fields.
        """
        trunc_columns = []
        for column in columns:
            if 'first_seen' in column:
                trunc_columns.append(f"DATE_TRUNC('hour', {column}) AS {column}")
        
        # Using parameters to safely include datetime values
        query = text(f"""
            SELECT {', '.join(trunc_columns)}
            FROM wifi_main
            WHERE area1_first_seen >= :start_datetime AND area1_last_seen <= :end_datetime
        """)
        
        return pd.read_sql(query, self.postgres_engine, params={'start_datetime': start_datetime, 'end_datetime': end_datetime})

    def add_area_columns(self, num_areas):
        """
        Dynamically add new area columns to the "wifi_main" table in PostgreSQL.

        This function adds multiple new columns related to area information for the "wifi_main"
        table if they don't already exist. For each area (starting from 2 up to the provided `num_areas`),
        the function adds columns for tracking the first and last seen timestamps, the total count, 
        and a category.

        Args:
            num_areas (int): The number of areas to create columns for, starting from 2 up to `num_areas`.

        Returns:
            None: The function directly modifies the table schema if necessary.
        """
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

        This function takes various parameters describing a case and inserts a new record into the `odcase` table.
        It returns the generated primary key value (case ID) of the newly inserted record.

        Args:
            case_description (str): A description of the case to be inserted.
            origin_id (int): The ID of the origin location.
            destination_ids (list[int]): A list of destination location IDs associated with the case.
            origin_entrance (datetime): The entrance time at the origin location.
            origin_exit (datetime): The exit time from the origin location.

        Returns:
            int: The ID of the newly inserted case record. If the insertion fails, returns `None`.
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
            case_id = result.fetchone()[0]  
            logging.info(f"New case inserted with ID: {case_id}")
            return case_id
        except SQLAlchemyError as e:
            session.rollback()
            logging.error(f"An error occurred during data insertion: {e}")
            return None
        finally:
            session.close()
    
    def get_coordinates(self, area_ids):
        """
        Retrieve coordinates for a list of area IDs from the PostgreSQL database.

        This function queries the `area` table for specified area IDs and retrieves their associated
        coordinate data. The coordinates are expected to be stored as JSON in the `cordinate` field.
        It returns a dictionary mapping each area ID to a list of formatted (latitude, longitude) tuples.

        Args:
            area_ids (list[int]): A list of area IDs to fetch coordinates for.

        Returns:
            dict: A dictionary mapping area IDs to a list of (latitude, longitude) coordinate tuples.
        """
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
        """
        Retrieve coordinates for a list of area IDs from the PostgreSQL database.

        This function queries the `area` table for specified area IDs and retrieves their associated
        coordinate data. The coordinates are expected to be stored as JSON in the `cordinate` field.
        It returns a dictionary mapping each area ID to a list of formatted (latitude, longitude) tuples.

        Args:
            area_ids (list[int]): A list of area IDs to fetch coordinates for.

        Returns:
            dict: A dictionary mapping area IDs to a list of (latitude, longitude) coordinate tuples.
        """
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
        """
        Write data from a Pandas DataFrame to a specified PostgreSQL table.

        This function writes data from a Pandas DataFrame into the specified PostgreSQL table.
        If the table doesn't exist, it will be created. If new columns in the DataFrame are not present in the table,
        they will be added dynamically before inserting the data. The columns "first_seen" and "last_seen" are
        treated as `DateTime`, while those containing "total" are treated as `Integer`.

        Args:
            df (DataFrame): The Pandas DataFrame containing data to be written to the PostgreSQL table.
            table_name (str): The name of the table in the PostgreSQL database where data will be written.

        Returns:
            None: The function directly writes or updates the specified table.
        """
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
