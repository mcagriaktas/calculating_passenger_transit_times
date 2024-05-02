# Date format modules
from datetime import datetime

# Main program modules
from DatabaseController import DatabaseController
from DataProcessor import DataProcessor
from UserInteraction import UserInteraction  

# Debugging modules
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    """
    Orchestrates the data processing workflow from user input to database operations.
    
    This function guides the user through several steps to fetch and process data based on user-defined 
    parameters and areas of interest. It handles the flow of:
    
    1. Establishing database connections.
    2. Collecting user input for date ranges and area definitions.
    3. Fetching data from MongoDB based on user-specified times.
    4. Processing data through specified analysis methods.
    5. Writing results to a PostgreSQL database.
    
    The function utilizes datetime inputs, handles multiple areas for data processing, and 
    gives the user choice between standard and sequence-based processing. It logs all major 
    steps and calculates the total execution time for performance monitoring.
    
    Outputs:
        - Data is written to the PostgreSQL database if applicable.
        - Logs the process and any errors or important information.
        - Provides database entries' information through logging.
    """

    logging.info("Starting main function")
    start_time = datetime.now()
    ui = UserInteraction()
    db_controller = DatabaseController(
        mongo_ip="127.0.0.1", mongo_port=27017, mongo_authSource="admin", mongo_username="cagri",
        mongo_password="3541", mongo_database="wifi",
        postgres_ip="127.0.0.1", postgres_port=5432, postgres_database="mydb", postgres_username="cagri",
        postgres_password="3541"
    )

    db_controller.connect_to_mongodb("climac_positions_big")
    start_datetime = ui.get_date_input("Enter start date and time")
    end_datetime = ui.get_date_input("Enter end date and time")
    num_areas = ui.get_integer_input("Enter the number of areas: ")
    area_ids = ui.get_multiple_ids(num_areas)
    table_name_input = ui.get_table_name("Enter your Postgres table name, ")
    db_controller.connect_to_postgres()
    vertices_map = db_controller.get_coordinates(area_ids)
    
    df = db_controller.fetch_data_from_mongo(start_datetime=start_datetime, end_datetime=end_datetime)

    vertices_list = [vertices_map[area_id] for area_id in area_ids if area_id in vertices_map]

    processor = DataProcessor()
    processing_choice = ui.get_processing_choice()
    if processing_choice == 1:
        processed_df = processor.calculate_first_last_seen(df, vertices_list)
    elif processing_choice == 2:
        processed_df = processor.calculate_transitions_between_areas(df, vertices_list)

    if not processed_df.empty:
        db_controller.write_to_postgres(processed_df, table_name=table_name_input)
    else:
        logging.info("No data to write to PostgreSQL.")

    end_time = datetime.now()
    elapsed_time = end_time - start_time
    logging.info(f"Total execution time: {elapsed_time}")

    processed_df.info()

if __name__ == "__main__":
    main()
