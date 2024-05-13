# Import necessary modules
import datetime
from DatabaseController import DatabaseController
from DataProcessor import DataProcessor
from UserInteraction import UserInteraction
from Graph import Graph
import logging

# Setup logging for debugging
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

    print("""
    ###########################################################
    ########### Calculate Transitions Between Areas ###########
    ###########                                     ###########
    ###########                                     ###########
    ###########################################################
    """)

    ui = UserInteraction()
    user_inputs = ui.get_all_user_inputs()
    db_controller = DatabaseController(
        mongo_ip="127.0.0.1", mongo_port=27017, mongo_authSource="admin", mongo_username="cagri",
        mongo_password="3541", mongo_database="wifi",
        postgres_ip="127.0.0.1", postgres_port=5432, postgres_database="mydb", postgres_username="cagri",
        postgres_password="3541"
    )
  
    logging.info("Starting main function")
    start_time = datetime.datetime.now()

    db_controller.connect_to_mongodb("climac_positions_big")     
    db_controller.connect_to_postgres()
    vertices_map = db_controller.get_coordinates(user_inputs['area_ids'])
    df = db_controller.fetch_data_from_mongo(
        start_datetime=user_inputs['start_datetime'], end_datetime=user_inputs['end_datetime']
    )
    logging.info(f"Fetched data from MongoDB: {df.shape[0]} records found.")

    if not df.empty:
        vertices_list = [vertices_map[area_id] for area_id in user_inputs['area_ids'] if area_id in vertices_map]
        processor = DataProcessor() 

        if user_inputs["processing_choice"] == 1:
            processed_df = processor.calculate_first_last_seen(df, vertices_list)
            db_controller.write_to_postgres_flexible(processed_df, table_name=user_inputs["table_name"])
        elif user_inputs['processing_choice'] == 2:
            processed_df = processor.calculate_transitions_between_areas(df, vertices_list)
            if not processed_df.empty:
                case_id = db_controller.insert_and_return_case_id(
                    user_inputs['case_description'],
                    user_inputs['area_ids'][0],  
                    user_inputs['area_ids'][1:],  
                    user_inputs['start_datetime'],  
                    user_inputs['end_datetime']  
                )
                if case_id is not None:
                    processed_df['case_id'] = case_id
                    db_controller.write_to_postgres(processed_df)
                    logging.info("Data written to PostgreSQL with case_id successfully.")
                else:
                    logging.error("Failed to obtain case_id; data won't be written to wifi_main.")
        
            if user_inputs['graph_choice'] == "Y":
                visualization_data = db_controller.fetch_from_postgres(processed_df.columns)
                graph = Graph(visualization_data)
                graph.plot_multiple_area_distributions()
            else:
                logging.info("Skipping graph generation.")
        
        else:
            logging.info("Processed DataFrame is empty, nothing to write to PostgreSQL.")
    else:
        logging.info("No data fetched from MongoDB for the given time range.")

    end_time = datetime.datetime.now()
    elapsed_time = end_time - start_time
    logging.info(f"Total execution time: {elapsed_time}")

if __name__ == "__main__":
    main()
