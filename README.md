# calculating_passenger_transit_times
Developed an automated system for calculating passenger transit times,

Orchestrates the data processing workflow from user input to database operations.
    
This program guides the user through several steps to fetch and process data based on user-defined 
parameters and areas of interest. It handles the flow of:

1. Establishing database connections.
2. Collecting user input for date ranges and area definitions.
3. Fetching data from MongoDB based on user-specified times.
4. Processing data through specified analysis methods.
5. Writing results to a PostgreSQL database.

The program utilizes datetime inputs, handles multiple areas for data processing, and 
gives the user choice between standard and sequence-based processing. It logs all major 
steps and calculates the total execution time for performance monitoring.

Outputs:
    - Data is written to the PostgreSQL database if applicable.
    - Logs the process and any errors or important information.
    - Provides database entries' information through logging.

![image](https://github.com/mcagriaktas/calculating_passenger_transit_times/assets/52080028/a1d6bbd8-cac9-4873-8057-69e9559efd4e)

![image](https://github.com/mcagriaktas/calculating_passenger_transit_times/assets/52080028/9a25cbe6-cb8f-4183-8ccd-cb5ea73655f2)

