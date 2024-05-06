# Date format modules
import datetime
import re 

class UserInteraction:
    def __init__(self):
        self.input = {}

    def get_all_user_inputs(self):
        """
        Gathers all necessary user inputs for the application.
        
        Returns:
            dict: A dictionary containing all inputs required for processing.
        """
        self.input['processing_choice'] = self.get_processing_choice()
        if self.input["processing_choice"] == 1:
            self.input['start_datetime'] = self.get_date_input("Enter start date and time")
            self.input['end_datetime'] = self.get_date_input("Enter end date and time")
            self.input['num_areas'], self.input['area_ids'] = self.get_area_details() 
            self.input['table_name'] = self.get_table_name("Enter your new table name: ")
            self.input['graph_choice'] = self.get_graph_choice()
        elif self.input["processing_choice"] == 2:
            self.input['start_datetime'] = self.get_date_input("Enter start date and time")
            self.input['end_datetime'] = self.get_date_input("Enter end date and time")
            self.input['num_areas'], self.input['area_ids'] = self.get_area_details() 
            self.input['case_description'] = self.get_case_description()
            self.input['graph_choice'] = self.get_graph_choice()
        return self.input

    def get_processing_choice(self):
        """ Choose the processing type. """
        print("Choose the processing type:")
        print("1: Standard area processing")
        print("2: Sequence-based processing (discard records not meeting sequence conditions)")
        choice = self.get_integer_input("Enter your choice (1 or 2): ")
        while choice not in [1, 2]:
            print("Invalid choice. Please choose 1 or 2.")
            choice = self.get_integer_input("Enter your choice (1 or 2): ")
        return choice

    def get_table_name(self, promt):
        """ Get a name for PostgreSQL table."""
        pattern = re.compile(r'^[A-Za-z0-9_]+$') 
        while True:
            table_name_input = input(promt + "table name only letters, numbers and underscores: ")
            if pattern.match(table_name_input):
                return table_name_input
            else:
                print("Invalid table name. Please use only alphabetic characters and underscores.")

    def get_case_description(self):
        """Collects a description for the case from the user."""
        return input("Enter a description for the case: ")

    def get_date_input(self, prompt):
        """ Safely collect a datetime input in a user-friendly format. """
        while True:
            date_str = input(prompt + " (YYYY-MM-DD HH:MM:SS): ")
            try:
                return datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                print("Invalid format, please enter the date and time in the format YYYY-MM-DD HH:MM:SS.")

    def get_graph_choice(self):
        """ Asks the user if they want to see the graph. """
        while True:
            garph_choice = input("Do you want to see the graph? Yes for (Y) & No for (N): ").strip().upper()
            if garph_choice in ["Y", "N"]:
                return garph_choice
            else:
                print("Invalid input. Please enter 'Y' for Yes or 'N' for No.")

    def get_integer_input(self, prompt):
        """ Safely collect an integer input. """
        while True:
            try:
                return int(input(prompt))
            except ValueError:
                print("Invalid input, please enter a valid integer.")

    def get_area_details(self):
        num_areas = self.get_integer_input("Enter the number of areas: ")
        ids = []
        for i in range(num_areas):
            ids.append(self.get_integer_input(f"Type ID for area {i + 1}: "))
        return num_areas, ids

    def get_time_frame(self):
        """ Collect start and end timestamps. """
        start = self.get_integer_input("Type start timestamp: ")
        end = self.get_integer_input("Type end timestamp: ")
        return start, end

    def display_message(self, message):
        """ Display a message to the user. """
        print(message)
