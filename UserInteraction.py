import datetime
import re 

class UserInteraction:
    def __init__(self):
        pass

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

    def get_date_input(self, prompt):
        """ Safely collect a datetime input in a user-friendly format. """
        while True:
            date_str = input(prompt + " (YYYY-MM-DD HH:MM:SS): ")
            try:
                return datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                print("Invalid format, please enter the date and time in the format YYYY-MM-DD HH:MM:SS.")

    def get_table_name(self, promt):
        """ Get a name for PostgreSQL table."""
        pattern = re.compile(r'^[A-Za-z_]+$') 
        while True:
            table_name_input = input(promt + "table name only letters and underscores: ")
            if pattern.match(table_name_input):
                return table_name_input
            else:
                print("Invalid table name. Please use only alphabetic characters and underscores.")


    def get_integer_input(self, prompt):
        """ Safely collect an integer input. """
        while True:
            try:
                return int(input(prompt))
            except ValueError:
                print("Invalid input, please enter a valid integer.")

    def get_multiple_ids(self, num):
        """ Collect multiple IDs from the user. """
        ids = []
        for i in range(num):
            ids.append(self.get_integer_input(f"Type ID for area {i + 1}: "))
        return ids

    def get_time_frame(self):
        """ Collect start and end timestamps. """
        start = self.get_integer_input("Type start timestamp: ")
        end = self.get_integer_input("Type end timestamp: ")
        return start, end

    def display_message(self, message):
        """ Display a message to the user. """
        print(message)
