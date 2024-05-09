# Data manipulation modules
import pandas as pd

# Debugging modules
import logging

class DataProcessor:
    """
    There are total 5 functions.
        check_left_area
        parse_position
        is_point_in_polygon
        calculate_first_last_seen
        calculate_transitions_between_areas
    """
    def __init__(self):
        """
        Initializes the DataProcessor class.
        """
        pass
        
    def check_left_area(self, row, vertices):
        """
        Checks whether a point specified in the "POSITION" field of the provided row
        is inside or outside a polygon defined by a list of vertices.

        Args:
            row (dict): A dictionary containing the "POSITION" field. 
                        The "POSITION" field should be a string or a dictionary
                        representing the coordinates.
            vertices (list of tuple): A list of tuples representing the polygon's vertices.

        Returns:
            str: "in" if the point is inside the polygon, otherwise "out".
        """
        x, y = self.parse_position(row["POSITION"])
        inside = self.is_point_in_polygon(x, y, vertices)
        return "in" if inside else "out"
    
    @staticmethod
    def parse_position(position):
        """
        Parses the "POSITION" data into (x, y) coordinates.

        Args:
            position (str or dict): The position data in string or dictionary format.
                                    Example string format: "{'X': '5.0', 'Y': '3.0'}"
                                    Example dictionary format: {'X': '5.0', 'Y': '3.0'}

        Returns:
            tuple: A tuple containing (x, y) coordinates as floats.

        Raises:
            ValueError: If the position is not a string or a dictionary.
        """
        if isinstance(position, str):
            position_dict = eval(position)
        elif isinstance(position, dict):
            position_dict = position
        else:
            raise ValueError("POSITION data is not string nor dictionary")
        return (float(position_dict["X"]), float(position_dict["Y"]))

    @staticmethod
    def is_point_in_polygon(px, py, vertices):
        """
        Determines whether a point (px, py) lies inside a polygon.

        Args:
            px (float): The x-coordinate of the point.
            py (float): The y-coordinate of the point.
            vertices (list of tuple): A list of tuples representing the polygon's vertices.

        Returns:
            bool: True if the point is inside the polygon, otherwise False.
        """
        n = len(vertices)
        inside = False
        xinters = 0
        p1x, p1y = vertices[0]
        for i in range(n + 1):
            p2x, p2y = vertices[i % n]
            if py > min(p1y, p2y):
                if py <= max(p1y, p2y):
                    if px <= max(p1x, p2x):
                        if p1y != p2y:
                            xinters = (py - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
                        if p1x == p2x or px <= xinters:
                            inside = not inside
            p1x, p1y = p2x, p2y
        return inside
    
    def calculate_first_last_seen(self, df, vertices_list):
        """
        Calculate the first and last seen times for each unique identifier within a main area and multiple smaller areas,
        based on a given DataFrame. It also categorizes dwell times into specific ranges.

        Args:
            df (pd.DataFrame): The input DataFrame containing positional data. It should have at least the following columns:
                - "WINDOW_START" (int or float): Unix timestamps in seconds indicating the start of observation.
                - "CLIMAC" (str or int): Unique identifier for each observed entity.
            vertices_list (list of list of tuples): A list of polygon vertex sets.
                Each vertex set should define the boundaries of a particular area.
                Example format: [[(x1, y1), (x2, y2), ...], [(x3, y3), ...], ...]

        Returns:
            pd.DataFrame: A DataFrame containing first and last seen times and total observation periods for each "CLIMAC",
                          across both the main area and each specified sub-area.
        """
        if df.empty:
            logging.warning("No data fetched from MongoDB. Exiting processing.")
            return pd.DataFrame()
        
        logging.info(f"Starting data processing on DataFrame with {len(df)} rows.")
        area_count = len(vertices_list)
        logging.info(f"Total number of areas to process: {area_count}.")
        
        df["WINDOW_START"] = pd.to_datetime(df["WINDOW_START"], unit="s")
        df["WINDOW"] = pd.to_datetime(df["WINDOW_START"], unit="s")
        
        result_df = pd.DataFrame(df['CLIMAC'].unique(), columns=['CLIMAC'])

        main_first_seen = df.groupby('CLIMAC')['WINDOW'].min().to_frame(name='main_first_seen')
        main_last_seen = df.groupby('CLIMAC')['WINDOW'].max().to_frame(name='main_last_seen')
        main_total = ((main_last_seen['main_last_seen'] - main_first_seen['main_first_seen'])
                      .dt.total_seconds().div(60).round().astype('Int64').to_frame(name='main_total'))
        
        result_df = result_df.merge(main_first_seen, on='CLIMAC', how='left')
        result_df = result_df.merge(main_last_seen, on='CLIMAC', how='left')
        result_df = result_df.merge(main_total, on='CLIMAC', how='left')

        for index, vertices in enumerate(vertices_list, start=1):
            area_name = f'area{index}'
            
            df[area_name + '_in'] = df.apply(lambda row: self.check_left_area(row, vertices), axis=1)
            
            area_in_df = df[df[area_name + '_in'] == 'in']
            first_seen = area_in_df.groupby('CLIMAC')['WINDOW'].min().to_frame(name=f'{area_name}_first_seen')
            last_seen = area_in_df.groupby('CLIMAC')['WINDOW'].max().to_frame(name=f'{area_name}_last_seen')
            area_total_time = (area_in_df.groupby('CLIMAC').size().multiply(30).div(60)
                            .round().astype('Int64').to_frame(name=f'{area_name}_total'))

            result_df = result_df.merge(first_seen, on='CLIMAC', how='left')
            result_df = result_df.merge(last_seen, on='CLIMAC', how='left')
            result_df = result_df.merge(area_total_time, on='CLIMAC', how='left')

            bins = [0, 1, 10, 20, 30, 40, 50, float("inf")]
            labels = ["Just Seen", "1-9", "10-19", "20-29", "30-39", "40-49", "50+"]
            result_df[f"{area_name}_category"] = pd.cut(result_df[f"{area_name}_total"], bins=bins, labels=labels, right=False)

        result_df = result_df.where(pd.notnull(result_df), None)
        logging.info(f"Data processing completed for all areas.")

        return result_df
        
    def calculate_transitions_between_areas(self, df, vertices_list):
        """
        Calculate valid transitions between different areas based on positional data. 
        Determines the first and last seen times for each unique identifier across each area and validates the transition sequence.

        Args:
            df (pd.DataFrame): Input DataFrame containing positional data. It should have at least the following columns:
                - "WINDOW_START" (int or float): Unix timestamps in seconds indicating the start of observation.
                - "CLIMAC" (str or int): Unique identifier for each observed entity.
            vertices_list (list of list of tuples): A list of polygon vertex sets.
                Each set defines the boundaries of a specific area.
                Example format: [[(x1, y1), (x2, y2), ...], [(x3, y3), ...], ...]

        Returns:
            pd.DataFrame: A DataFrame containing valid sequences between different areas.
                          It also contains categorized dwell times for each entity in the areas.
                          If no valid sequences are found, returns an empty DataFrame.
        """
        logging.info("Starting sequence-based processing...")
        if df.empty:
            logging.warning("No data fetched from MongoDB. Exiting processing.")
            return pd.DataFrame()

        df["WINDOW_START"] = pd.to_datetime(df["WINDOW_START"], unit="s")
        result_df = pd.DataFrame(df['CLIMAC'].unique(), columns=['CLIMAC'])
        
        for index, vertices in enumerate(vertices_list, start=1):
            area_name = f'area{index}'
            df[area_name + '_in'] = df.apply(lambda row: self.check_left_area(row, vertices), axis=1)
            area_in_df = df[df[area_name + '_in'] == 'in']
            logging.info(f"{area_name} contains {area_in_df.shape[0]} records post filter.")

            if area_in_df.empty:
                logging.warning(f"No records found in area {index} after applying position filter.")
                continue
            
            first_seen = area_in_df.groupby('CLIMAC')['WINDOW_START'].min().to_frame(name=f'{area_name}_first_seen')
            last_seen = area_in_df.groupby('CLIMAC')['WINDOW_START'].max().to_frame(name=f'{area_name}_last_seen')
            area_total_time = (area_in_df.groupby('CLIMAC').size().multiply(30).div(60).round().astype('Int64').to_frame(name=f'{area_name}_total'))
            
            result_df = result_df.merge(first_seen, on='CLIMAC', how='left')
            result_df = result_df.merge(last_seen, on='CLIMAC', how='left')
            result_df = result_df.merge(area_total_time, on='CLIMAC', how='left')

            bins = [0, 1, 10, 20, 30, 40, 50, float("inf")]
            labels = ["Just Seen", "1-9", "10-19", "20-29", "30-39", "40-49", "50+"]
            result_df[f"{area_name}_category"] = pd.cut(result_df[f"{area_name}_total"], bins=bins, labels=labels, right=False)

        # Validate sequence validity across all areas
        valid_sequence = result_df
        for i in range(1, len(vertices_list)):
            current_area = f'area{i}_first_seen'
            next_area = f'area{i+1}_first_seen'
            condition = valid_sequence[current_area].notnull() & valid_sequence[next_area].notnull() & (valid_sequence[current_area] < valid_sequence[next_area])
            valid_sequence = valid_sequence[condition]
            logging.info(f"Valid sequences between area {i} and area {i+1}: {valid_sequence.shape[0]}")

        if valid_sequence.empty:
            logging.warning("No valid sequences found. All data filtered out.")

        return valid_sequence
