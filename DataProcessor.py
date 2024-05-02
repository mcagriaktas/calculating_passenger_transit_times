# Data manipulation modules
import pandas as pd

# Debugging modules
import logging

class DataProcessor:
    def __init__(self):
        pass
        
    def check_left_area(self, row, vertices):
        x, y = self.parse_position(row["POSITION"])
        inside = self.is_point_in_polygon(x, y, vertices)
        return "in" if inside else "out"
    
    @staticmethod
    def parse_position(position):
        if isinstance(position, str):
            position_dict = eval(position)
        elif isinstance(position, dict):
            position_dict = position
        else:
            raise ValueError("POSITION data is not string nor dictionary")
        return (float(position_dict["X"]), float(position_dict["Y"]))

    @staticmethod
    def is_point_in_polygon(px, py, vertices):
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
        if df.empty:
            logging.warning("No data fetched from MongoDB. Exiting processing.")
            return pd.DataFrame()
        
        logging.info(f"Starting data processing on DataFrame with {len(df)} rows.")
        area_count = len(vertices_list)
        logging.info(f"Total number of areas to process: {area_count}.")
        
        df["col2"] = pd.to_datetime(df["col2"], unit="s")
        df["WINDOW"] = pd.to_datetime(df["col2"], unit="s")
        
        result_df = pd.DataFrame(df['col1'].unique(), columns=['col1'])

        main_first_seen = df.groupby('col1')['WINDOW'].min().to_frame(name='main_first_seen')
        main_last_seen = df.groupby('col1')['WINDOW'].max().to_frame(name='main_last_seen')
        main_total = ((main_last_seen['main_last_seen'] - main_first_seen['main_first_seen'])
                      .dt.total_seconds().div(60).round().astype('Int64').to_frame(name='main_total'))
        
        result_df = result_df.merge(main_first_seen, on='col1', how='left')
        result_df = result_df.merge(main_last_seen, on='col1', how='left')
        result_df = result_df.merge(main_total, on='col1', how='left')

        for index, vertices in enumerate(vertices_list, start=1):
            area_name = f'area{index}'
            
            df[area_name + '_in'] = df.apply(lambda row: self.check_left_area(row, vertices), axis=1)
            
            area_in_df = df[df[area_name + '_in'] == 'in']
            first_seen = area_in_df.groupby('col1')['WINDOW'].min().to_frame(name=f'{area_name}_first_seen')
            last_seen = area_in_df.groupby('col1')['WINDOW'].max().to_frame(name=f'{area_name}_last_seen')
            area_total_time = (area_in_df.groupby('col1').size().multiply(30).div(60)
                            .round().astype('Int64').to_frame(name=f'{area_name}_total'))

            result_df = result_df.merge(first_seen, on='col1', how='left')
            result_df = result_df.merge(last_seen, on='col1', how='left')
            result_df = result_df.merge(area_total_time, on='col1', how='left')

        result_df = result_df.where(pd.notnull(result_df), None)
        logging.info(f"Data processing completed for all areas.")

        return result_df
        
    def calculate_transitions_between_areas(self, df, vertices_list):
        if df.empty:
            logging.warning("No data fetched from MongoDB. Exiting processing.")
            return pd.DataFrame()

        df["col2"] = pd.to_datetime(df["col2"], unit="s")
        
        result_df = pd.DataFrame(df['col1']. unique(), columns=['col1'])
        
        for index, vertices in enumerate(vertices_list, start=1):
            area_name = f'area{index}'
            df[area_name + '_in'] = df.apply(lambda row: self.check_left_area(row, vertices), axis=1)
            
            area_in_df = df[df[area_name + '_in'] == 'in']
            first_seen = area_in_df.groupby('col1')['col2'].min().to_frame(name=f'{area_name}_first_seen')
            last_seen = area_in_df.groupby('col1')['col2'].max().to_frame(name=f'{area_name}_last_seen')
            area_total_time = (area_in_df.groupby('col1').size().multiply(30).div(60)
                            .round().astype('Int64').to_frame(name=f'{area_name}_total'))
            
            result_df = result_df.merge(first_seen, on='col1', how='left')
            result_df = result_df.merge(last_seen, on='col1', how='left')
            result_df = result_df.merge(area_total_time, on='col1', how='left')

        valid_sequence_conditions = [result_df[f'area{1}_first_seen'].notnull()] 
        for i in range(1, len(vertices_list)):
            current_area = f'area{i}_first_seen'
            next_area = f'area{i+1}_first_seen'
            condition = result_df[current_area].notnull() & result_df[next_area].notnull() & (result_df[current_area] < result_df[next_area])
            valid_sequence_conditions.append(condition)

        if valid_sequence_conditions:
            valid_sequence = valid_sequence_conditions[0]
            for condition in valid_sequence_conditions[1:]:
                valid_sequence &= condition

            result_df = result_df[valid_sequence]

        return result_df
