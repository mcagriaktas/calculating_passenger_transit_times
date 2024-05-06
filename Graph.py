# Graph module
import matplotlib.pyplot as plt

class Graph:
    def __init__(self, data):
        """
        Initializes the Graph with data.
        
        Args:
        data (DataFrame): A pandas DataFrame containing the categorized first_seen data for each area.
        """
        self.data = data

    def plot_area_distribution(self, area_name):
        """
        Plots the distribution of 'first seen' times for a specified area.
        
        Args:
        area_name (str): The column name in the DataFrame for which to plot the distribution.
        """
        if area_name not in self.data.columns:
            raise ValueError(f"{area_name} not found in DataFrame")
        
        # Prepare the data
        category_counts = self.data[area_name].value_counts().sort_index()
        
        # Create the plot
        plt.figure(figsize=(10, 6))
        category_counts.plot(kind='bar', color='skyblue')
        plt.title(f'First Seen Times for {area_name}')
        plt.xlabel('Categories')
        plt.ylabel('Count')
        plt.xticks(rotation=45)  # Rotate category labels for better readability
        plt.grid(True)
        plt.show()

    def plot_multiple_area_distributions(self):
        """
        Plots the distribution of 'first seen' times for multiple areas side-by-side.
        """
        num_areas = len(self.data.columns)
        cols = 2  
        rows = (num_areas + cols - 1) // cols  
        plt.figure(figsize=(cols * 10, rows * 6))
        
        for i, column in enumerate(self.data.columns, 1):
            plt.subplot(rows, cols, i)
            category_counts = self.data[column].value_counts().sort_index()
            category_counts.plot(kind='bar', color='skyblue')
            plt.title(f'Distribution for {column}')
            plt.xlabel('Time Categories')
            plt.ylabel('Count')
            plt.xticks(rotation=45)
            plt.grid(True)
        plt.tight_layout()
        plt.show()
