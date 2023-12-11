import pandas as pd

# Function to generate a car matrix from a DataFrame
def generate_car_matrix(df):
    try:
        # Pivot the DataFrame to create a matrix using 'id_1', 'id_2', and 'car'
        car_matrix = df.pivot(index='id_1', columns='id_2', values='car')
        
        # Fill NaN values with 0
        car_matrix = car_matrix.fillna(0)
        
        # Set diagonal elements to 0
        for index in car_matrix.index:
            car_matrix.at[index, index] = 0
        
        return car_matrix
    except Exception as e:
        print(f"An error occurred in generate_car_matrix: {e}")
        return None

# Function to count car types based on values in the 'car' column
def get_type_count(df):
    try:
        # Create a new column 'car_type' based on conditions
        df['car_type'] = 'low'
        df.loc[df['car'] > 15, 'car_type'] = 'medium'
        df.loc[df['car'] > 25, 'car_type'] = 'high'
        
        # Count occurrences of each car type and sort the dictionary
        type_counts = df['car_type'].value_counts()
        type_counts = dict(sorted(type_counts.items()))
        
        return type_counts
    except Exception as e:
        print(f"An error occurred in get_type_count: {e}")
        return None

# Function to get indexes of rows where 'bus' values are greater than twice the mean
def get_bus_indexes(df):
    try:
        # Calculate the mean of 'bus' column
        bus_mean = df['bus'].mean()
        
        # Get indexes where 'bus' values are greater than 2 times the mean
        bus_indexes = df[df['bus'] > 2 * bus_mean].index.tolist()
        bus_indexes.sort()
        
        return bus_indexes
    except Exception as e:
        print(f"An error occurred in get_bus_indexes: {e}")
        return None

# Function to filter routes based on average 'truck' values
def filter_routes(df):
    try:
        # Calculate the average 'truck' values for each route
        route_avg_truck = df.groupby('route')['truck'].mean()
        
        # Get routes where the average 'truck' value is greater than 7
        filtered_routes = route_avg_truck[route_avg_truck > 7].index.tolist()
        filtered_routes.sort()
        
        return filtered_routes
    except Exception as e:
        print(f"An error occurred in filter_routes: {e}")
        return None

# Function to multiply values in a car matrix based on a condition
def multiply_matrix(car_matrix):
    try:
        # Create a copy of the car matrix
        modified_matrix = car_matrix.copy()
        
        # Multiply values by 0.75 if greater than 20, else multiply by 1.25
        modified_matrix = modified_matrix.applymap(lambda x: x * 0.75 if x > 20 else x * 1.25)
        
        # Round values to one decimal place
        modified_matrix = modified_matrix.round(1)
        
        return modified_matrix
    except Exception as e:
        print(f"An error occurred in multiply_matrix: {e}")
        return None

# Function to perform time-based checks on a DataFrame
def time_check(df):
    try:
        # Convert 'startDay' and 'endDay' columns to datetime format
        df['startTimestamp'] = pd.to_datetime(df['startDay'] + ' ' + df['startTime'], format='%A %X')
        df['endTimestamp'] = pd.to_datetime(df['endDay'] + ' ' + df['endTime'], format='%A %X')
        
        # Check conditions for each pair of 'id' and 'id_2'
        check_condition = (
            (df['startTimestamp'].dt.time == pd.Timestamp('00:00:00').time()) &
            (df['endTimestamp'].dt.time == pd.Timestamp('23:59:59').time()) &
            (df['startTimestamp'].dt.day_name() == df['endTimestamp'].dt.day_name())
        )
        
        # Aggregate results for each pair
        result_series = check_condition.groupby([df['id'], df['id_2']]).all()
        
        return result_series
    except Exception as e:
        print(f"An error occurred in time_check: {e}")
        return None

if __name__ == "__main__":
    try:
        # Read the CSV file into a DataFrame
        df = pd.read_csv(r"C:\Users\Aman\OneDrive\Desktop\MapUp\MapUp-Data-Assessment-F\datasets\dataset-1.csv")
        
        # Question 1: Generate and print the car matrix
        q1_result = generate_car_matrix(df)
        print(q1_result)
        
        # Question 2: Get and print the count of car types
        q2_result = get_type_count(df)
        print(q2_result)
        
        # Question 3: Get and print indexes of rows where 'bus' values are greater than twice the mean
        q3_result = get_bus_indexes(df)
        print(q3_result)
        
        # Question 4: Filter and print routes based on average 'truck' values
        q4_result = filter_routes(df)
        print(q4_result)
        
        # Question 5: Multiply values in the car matrix based on a condition and print the result
        q5_result = multiply_matrix(q1_result)
        print(q5_result)
        
        # Read the second CSV file into a DataFrame for Question 6
        df_q6 = pd.read_csv(r"C:\Users\Aman\OneDrive\Desktop\MapUp\MapUp-Data-Assessment-F\datasets\dataset-2.csv")
        
        # Question 6: Perform time-based checks and print the result
        q6_result = time_check(df_q6)
        print(q6_result)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
