import pandas as pd
from datetime import datetime, time

def calculate_distance_matrix(df):
    try:
        unique_nodes = []
        # Iterate through the DataFrame to dynamically determine unique nodes
        for _, row in df.iterrows():
            if row['id_start'] not in unique_nodes:
                unique_nodes.append(row['id_start'])
            if row['id_end'] not in unique_nodes:
                unique_nodes.append(row['id_end'])
        # Sort the unique nodes
        unique_nodes = sorted(unique_nodes)
        num_nodes = len(unique_nodes)
        # Initialize distance matrix with zeros
        distance_matrix = pd.DataFrame(0.0, index=map(int, unique_nodes), columns=map(int, unique_nodes))
        # Fill distance matrix with known distances
        for _, row in df.iterrows():
            distance_matrix.at[int(row['id_start']), int(row['id_end'])] = float(row['distance'])
            distance_matrix.at[int(row['id_end']), int(row['id_start'])] = float(row['distance'])
        # Calculate cumulative distances through intermediate nodes
        for k in map(int, unique_nodes):
            for i in map(int, unique_nodes):
                for j in map(int, unique_nodes):
                    if distance_matrix.at[i, j] == 0.0 and i != j:
                        # If distance is not known and not the same node, calculate the sum of distances through other points
                        if distance_matrix.at[i, k] != 0.0 and distance_matrix.at[k, j] != 0.0:
                            distance_matrix.at[i, j] = distance_matrix.at[i, k] + distance_matrix.at[k, j]
        return distance_matrix
    except Exception as e:
        print(f"An error occurred in calculate_distance_matrix: {e}")
        return None

def unroll_distance_matrix(df):
    try:
        unrolled_data = []
        # Iterate through the rows and columns of the distance matrix
        for start_node in df.index:
            for end_node in df.columns:
                # Exclude same start and end nodes
                if start_node != end_node:
                    distance = df.at[start_node, end_node]
                    unrolled_data.append({'id_start': start_node, 'id_end': end_node, 'distance': distance})
        # Create a DataFrame from the unrolled data
        unrolled_df = pd.DataFrame(unrolled_data)
        return unrolled_df
    except Exception as e:
        print(f"An error occurred in unroll_distance_matrix: {e}")
        return None

def find_ids_within_ten_percentage_threshold(df, reference_id):
    try:
        rate_coefficients = {'moto': 0.8, 'car': 1.2, 'rv': 1.5, 'bus': 2.2, 'truck': 3.6}

        for vehicle_type, rate_coefficient in rate_coefficients.items():
            df[vehicle_type] = df['distance'] * rate_coefficient
        reference_rows = df[df['id_start'] == reference_id]
        # fetch the avg dist. for the reference_id
        reference_average_distance = reference_rows['distance'].mean()
        # Calculate the threshold range (10% of the avg dist.)
        threshold_range = 0.1 * reference_average_distance
        # Filter DataFrame to include only rows within the threshold range
        result_df = df[(df['distance'] >= (reference_average_distance - threshold_range)) 
                        & (df['distance'] <= (reference_average_distance + threshold_range))]
        
        return result_df.sort_values(by=['id_start', 'id_end'])
    except Exception as e:
        print(f"An error occurred in find_ids_within_ten_percentage_threshold: {e}")
        return None

def calculate_toll_rate(df):
    try:
        # Define rate coefficients for each vehicle type
        rate_coefficients = {'moto': 0.8, 'car': 1.2, 'rv': 1.5, 'bus': 2.2, 'truck': 3.6}

        for vehicle_type, rate_coefficient in rate_coefficients.items():
            df[vehicle_type] = df['distance'] * rate_coefficient

        return df[['id_start', 'id_end', 'moto', 'car', 'rv', 'bus', 'truck']]
    except Exception as e:
        print(f"An error occurred in calculate_toll_rate: {e}")
        return None

def calculate_time_based_toll_rates(df):
    try:
        df['start_time'] = '00:00:00'
        df['end_time'] = '23:59:59'
        time_intervals = {
            'Weekdays': [('00:00:00', '10:00:00', 0.8), ('10:00:00', '18:00:00', 1.2), ('18:00:00', '23:59:59', 0.8)],
            'Weekends': [('00:00:00', '23:59:59', 0.7)]
        }

        result_data = []

        for (start_day, factors) in time_intervals.items():
            for (start_time, end_time, discount_factor) in factors:
                # Use only "%H:%M:%S" for the time format
                start_datetime = datetime.strptime(start_time, "%H:%M:%S")
                end_datetime = datetime.strptime(end_time, "%H:%M:%S")

                for _, row in df.iterrows():
                    distance = row['distance']
                    interval_start_time = datetime.strptime(row['start_time'], "%H:%M:%S").time()
                    interval_end_time = datetime.strptime(row['end_time'], "%H:%M:%S").time()

                    if interval_start_time <= start_datetime.time() <= interval_end_time and \
                       interval_start_time <= end_datetime.time() <= interval_end_time:
                        discounted_distance = distance * discount_factor
                        result_data.append({
                            'id_start': row['id_start'],
                            'id_end': row['id_end'],
                            'distance': row['distance'],
                            'start_day': start_day,
                            'start_time': start_datetime.time(),
                            'end_day': start_day,
                            'end_time': end_datetime.time(),
                            'moto': discounted_distance * 0.8,
                            'car': discounted_distance * 1.2,
                            'rv': discounted_distance * 1.5,
                            'bus': discounted_distance * 2.2,
                            'truck': discounted_distance * 3.6
                        })

        return pd.DataFrame(result_data)
    except Exception as e:
        print(f"An error occurred in calculate_time_based_toll_rates: {e}")
        return None
        
if __name__ == "__main__":
    # Read the CSV file into a DataFrame
    df = pd.read_csv(r"C:\Users\Aman\OneDrive\Desktop\MapUp\MapUp-Data-Assessment-F\datasets\dataset-3.csv")
    q1_result = calculate_distance_matrix(df)
    print("Question 1 Result:")
    print(q1_result)
    q2_result = unroll_distance_matrix(q1_result)
    print("\nQuestion 2 Result:")
    print(q2_result)
    q3_result = find_ids_within_ten_percentage_threshold(q2_result, 1001400)
    print("\nQuestion 3 Result:")
    print(q3_result)
    q4_result = calculate_toll_rate(q2_result)
    print("\nQuestion 4 Result:")
    print(q4_result)
    q5_result = calculate_time_based_toll_rates(q3_result)
    print("\nQuestion 5 Result:")
    print(q5_result)