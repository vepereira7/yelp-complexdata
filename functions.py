import pandas as pd 

def missing_values_table(df, name):
    mis_val = df.isnull().sum()
    mis_val_percent = 100 * df.isnull().sum() / len(df)
    mis_val_table = pd.concat([mis_val, mis_val_percent], axis=1)
    mis_val_table_ren_columns = mis_val_table.rename(
    columns = {0 : 'Missing Values', 1 : '% of Total Values'})
    mis_val_table_ren_columns = mis_val_table_ren_columns[
        mis_val_table_ren_columns.iloc[:,1] != 0].sort_values(
    '% of Total Values', ascending=False).round(1)
    print ("The " + str(name) + " dataframe has " + str(df.shape[0]) + " rows and " + str(df.shape[1]) + " columns.\n"      
        "There are " + str(mis_val_table_ren_columns.shape[0]) +
            " columns that have missing values.")
    return mis_val_table_ren_columns



def clean_df(connections, threshold: int):
    connections = connections[~(connections['connection_id'] == 'None')]
    # Count the occurrences of each value in 'connection_id'
    value_counts = connections['connection_id'].value_counts()

    # Create a mask to filter rows where value counts are >= 10
    mask = connections['connection_id'].map(value_counts) >= threshold

    # Apply the mask to filter the DataFrame
    connections = connections[mask].reset_index(drop=True)

    return connections




def create_nodes_and_links(connections):
    # Concatenate the two columns and get unique values
    unique_values = pd.concat([connections['user_id'], connections['connection_id']]).unique()

    # Convert the unique values to a list
    unique_values_list = unique_values.tolist()

    df = pd.DataFrame({'unique_values': unique_values_list})

    # Create a mapping of unique values to unique IDs
    unique_id_mapping = {value: f's{i:02}' for i, value in enumerate(df['unique_values'], start=1)}

    # Create a new column 'unique_id' based on the mapping
    df['unique_id'] = df['unique_values'].map(unique_id_mapping)

    # Add 'from' and 'to' columns to the 'connections' DataFrame
    connections['from'] = connections['user_id'].map(unique_id_mapping)
    connections['to'] = connections['connection_id'].map(unique_id_mapping)

    # Drop unwanted columns
    cols = ['user_id', 'connection_id']
    connections = connections.drop(columns=cols)

    # Reorder dataframe
    desired_column_order = ['unique_id', 'unique_values']
    # Reassign the DataFrame with the desired column order
    df = df[desired_column_order]

    return connections, df