import os
import time
import dask.dataframe as dd

def main():
    """
    This script reads in a dask dataframe and handles redundancies based on a ID columns.
    This script was written to handle redundancies in over 15 million records.
    """

    start_time = time.time()

    # TODO SET VARIABLES HERE
    file_path = r'ENTER_PATH_HERE'
    file_name = r'ENTER_NAME_HERE'
    out_csv_name = r'ENTER_OUTPUT_NAME'
    dtype = {}
    id_col_name = 'ENTER_ID_COL_NAME'

    # Load data with error handling
    try:
        # Load the Dask DataFrame
        df_dask = dd.read_csv(os.path.join(file_path, file_name), dtype=dtype)

    except (ValueError, FileNotFoundError) as e:
        print(f"Error loading data: {e}")
        raise


    # print number of unique IDs
    print('Number of original rows: ' + str(len(df_dask.index)))
    api_series =df_dask.NETL_INTERNAL_API.compute() 
    print('Unique IDs: ' + str(api_series.nunique()))

    # Subset out IDs that have no duplicates
    df_unique = df_dask[api_series.duplicated(keep=False)==False]
    df_dask = df_dask[api_series.duplicated(keep=False)==True]
    print('Number of IDs with only one record: '+ str(len(df_unique.index)))
    print('Number of IDs with multiple record: '+ str(len(df_dask.index)))

    # Aggregate with different values separated by ;
    custom_agg = dd.Aggregation(name = 'custom', chunk = lambda x: x.agg(lambda s: '; '.join([str(y) for y in s.unique() if str(y) != 'nan'] )), agg = lambda y: y.agg(lambda s: '; '.join([str(y) for y in s.unique() if str(y) != 'nan'] )))
    df_dask_agg = df_dask.groupby(id_col_name).agg(custom_agg).reset_index()

    # add back in df_unique (with no duplicates on API)
    df_dask_agg = dd.concat([df_dask_agg, df_unique])


    # compute into pandas df
    print('Computing')
    final_file_pd = df_dask_agg.compute()

    # Save final_file_pd to CSV
    final_file_path = os.path.join(file_path, out_csv_name)
    final_file_pd.to_csv(final_file_path, index=False)


    print(f"Data saved to {final_file_path}")

    # print time
    end_time = time.time()
    execution_time = end_time - start_time
    print("Execution time:", execution_time)

    print('fin')

if __name__ == "__main__":
    main()