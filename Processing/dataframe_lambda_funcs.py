import pandas as pd

# Load data
df = pd.read_csv(r'')

# ------------------------------ EXAMPLE ONE ------------------------------
# Function to apply changes in one column. Calculating cost when it is null from other cost columns
def calc_cost(row):
   if str(row['COST_COLUMN']) == 'nan':
      return row['COST_TYPE1'] + row['COST_TYPE2'] + row['COST_TYPE3'] + row['COST_TYPE3']
   else:
      return row['COLUMN_NAME']


df['COLUMN_NAME'] = df.apply (lambda row: calc_cost(row), axis=1)

# ------------------------------ EXAMPLE TWO ------------------------------
# Set a value in NEW_COLUMN based on values in other columns
df['NEW_COLUMN'] = df.apply(lambda row: True if row['EX_COL1'] > row['EX_COL2'] else False, axis=1)


# ------------------------------ EXAMPLE TWO ------------------------------
# Set a value in NEW_COLUMN summing other columns
df['NEW_COLUMN'] = df.apply(lambda row: row['EX_COL1'] + row['EX_COL2'], axis=1)