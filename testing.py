import pandas as pd

# Load your output
output = pd.read_csv('data/assigned-rides-output (3).csv')
output_ids = set(output['trapeze client id'].astype(str))

# Load actual assignments, only customer id and provider columns
actual = pd.read_excel('data/6-25-2025 Trip Cancellation.xlsx', usecols=['Customer ID', 'Provider'])
# Filter out 'uberx' providers
actual = actual[~actual['Provider'].str.lower().str.contains('uberx')]
actual_ids = set(actual['Customer ID'].astype(str))

# Rides in actual but not in your output
missed = actual_ids - output_ids
# Rides in your output but not in actual
extra = output_ids - actual_ids

print(f'Rides assigned in actual but missing in your output: {len(missed)}')
print(f'Rides assigned in your output but not in actual: {len(extra)}')

# Optionally, print the IDs
print('Missed customer ids:', missed)
print('Extra customer ids:', extra)