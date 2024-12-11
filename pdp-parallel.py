import dask.dataframe as dd
from datetime import datetime

# Start time recording
start_timestamp = datetime.now()
current_year = start_timestamp.year

# Load datasets
students_data = dd.read_csv('data/students.csv', encoding='utf-8')
fees_data = dd.read_csv('data/student_fees.csv', encoding='utf-8')

# Normalize payment status column
fees_data['Payment Status'] = fees_data['Payment Status'].str.strip().str.lower()

# Append the current year to payment date
fees_data['Payment Date'] = fees_data['Payment Date'].map_partitions(
    lambda partition: partition + f" {current_year}", meta=('Payment Date', 'str')
)
fees_data['Payment Date'] = dd.to_datetime(fees_data['Payment Date'], format='%B %d %Y', errors='coerce')

# Filter only paid fees
filtered_fees = fees_data[fees_data['Payment Status'] == 'paid']

# Extract day of the month from payment date
filtered_fees['Payment Day'] = filtered_fees['Payment Date'].dt.day

# Group by Student ID and Payment Day to count occurrences
payment_day_summary = (
    filtered_fees.groupby(['Student ID', 'Payment Day'])
    .size()
    .reset_index()
)
payment_day_summary.columns = ['Student ID', 'Payment Day', 'Total Payments']

# Convert Dask DataFrame to Pandas for further processing
payment_day_summary_pd = payment_day_summary.compute()

# Find the most frequent payment day for each student
frequent_payment_days = (
    payment_day_summary_pd.loc[
        payment_day_summary_pd.groupby('Student ID')['Total Payments'].idxmax()]
)

# Convert students data to Pandas and merge with payment summary
students_data_pd = students_data.compute()
result_data = students_data_pd.merge(
    frequent_payment_days[['Student ID', 'Payment Day', 'Total Payments']],
    on='Student ID',
    how='left'
)

# End time recording
end_timestamp = datetime.now()
execution_time = end_timestamp - start_timestamp

# Display results
print("Processed Output:")
print(result_data[['Student ID', 'Payment Day', 'Total Payments']])

print("\nExecution Timing:")
print(f"Started at: {start_timestamp}")
print(f"Finished at: {end_timestamp}")
print(f"Duration: {execution_time}")

