import csv
from datetime import datetime
from collections import Counter

# Start timer
start_timestamp = datetime.now()

# Fetch the current year dynamically
current_year = datetime.now().year

# Read and parse student information
with open('data/students.csv', mode='r', encoding='utf-8') as student_file:
    student_reader = csv.DictReader(student_file)
    student_records = [row for row in student_reader]

# Parse and organize fee data by Student ID
fees_by_id = {}
with open('data/student_fees.csv', mode='r', encoding='utf-8') as fees_file:
    fees_reader = csv.DictReader(fees_file)
    for record in fees_reader:
        try:
            if record["Payment Status"].strip().lower() == "paid":
                # Parse date using the current year as fallback
                full_date = datetime.strptime(f"{record['Payment Date']} {current_year}", "%B %d %Y")
                student_id = record["Student ID"].strip()

                if student_id not in fees_by_id:
                    fees_by_id[student_id] = []

                fees_by_id[student_id].append(full_date)
        except (ValueError, KeyError) as error:
            print(f"Skipped record with invalid data: {record} - Error: {error}")

# Preview parsed fee data
print("Sample of processed fee records:", dict(list(fees_by_id.items())[:3]))

# Prepare results
analysis_results = []

for student in student_records:
    student_id = student["Student ID"].strip()
    fee_dates = fees_by_id.get(student_id, [])

    if fee_dates:
        # Count frequency of each day of the month
        day_counter = Counter(date.day for date in fee_dates)
        most_frequent_day = day_counter.most_common(1)[0]

        # Store analysis result
        analysis_results.append({
            "Student ID": student_id,
            "Most Frequent Submission Day": most_frequent_day[0],
            "Submission Count": most_frequent_day[1]
        })

# Preview analysis results
print("Analysis Results Sample:", analysis_results[:3])

# End timer
end_timestamp = datetime.now()

total_duration = end_timestamp - start_timestamp

print("\nAnalysis Summary:")
for result in analysis_results:
    print(f"Student ID: {result['Student ID']}, "
          f"Most Frequent Day: {result['Most Frequent Submission Day']}, "
          f"Submission Count: {result['Submission Count']}")

print("\nExecution Time Details:")
print(f"Started At: {start_timestamp}")
print(f"Finished At: {end_timestamp}")
print(f"Duration: {total_duration}")
