import csv
from collections import OrderedDict
import pandas as pd
import numpy as np

def read_fn():
    path = "data.txt"
    with open(path, "r") as f:
        for line in f:
            if line.startswith('0,ACCOUNT') or line.startswith(','):
                continue
            if line.count(",") > 7:
                continue

            yield ",".join(line.split(',')[1:])

reader = csv.reader(read_fn())
csv_trimmed_data = []
for row in reader:
    row_datatypes = []
    for i in row:
        i = i.strip()
        if i.isdigit():
            row_datatypes.append(float(i))
        elif i == '-' or i == '.':
            row_datatypes.append(float(0))
        elif not i:
            row_datatypes.append(float(0))
        else:
            row_datatypes.append(i)


    if row_datatypes not in csv_trimmed_data:
        csv_trimmed_data.append(row_datatypes)

df_data=pd.DataFrame(columns=csv_trimmed_data[0])
for i in range(len(csv_trimmed_data)-1):
    df_data.loc[i] = csv_trimmed_data[i+1]
# print(df_data.to_string())
df_data['Debit(Dr.)'] = df_data['Debit(Dr.)'].replace(" ", 0)
df_data["Debit(Dr.)"] = pd.to_numeric(df_data["Debit(Dr.)"])
# print(df_data.nlargest(10,"Debit(Dr.)").to_string())
print("="*60)
print("Top 10 Debit transactions from data.txt in below format \n \
 Description, date, debit, Credit value and balance.")
print("="*60)
print(df_data.nlargest(10,"Debit(Dr.)").loc[:, ['Description/Narration','Date', 'Debit(Dr.)', 'Credit(Cr.)', 'Balance' ]].to_string())


print("="*60)
print("Top 10 Credit transactions from data.txt in below format \n \
 Description, date, debit, Credit value and balance.")
print("="*60)
df_data['Credit(Cr.)'] = df_data['Credit(Cr.)'].replace(" ", 0)
df_data["Credit(Cr.)"] = pd.to_numeric(df_data["Credit(Cr.)"])
print(df_data.nlargest(10,"Credit(Cr.)").loc[:, ['Description/Narration', 'Date', 'Debit(Dr.)', 'Credit(Cr.)', 'Balance' ]].to_string())


# df_data['Balance'] = df_data['Balance'].replace(" ", 0)
# df_data["Balance"] = pd.to_numeric(df_data["Balance"])
# print(df_data.nlargest(10,"Balance").to_string())


print("="*60)
print("Group Similar transaction and Debit aggregation values.")
print("="*60)

grouped_multiple = df_data.groupby(['Description/Narration']).agg({'Debit(Dr.)': ['mean', 'min', 'max']})
grouped_multiple.columns = ['debit_mean', 'debit_min', 'debit_max']
grouped_multiple = grouped_multiple.reset_index()
print(grouped_multiple.to_string())

print("=" * 60)
print("Group Similar transaction and Credit aggregation values.")
print("=" * 60)

grouped_multiple = df_data.groupby(['Description/Narration']).agg({'Credit(Cr.)': ['mean', 'min', 'max']})
grouped_multiple.columns = ['credit_mean', 'credit_min', 'credit_max']
grouped_multiple = grouped_multiple.reset_index()
print(grouped_multiple.to_string())