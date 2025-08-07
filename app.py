from lib import file_load

file = file_load.list_csv_files(folder_path='data')

print(file)

df = file_load.read_latest_csv(folder_path='data', encoding="latin1")

print(df.head())