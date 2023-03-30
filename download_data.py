import requests
import itertools
import argparse
from calendar import monthrange

def one_cipher_to_str(num:int): return num if len(str(num)) != 1 else f'0{num}'

def retrieve_data(year:str, month:str)->None:
    print("Downloading data...")
    days_in_month = monthrange(int(year), int(month))[-1]
    for day, hour in itertools.product(*(range(1, days_in_month +1), range(24))):
        filename = f'{year}-{month}-{one_cipher_to_str(day)}-{hour}.json.gz'
        print(f'https://data.gharchive.org/{filename}')
        with open(f'./data/{filename}', 'wb') as f:
            f.write(requests.get(f'https://data.gharchive.org/{filename}').content)
    
    print("Data downloaded succesfully...")
    


if __name__ == '__main__':

    argParser = argparse.ArgumentParser()
    argParser.add_argument("-y", "--year", help="Year of the report")
    argParser.add_argument("-m", "--month", help="Month of the report")

    args = argParser.parse_args()

    retrieve_data(args.year, args.month)