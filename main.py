import pandas as pd
import multiprocessing as mp
import time
import os
import csv
from datetime import datetime


class FileNotFoundError(Exception):
    pass


class MissingColumnError(Exception):
    pass


class FileLoader:
    """
        Method for loading the file
    """
    def load_file(file_path):
        try:
            if not os.path.isfile(file_path):
                raise FileNotFoundError(f"Error: The file '{file_path}' does not exist.")
            try:
                data = pd.read_csv(file_path)
            except Exception as e:
                raise Exception(f"Error: Unable to read the file '{file_path}'. {e}")

            required_columns = ['Country', 'Total Profit', 'Order Date']
            missing_columns = [col for col in required_columns if col not in data.columns]
            if missing_columns:
                raise MissingColumnError(f"Error: Missing required columns: {', '.join(missing_columns)}.")

            return data
        except (FileNotFoundError, MissingColumnError, Exception) as e:
            print(e)
            return None


class DataProcessor:
    """
        class for all the methods, modeling and
    """
    @staticmethod

    def format_currency(amount):
        """
            formating currency
        :return: currency in better format;
        """
        try:
            return f"{amount:,.2f} USD"
        except Exception as e:
            print(f"Error in formatting currency: {e}")
            raise
    @staticmethod
    def calculate_profit(chunk, country):
        """
            Calculating profit of a country
        :param country: country name in colummn
        :return: all countries profit
        """
        try:
            if country not in chunk['Country'].values:
                raise ValueError(f"Country '{country}' not found in the data chunk.")
            return chunk[chunk['Country'] == country]['Total Profit'].sum()
        except Exception as e:
            print(f"Error in calculating profit: {e}")
            raise

    @staticmethod

    def analyze_trends_by_year(chunk):
        """
            Method for analyzing trends about years
        :return:
        """
        try:
            chunk['Year'] = pd.to_datetime(chunk['Order Date'], errors='coerce').dt.year
            chunk = chunk.dropna(subset=['Year'])
            return chunk.groupby('Year')['Total Profit'].sum().to_dict()
        except Exception as e:
            print(f"Error in analyzing trends by year: {e}")
            raise

    @staticmethod
    def validate_and_clean_data(chunk):
        """
            Method for validating and cleaning data from file
        :param chunk:
        :return:
        """
        try:
            initial_count = len(chunk)
            chunk = chunk.dropna(subset=['Country', 'Total Profit', 'Order Date'])
            chunk['Total Profit'] = pd.to_numeric(chunk['Total Profit'], errors='coerce')
            chunk = chunk.dropna(subset=['Total Profit'])
            chunk['Order Date'] = pd.to_datetime(chunk['Order Date'], errors='coerce')
            chunk = chunk.dropna(subset=['Order Date'])
            chunk = chunk[chunk['Units Sold'] > 0]
            chunk = chunk[chunk['Unit Price'] > 0]
            chunk = chunk[chunk['Unit Cost'] > 0]
            chunk = chunk[chunk['Total Revenue'] > 0]
            current_date = pd.to_datetime('today')
            chunk = chunk[chunk['Order Date'] <= current_date]
            cleaned_count = len(chunk)
            invalid_count = initial_count - cleaned_count
            return chunk, invalid_count
        except Exception as e:
            print(f"Error in data cleaning: {e}")
            raise
    @staticmethod
    def calculate_average_profit_per_order(chunk):
        """
            Calculating
        :param chunk:
        :return:
        """
        try:
            if chunk.empty:
                raise ValueError("Chunk is empty. Cannot calculate average profit.")

            total_profit = chunk['Total Profit'].sum()
            num_orders = chunk.shape[0]

            min_profit = chunk['Total Profit'].min()
            max_profit = chunk['Total Profit'].max()
            avg_profit = total_profit / num_orders if num_orders > 0 else 0

            print(f"Total profit: {total_profit}")
            print(f"Number of orders: {num_orders}")
            print(f"Min profit per order: {min_profit}")
            print(f"Max profit per order: {max_profit}")
            print(f"Average profit per order: {avg_profit}")

            with open("profit_analysis_log.txt", "a") as log_file:
                log_file.write(f"Avg Profit: {avg_profit}, Min Profit: {min_profit}, Max Profit: {max_profit}\n")

            return avg_profit
        except Exception as e:
            print(f"Error in calculating average profit per order: {e}")
            raise

    @staticmethod
    def calculate_profit_margin(chunk):
        """
            Calculating margin profit
        :param chunk:
        :return:
        """
        try:
            if 'Total Profit' in chunk.columns and 'Total Revenue' in chunk.columns and 'Item Type' in chunk.columns:
                chunk['Profit Margin'] = chunk['Total Profit'] / chunk['Total Revenue']
                return chunk[['Item Type', 'Profit Margin']]
            else:
                raise MissingColumnError("Missing required columns: 'Total Profit', 'Total Revenue', or 'Item Type'")
        except Exception as e:
            print(f"Error in calculating profit margin: {e}")
            raise

    @staticmethod
    def calculate_average_profit_by_country(chunk):
        """
            Method for calculating profit for each country
        :param chunk:
        :return:
        """
        try:
            chunk['Country'] = chunk['Country'].str.strip()
            avg_profit_by_country = chunk.groupby('Country')['Total Profit'].apply(
                lambda x: x.sum() / x.count()).to_dict()
            sorted_avg_profit_by_country = dict(sorted(avg_profit_by_country.items()))

            for country, avg_profit in sorted_avg_profit_by_country.items():
                print(f"Country: {country}, Average Profit: {DataProcessor.format_currency(avg_profit)}")

            return sorted_avg_profit_by_country
        except Exception as e:
            print(f"Error in calculating average profit by country: {e}")
            raise

    @staticmethod
    def analyze_profit_by_region(chunk):
        """
            Method for analyzing profit by region
        :param chunk:
        :return:
        """
        try:
            if 'Region' not in chunk.columns:
                raise MissingColumnError("Missing 'Region' column. Cannot analyze profit by region.")
            profit_by_region = chunk.groupby('Region')['Total Profit'].sum().to_dict()
            sorted_profit_by_region = dict(sorted(profit_by_region.items(), key=lambda item: item[1], reverse=True))

            for index, (region, profit) in enumerate(sorted_profit_by_region.items(), start=1):
                print(f"{index}. Region: {region}, Total Profit: {DataProcessor.format_currency(profit)}")

            return sorted_profit_by_region
        except Exception as e:
            print(f"Error in analyzing profit by region: {e}")
            raise

    def compare_profits(data, country1, country2):
        """
            Method for comparing profits
        :param country1: user input
        :param country2: user input
        :return:
        """
        try:
            print("Processing data...")
            start_processing_time = time.time()
            chunk_size = len(data) // 4
            chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
            with mp.Pool(processes=4) as pool:
                results_country1 = pool.starmap(DataProcessor.calculate_profit, [(chunk, country1) for chunk in chunks])
                results_country2 = pool.starmap(DataProcessor.calculate_profit, [(chunk, country2) for chunk in chunks])

            total_profit_country1 = sum(results_country1)
            total_profit_country2 = sum(results_country2)
            end_processing_time = time.time()

            if total_profit_country1 == 0:
                print(f"Warning: '{country1}' does not exist in the dataset or was entered incorrectly.")
            else:
                print(f"Total profit for {country1}: {DataProcessor.format_currency(total_profit_country1)}")

            if total_profit_country2 == 0:
                print(f"Warning: '{country2}' does not exist in the dataset or was entered incorrectly.")
            else:
                print(f"Total profit for {country2}: {DataProcessor.format_currency(total_profit_country2)}")

            # Adding comparison of profits
            if total_profit_country1 > total_profit_country2:
                profit_diff = total_profit_country1 - total_profit_country2
                print(f"{country1} has a higher profit by {DataProcessor.format_currency(profit_diff)}.")
            elif total_profit_country2 > total_profit_country1:
                profit_diff = total_profit_country2 - total_profit_country1
                print(f"{country2} has a higher profit by {DataProcessor.format_currency(profit_diff)}.")
            else:
                print(f"Both {country1} and {country2} have equal profits.")

            print(f"Processing completed in {end_processing_time - start_processing_time:.2f} seconds\n")
        except Exception as e:
            print(f"Error in comparing profits: {e}")
            raise

    @staticmethod
    def analyze_trends(data, country):
        """
            Method for predicting future trends
        :param data:
        :param country:
        :return:
        """
        try:
            print(f"Analyzing trends by year for {country}...")
            start_time = time.time()
            chunk_size = len(data) // 4
            chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

            with mp.Pool(processes=4) as pool:
                results = pool.starmap(analyze_trends_for_country, [(chunk, country) for chunk in chunks])

            yearly_top_products = {}
            for result in results:
                for _, row in result.iterrows():
                    year = row['Year']
                    if year not in yearly_top_products:
                        yearly_top_products[year] = []
                    yearly_top_products[year].append(row['Item Type'])

            for year, products in sorted(yearly_top_products.items()):
                print(f"Year {year}: {', '.join(products)}")

            print(f"Trend analysis completed in {time.time() - start_time:.2f} seconds\n")
        except Exception as e:
            print(f"Error in analyzing trends: {e}")
            raise

    @staticmethod
    def validate_data(data):
        """
            Method for validating data in csv
        :param data:
        :return:
        """
        try:
            print("Validating and cleaning data...")
            start_time = time.time()
            chunk_size = len(data) // 4
            chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
            with mp.Pool(processes=4) as pool:
                results = pool.map(DataProcessor.validate_and_clean_data, chunks)

            cleaned_chunks, invalid_counts = zip(*results)
            cleaned_data = pd.concat(cleaned_chunks, ignore_index=True)
            total_invalid = sum(invalid_counts)
            print(f"Data validation and cleaning completed in {time.time() - start_time:.2f} seconds")
            print(f"Total invalid rows removed: {total_invalid}\n")
            return cleaned_data
        except Exception as e:
            print(f"Error in validating data: {e}")
            raise

    @staticmethod
    def calculate_avg_profit(data):
        """
            Method for calculating average profit
        :param data:
        :return:
        """
        try:
            print("Calculating average profit per order...")
            start_time = time.time()
            chunk_size = len(data) // 4
            chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
            with mp.Pool(processes=4) as pool:
                results = pool.map(DataProcessor.calculate_average_profit_per_order, chunks)
            avg_profit = sum(results) / len(results) if results else 0
            print(f"Average profit per order: {DataProcessor.format_currency(avg_profit)}")
            print(f"Calculation completed in {time.time() - start_time:.2f} seconds\n")
        except Exception as e:
            print(f"Error in calculating average profit: {e}")
            raise

    @staticmethod
    def calculate_profit_margin_data(data):
        """
            Method for calculating the margin profit
        :param data:
        :return:
        """
        try:
            print("Calculating profit margin...")
            start_time = time.time()
            chunk_size = len(data) // 4
            chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
            with mp.Pool(processes=4) as pool:
                results = pool.map(DataProcessor.calculate_profit_margin, chunks)
            all_margins = pd.concat(results, ignore_index=True)
            for index, margin in all_margins.head(10).iterrows():
                print(f"Product Name: {margin['Item Type']}, Profit Margin: {margin['Profit Margin']:.2%}")
            print(f"Profit margin calculation completed in {time.time() - start_time:.2f} seconds\n")
        except Exception as e:
            print(f"Error in calculating profit margin data: {e}")
            raise

def analyze_trends_for_country(chunk, country):
    """
        Method for analyzing countries profit and best profitable item
    :param chunk:
    :param country:
    :return:
    """
    try:
        chunk['Year'] = pd.to_datetime(chunk['Order Date'], errors='coerce').dt.year
        chunk = chunk[chunk['Country'] == country]
        top_products_by_year = chunk.groupby(['Year', 'Item Type']).agg({'Units Sold': 'sum'}).reset_index()
        top_products_by_year = top_products_by_year.sort_values(['Year', 'Units Sold'], ascending=[True, False])
        top_products_by_year = top_products_by_year.groupby('Year').head(3)
        return top_products_by_year
    except Exception as e:
        print(f"Error in analyzing trends for country '{country}': {e}")
        raise




def process_chunk(chunk, dest_file):
    with open(dest_file, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerows(chunk)


def read_csv_in_chunks(src_file, chunk_size):
    """
        :parameterrthod for diversing the csv dile in chunks
    :param src_file:
    :param chunk_size:
    :return:
    """
    with open(src_file, mode='r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        chunk = []
        for row in reader:
            chunk.append(row)
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk


def copy_csv_parallel(src_file, dest_folder, num_processes=4, chunk_size=50000):
    """
        method forcopiing csv pararerlly

    """
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest_file = os.path.join(dest_folder, f"copied_file_{timestamp}.csv")

    pool = mp.Pool(processes=num_processes)
    chunks = read_csv_in_chunks(src_file, chunk_size)

    results = []
    for chunk in chunks:
        results.append(pool.apply_async(process_chunk, args=(chunk, dest_file)))

    pool.close()
    pool.join()

    print(f"Soubor {src_file} byl úspěšně zkopírován do {dest_file}.")


class MainApp:
    """
        main branch of the program
    """
    def __init__(self, file_path):
        self.file_path = file_path
        self.data = None

    def load_data(self):
        """
            Loading data method
        :return: loaded data
        """
        try:
            print("Loading data...")
            start_load_time = time.time()
            self.data = FileLoader.load_file(self.file_path)
            if self.data is None:
                raise ValueError("Failed to load the file.")
            print(f"File loaded successfully in {time.time() - start_load_time:.2f} seconds\n")
        except Exception as e:
            print(f"Error in loading data: {e}")
            raise

    def menu(self):
        """
            Menu with options
        :return:
        """
        try:
            while True:
                print("Menu:")
                print("1. Compare profits between two countries")
                print("2. Analyze yearly trends")
                print("3. Validate and clean data")
                print("4. Calculate average profit per order")
                print("5. Calculate profit margin")
                print("6. Calculate average profit by country")
                print("7. Analyze profit by region")
                print("8. Copy file")
                print("9. Exit")

                choice = input("Enter your choice: ")

                if choice == '1':
                    country1 = input("Enter first country: ")
                    country2 = input("Enter second country: ")
                    DataProcessor.compare_profits(self.data, country1, country2)
                elif choice == '2':
                    country = input("Enter the country for trend analysis: ")
                    DataProcessor.analyze_trends(self.data, country)
                elif choice == '3':
                    self.data = DataProcessor.validate_data(self.data)
                elif choice == '4':
                    DataProcessor.calculate_avg_profit(self.data)
                elif choice == '5':
                    DataProcessor.calculate_profit_margin_data(self.data)
                elif choice == '6':
                    avg_profit_by_country = DataProcessor.calculate_average_profit_by_country(self.data)
                    for country, avg_profit in avg_profit_by_country.items():
                        print(f"Average profit for {country}: {DataProcessor.format_currency(avg_profit)}")
                elif choice == '7':
                    profit_by_region = DataProcessor.analyze_profit_by_region(self.data)
                    for region, profit in profit_by_region.items():
                        print(f"Profit for {region}: {DataProcessor.format_currency(profit)}")
                elif choice == '8':
                    dest_folder = 'new_file'
                    copy_csv_parallel("records.cvs", dest_folder)
                elif choice == '9':
                    break
                else:
                    print("Invalid choice. Please try again.")
        except Exception as e:
            print(f"Error in menu operation: {e}")
            raise

if __name__ == "__main__":
    app = MainApp("record.csv")
    app.load_data()
    app.menu()
