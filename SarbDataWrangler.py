
'''
In this file we will be doing the data wrangling for the SARB data set.
We then use data wrangling methods not included in this section to get the data into the format we now have. 
BA700 cvs's which are csv files containing Capital adquecy and leverage data. BA120 csv's which are csv files containing Income statment data.
Data Source: https://www.resbank.co.za/en/home/what-we-do/Prudentialregulation/Sector_data/banking-sector-data/BA-returns-of-total-banks-data\
'''

import os
import pandas as pd
import csv

class SARBDataWrangler:
    def __init__(self, data_folder, year):
        self.data_folder = data_folder
        self.year = year
        self.path = os.path.join(os.getcwd(), 'data', self.data_folder, self.year)
        self.filetype = self.data_folder.split('_')[0]
        self.save_all = os.path.join(os.getcwd(), 'data', self.data_folder)
        
    def get_filenames(self):
        os.chdir(self.path)
        return os.listdir()
    
    def convert_csv_to_df(self, filename):
        '''
        This method wll convert 12 csv files into 12 dataframes
        '''

        lines = []
        with open(filename, 'r') as readFile:
            reader = csv.reader(readFile)
            for row in reader:
                lines.append(row)
        get_date = lines[0][1].split(' ')
        get_date.insert(0, f'{self.filetype}')
        if self.filetype == 'BA120':
            table_1 = lines[5:35]+lines[44:47]+lines[76:89]+lines[95:102] #here i manually sellect the table rows
            table_1.insert(0, get_date)
            returned_table = pd.DataFrame(table_1)
            #print(returned_table)
            return returned_table
        elif self.filetype == 'BA700':
            table_1 = lines[6:15]
            table_1.insert(0, get_date)
            returned_table = pd.DataFrame(table_1)
            print(returned_table)
            return returned_table
        
        
    
    def create_data_file(self):
        self.get_filenames()
        descriptions = pd.DataFrame(self.convert_csv_to_df(f'I9999999_{self.year}-01-01.csv')[0])
        all_combined_as_list = [self.convert_csv_to_df(t)[2] for t in self.get_filenames()]
        all_data_tables_combined = pd.DataFrame(all_combined_as_list).T
        combined_table = pd.concat([descriptions.T, all_data_tables_combined.T]).T.drop(index=0)[1:]
        column_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        index_col = combined_table.iloc[0][0]
        column_names.insert(0, index_col)
        combined_table.columns = column_names
        
        if self.filetype == 'BA120': 
            c = combined_table[1:]
            Table_final = c.set_index('Description of item')
            print(Table_final)
            #Table_final = c.set_index('Description of item')
            Table_final.to_excel(f'{self.save_all}\All\{self.filetype}_{self.year}.xlsx')
        else:
            c = combined_table[2:].drop([6])
            c.to_excel(f'{self.save_all}\All\{self.filetype}_{self.year}.xlsx', index=False)
        
        print(c)
    
    def combine_all_files(self):
        all_data_folder = os.path.join(os.getcwd(), 'data', self.data_folder, 'All')
        os.chdir(all_data_folder)
        files = os.listdir()
        year_range = pd.date_range('2013-01-01', f'{self.year}-12-30', freq='MS').strftime("%Y-%b").tolist() #BA120 starts from 2015
        combined_csv = pd.concat([pd.read_excel(f).T for f in files], axis=0)
        combined_csv.columns = combined_csv.iloc[0]
        combined_csv = combined_csv.drop(combined_csv.index[0])
        try:
            combined_csv['Year'] = year_range
            combined_csv = combined_csv.set_index('Year').round(decimals=4)
        except:
            Exception('Year range does not match the number of files change tear range in code')
        combined_csv.to_excel(f'{self.save_all}\All_{self.data_folder}.xlsx')
        print('File saved in the All folder')
        print(combined_csv)

# Usage example
SARB_Data = 'BA700_Data'
YEAR = '2022'

data_wrangler = SARBDataWrangler(SARB_Data, YEAR)

#this function combines 12 months of data into one file for 1 year
#data_wrangler.create_data_file()

#this function combines all the years into one file
data_wrangler.combine_all_files()
