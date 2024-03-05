import os
import json  
import time
import pandas as pd

from src.app.utils.utils import Utils
from src.app.utils.service_result import ServiceResult

class MainService:
    
    @staticmethod
    def csv_data_ingestion():
        
        try:
            directory = os.environ["INGESTION_DEFAULT_DATA_PATH"]

            for filename in os.listdir(directory):
                if filename.endswith(".csv"):
                    file_path = os.path.join(directory, filename)
                    
                    df = pd.read_csv(file_path)
                    
                    df_out = MainService.transformation(df)
                        
            
        except ValueError as e:
            return ServiceResult({
                    "status_code": 404,
                    "message": "Problem while ingesting data.",
                    "data": [e]
                })
            
        return ServiceResult({
            "status_code": 200,
            "message": "Load finished.",
            "data": []
        })                
    
    
    @staticmethod
    def export_data(data_ingestion: pd.DataFrame, step_id: str, type: str):
        path = os.environ["EXPORT_DEFAULT_DATA_PATH"]
        file_name = f"{str(time.time()).replace('.','_')}.{type}"
        
        path_file_name = f"{path}/Step{step_id}_{file_name}"
        print(path_file_name)
                
        if Utils.file_exists(path, file_name+f".{type}"):
            print("file exists...")
            df = pd.read_parquet(path_file_name)
            
            MainService.check_existence_by_name(df)
        else:
            print("creating file...")
            if type == 'csv':
                data_ingestion.to_csv(path_file_name)
            else:
                data_ingestion.to_parquet(path_file_name) 
            
            print(path_file_name, " ", Utils.file_exists(path, file_name) )
            
        return True
    
    
    def check_existence_by_name(data_ingestion):
        return True
    
    
    def transformation_step2(df: pd.DataFrame):
        print("\n\nstep2")        
        #RULE 2.a
        rule2a_col = "pdays"
        if rule2a_col in df.columns:
            df = df[df[rule2a_col] != -1]
        else:
            print(f"No {rule2a_col} column.")
        
        #RULE 2.b
        rule2b_col = "name"
        if rule2b_col in df.columns:
            df[['first_name', 'second_name']] = df[rule2b_col].str.split(' ', 1, expand=True)
            
            df.drop(rule2b_col, axis=1, inplace=True)
        else:
            print(f"No {rule2b_col} column.")
    
        #RULE 2.c
        rule2c_col = "age"
        if rule2c_col in df.columns:
            df[rule2c_col] = [ Utils.age_to_bucket(age) for age in df[rule2c_col] ]
        else:
            print(f"No {rule2c_col} column.")
        
        #RULE 2.d
        columns_2d = []
        
        for col in df.columns:
            if df[col].iloc[0]=="yes" or df[col].iloc[0] == "no":
                columns_2d.append(col)
        
        for col in columns_2d:
            df[col] = df[col].replace({'yes': True, 'no': False})
            
        #RULE 2.e
        month_map = {'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04', 'may': '05',
              'jun': '06', 'jul': '07', 'aug': '08', 'sep': '09', 'oct': '10',
              'nov': '11', 'dec': '12'}
        
        df['month'] = df['month'].apply(lambda x: month_map.get(x.lower()))
        df['date'] = df['day'].astype(str) + '/' + df['month']
        
        #RULE 2.f
        df.rename(columns={'y': 'outcome'}, inplace=True)
        
        return df
    
    def transformation_step3(df: pd.DataFrame):
        print("\n\nstep3...")
        
        #RULE 3.a/b/c
        rule3_col = "address"
        if rule3_col in df.columns:
            df["address_category_water"]  = [ Utils.categorize_address(address, "water")  for address in df[rule3_col] ]
            df["address_category_relief"] = [ Utils.categorize_address(address, "relief") for address in df[rule3_col] ]
            df["address_category_flat"]   = [ Utils.categorize_address(address, "flat")   for address in df[rule3_col] ]
        else:
            print(f"No {rule3_col} column.")  
        
        return df

    def transformation(df: pd.DataFrame):
        df = MainService.transformation_step2(df)
        MainService.export_data(data_ingestion=df, step_id= 2, type='csv')
        MainService.export_data(data_ingestion=df, step_id= 2, type='parquet')
        
        df = MainService.transformation_step3(df)
        
        MainService.export_data(data_ingestion=df, step_id= 3, type='csv')
        MainService.export_data(data_ingestion=df, step_id= 3, type='parquet')
        
        return df
    
    def get_page(page_number, df):
        rows_per_page = 20
        start_index = (page_number - 1) * rows_per_page
        end_index = start_index + rows_per_page
        
        return df.iloc[start_index:end_index]
    
    def load_data(page_number=1, all_values=False):
        directory = os.environ["EXPORT_DEFAULT_DATA_PATH"]
        
        list_return = []
        file_loaded = []

        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            
            splitted_fname = filename.split("_")
            fname = splitted_fname[0]
            
            if fname not in file_loaded:
                file_loaded.append(splitted_fname[0])
            else:
                print(f"{fname} already added.")
                continue
            
            list_splited = filename.split(".")
            
            if list_splited[-1] == 'csv':
                df = pd.read_csv(file_path)
            elif list_splited[-1] == 'parquet':
                df = pd.read_parquet(file_path)
            else:
                print(f"type not found {list_splited[-1]}.")
                continue
            
            if not all_values:
                df = MainService.get_page(page_number, df)
            
            list_return.append(df)
        
        return list_return
        
    @staticmethod
    def get_data(page_number: int=1, all_values: bool=False):
        df_list = MainService.load_data(page_number, all_values)
        
        list_return = []
        
        for df in df_list:            
            list_return.append(json.loads(df.to_json()))
        
        return list_return
        
    
    def get_category_by_age(group_by_type: str):
        df_list = MainService.load_data(all_values=True)
        
        list_return = []
        
        for df in df_list:
            if group_by_type in df.columns:
            
                df_result = (df[df[group_by_type].notna()]  # Filter non-empty feature values
                            .groupby('age')  # Group by the 'feature' column
                            .apply(lambda x: x.sort_values(group_by_type).count()))  # Apply sort and count within each group
            
                list_return.append({group_by_type: json.loads(df_result[group_by_type].to_json())})
        
        return list_return
    
    def get_age_amount():
        df_list = MainService.load_data(all_values=True)
        
        list_return = []
        
        for df in df_list:
            values = json.loads(df["age"].value_counts().to_json())
            if values not in list_return:
                list_return.append(values)
        
        return list_return