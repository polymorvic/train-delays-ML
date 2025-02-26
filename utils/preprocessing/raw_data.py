import pickle, os
import pandas as pd
from typing import Callable

SaveMethodSelector = dict[str, Callable]

class TrainDelaysRawDataFileLoader:
    raw_data_dir: str = 'data/raw'

    def __init__(self, filename: str) -> None:
        self.filename: str = filename
        self.filepath: str = f'{self.raw_data_dir}/{self.filename}'
        self.__raw_data: list[pd.DataFrame] = None

        if not os.path.isfile(self.filepath):
            raise FileNotFoundError(f"File '{self.filepath}' not found.")
        
        if not os.path.isdir(os.path.dirname(self.filepath)):
            raise NotADirectoryError(f"Directory '{os.path.dirname(self.filepath)}' does not exist.")
        
        if self.filename.endswith(('.pickle', '.pkl')):
            self.__load_raw_data()
        else:
            raise ValueError(f"Only pickle extension is supported - you provided: {self.filename.split('.')[-1]}")

    def __load_raw_data(self) -> None:
        try:
            with open(self.filepath, 'rb') as file:
                self.__raw_data: list[pd.DataFrame] = pickle.load(file)
                self.__apply_id()
        except Exception as e:
            raise ValueError(f"Error loading pickle file: {e}")
        
    def __apply_id(self) -> None:
        for i, df in enumerate(self.__raw_data):
            df.insert(0, 'id', i)
        
    def get_data(self) -> list[pd.DataFrame]:
        return self.__raw_data
    
    @property
    def data_length(self) -> tuple[int, list[int]]:
        return len(self.__raw_data), [len(df) for df in self.__raw_data] if self.__raw_data is not None else ''

class TrainDelaysRawDataHandler(TrainDelaysRawDataFileLoader):
    preprocessed_data_dir: str = 'data/preprocessed'

    def __init__(self, filename: str, out_filename: str, autosave: bool = True):
        super().__init__(filename)
        self.extension = out_filename.split('.')[-1].lower()

        if self.extension not in ('csv', 'parquet'):
            raise ValueError('Unknown extension or dot not provided.')

        self.out_filename = out_filename
        self.output_fullpath: str = f'{self.preprocessed_data_dir}/{self.out_filename}'
        self.autosave: bool = autosave
        self.save_function_caller: SaveMethodSelector = {
            'parquet': self.__save_to_parquet,
            'csv': self.__save_to_csv
        }
        self.__raw_merged_data: pd.DataFrame = self.__combine_dataframes()

        if self.autosave:
            self.save_function_caller.get(self.extension)(self.output_fullpath)

    def __combine_dataframes(self):
        try:
            df = pd.concat(self.get_data(), ignore_index=True)
            df.columns = df.columns.str.strip().str.lower()
            return df
        except Exception as e:
            raise ValueError(f"Error combining dataframes: {e}")
    
    def __save_to_parquet(self, output_path: str):
        try:
            self.__raw_merged_data.to_parquet(output_path, index=False)
            print(f"Data saved to {output_path} in .parquet format.")
        except Exception as e:
            raise IOError(f"Error saving to Parquet: {e}")
    
    def __save_to_csv(self, output_path: str):
        try:
            self.__raw_merged_data.to_csv(output_path, index=False)
            print(f"Data saved to {output_path} in .csv format.")
        except Exception as e:
            raise IOError(f"Error saving to CSV: {e}")
        
    def get_merged_data(self):
        return self.__raw_merged_data