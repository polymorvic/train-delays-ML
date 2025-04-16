import pandas as pd
import numpy as np
from pathlib import Path
from .assembly import DataAssembler
from .const import (MODELING_READY_COLNAMES_BLACKLIST, CATEGORY_COLUMNS, SOURCE_TARGETS_COL_ARRIVAL, 
                    SOURCE_TARGETS_COL_DEPARTURE)
from sklearn.preprocessing import LabelEncoder

class GNNPreprocessor(DataAssembler):
    ML_TARGET_CLASSIFICATION_COLNAME: str = 'ml_target_class'
    ML_TARGET_REGRESSION_COLNAME: str = 'ml_target_reg'
    
    def __init__(self, filepath: str = None):
        if not filepath:
            filepath = Path(self.OUTPUT_DATA_DIR) / f"{self.READY_FOR_MODELING_FILENAME}_{self.TIMESTAMP}.parquet"

        try:
            self.data = pd.read_parquet(filepath)
            self.encoder = LabelEncoder()
        except Exception as e:
            print(f'Something went wrong reading the file: {e}')

    def model_data(self) -> None:
        self.__remove_cols_fillna()
        self.__category_mapping()
        self.__make_ml_targets()

    def __remove_cols_fillna(self) -> None:
        self.data = self.data\
            .drop(MODELING_READY_COLNAMES_BLACKLIST + [SOURCE_TARGETS_COL_DEPARTURE], axis=1)\
            .drop_duplicates()\
            .fillna(-1)
        
    def __category_mapping(self) -> None:
        for i, col in enumerate(CATEGORY_COLUMNS):
            self.data[col] = self.data[col].astype(str)
            mask = self.data[col] == '-1'
            
            if (~mask).any():
                encoded = pd.Series(-1, index=self.data.index)
                encoded[~mask] = self.encoder.fit_transform(self.data.loc[~mask, col])
                self.data.insert(loc=i, column=f'{col}_id', value=encoded.astype(int))
                
        self.data.drop(columns=CATEGORY_COLUMNS, inplace=True)

    def __make_ml_targets(self) -> None:
        bins = [-np.inf, 5, 20, 60, np.inf]
        labels = [0, 1, 2, 3]
        self.data[self.ML_TARGET_CLASSIFICATION_COLNAME] = pd.cut(self.data[SOURCE_TARGETS_COL_ARRIVAL], bins=bins, labels=labels)

        self.data.rename(columns={SOURCE_TARGETS_COL_ARRIVAL: self.ML_TARGET_REGRESSION_COLNAME}, inplace=True)
        delay_col = self.data.pop(self.ML_TARGET_REGRESSION_COLNAME)
        self.data[self.ML_TARGET_REGRESSION_COLNAME] = delay_col

