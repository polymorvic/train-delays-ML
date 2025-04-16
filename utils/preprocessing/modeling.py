import pandas as pd
import numpy as np
import torch
from torch_geometric.data import Data, InMemoryDataset
from pathlib import Path
from .assembly import DataAssembler
from .const import (MODELING_READY_COLNAMES_BLACKLIST, CATEGORY_COLUMNS, SOURCE_TARGETS_COL_ARRIVAL, 
                    SOURCE_TARGETS_COL_DEPARTURE, NODE_FEATS, EDGE_FEATS, ML_TARGET_REG_NAME, ML_TARGET_CLASS_NAME)
from sklearn.preprocessing import LabelEncoder

class GNNPreprocessor(DataAssembler):
    ML_TARGET_CLASSIFICATION_COLNAME: str = 'ml_target_class'
    ML_TARGET_REGRESSION_COLNAME: str = 'ml_target_reg'

    def __init__(self, filepath: str = None):
        if not filepath:
            filepath = Path(self.OUTPUT_DATA_DIR) / f"{self.READY_FOR_MODELING_FILENAME}_{self.TIMESTAMP}.parquet"

        try:
            self.df = pd.read_parquet(filepath)
            self.encoder = LabelEncoder()
        except Exception as e:
            print(f'Something went wrong reading the file: {e}')

    def model_data(self) -> None:
        self.__remove_cols_fillna()
        self.__category_mapping()
        self.__make_ml_targets()

    def __remove_cols_fillna(self) -> None:
        self.df = self.df\
            .drop(MODELING_READY_COLNAMES_BLACKLIST + [SOURCE_TARGETS_COL_DEPARTURE], axis=1)\
            .drop_duplicates()\
            .fillna(-1)

    def __category_mapping(self) -> None:
        for i, col in enumerate(CATEGORY_COLUMNS):
            self.df[col] = self.df[col].astype(str)
            mask = self.df[col] == '-1'

            if (~mask).any():
                encoded = pd.Series(-1, index=self.df.index)
                encoded[~mask] = self.encoder.fit_transform(self.df.loc[~mask, col])
                self.df.insert(loc=i, column=f'{col}_id', value=encoded.astype(int))

        self.df.drop(columns=CATEGORY_COLUMNS, inplace=True)

    def __make_ml_targets(self) -> None:
        bins = [-np.inf, 5, 20, 60, np.inf]
        labels = [0, 1, 2, 3]
        self.df[self.ML_TARGET_CLASSIFICATION_COLNAME] = pd.cut(self.df[SOURCE_TARGETS_COL_ARRIVAL], bins=bins, labels=labels).astype(int)

        self.df.rename(columns={SOURCE_TARGETS_COL_ARRIVAL: self.ML_TARGET_REGRESSION_COLNAME}, inplace=True)
        delay_col = self.df.pop(self.ML_TARGET_REGRESSION_COLNAME)
        self.df[self.ML_TARGET_REGRESSION_COLNAME] = delay_col

class RailwayDataset(GNNPreprocessor, InMemoryDataset):
    ROUTE_UNIQUE_ID: str = 'id'

    def __init__(self, filepath: str = None, task="regression", transform=None):
        GNNPreprocessor.__init__(self, filepath)
        self.model_data()
        self.task = task
        InMemoryDataset.__init__(self, None, transform)
        self.data_list = self._process_graphs()
        self.data, self.slices = self.collate(self.data_list)

    def _process_graphs(self):
        df = self.df
        data_list = []

        for route_id in df[self.ROUTE_UNIQUE_ID].unique():
            route_df = df[df[self.ROUTE_UNIQUE_ID] == route_id].reset_index(drop=True)

            if len(route_df) < 2:
                continue

            x = torch.tensor(route_df[NODE_FEATS].astype(float).values, dtype=torch.float)

            edge_index = torch.tensor([
                list(range(len(route_df) - 1)),
                list(range(1, len(route_df)))
            ], dtype=torch.long)

            edge_attr = torch.tensor(route_df[EDGE_FEATS].iloc[1:].astype(float).values, dtype=torch.float)

            if self.task == "regression":
                y = torch.tensor(route_df[ML_TARGET_REG_NAME].values, dtype=torch.float)
            elif self.task == "classification":
                y = torch.tensor(route_df[ML_TARGET_CLASS_NAME].values, dtype=torch.long)
            else:
                raise ValueError("Unsupported task type. Choose 'regression' or 'classification'.")

            data = Data(
                x=x,
                edge_index=edge_index,
                edge_attr=edge_attr,
                y=y
            )
            data_list.append(data)

        return data_list

