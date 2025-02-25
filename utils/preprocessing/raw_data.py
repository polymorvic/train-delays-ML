import pickle, os

class TrainDelaysRawDataFileLoader:
    raw_data_dir = 'data/raw'

    def __init__(self, filename: str) -> None:
        self.filename = filename
        self.filepath = f'{self.raw_data_dir}/{self.filename}'

        if not os.path.isfile(self.filepath):
            raise FileNotFoundError(f"File '{self.filepath}' not found.")
        
        if not os.path.isdir(os.path.dirname(self.filepath)):
            raise NotADirectoryError(f"Directory '{os.path.dirname(self.filepath)}' does not exist.")
        
        if self.filename.endswith(('.pickle', '.pkl')):
            self.__load_pickle()
        else:
            raise ValueError(f"Only pickle extension is supported - you provided: {self.filename.split('.')[-1]}")

    def __load_pickle(self):
        try:
            with open(self.filepath, 'rb') as file:
                self.__loaded_data = pickle.load(file)
        except Exception as e:
            raise ValueError(f"Error loading pickle file: {e}")
        
    def get_data(self):
        return self.__loaded_data
    

# TrainDelaysRawDataHandler
