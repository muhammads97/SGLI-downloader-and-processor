import pandas as pd
import numpy as np

class Collector:
    def __init__(self, files, columns, important, outfile):
        self.files = files
        self.columns = columns
        self.important = important
        self.outfile = outfile

    def filter_df(self, df):
        df = df[~df.isna().any(axis=1)].reset_index(drop=True)
        df = df[~(df == -1).any(axis = 1)].reset_index(drop=True)
        return df
    
    def convert_to_nan(self, df):
        df[df == -1] = np.nan
        return df
    
    def get_important(self, df):
        return df[self.important]

    def start(self):
        df = pd.DataFrame(columns = self.columns)
        for f in self.files:
            f_df = pd.read_csv(f)
            df =pd.concat([df, f_df], axis=0)
        # df = self.filter_df(df[self.important])
        df = self.convert_to_nan(df)
        df = self.get_important(df)
        df.to_csv(self.outfile, index = False)