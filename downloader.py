############################
# Code developed by SALEM IBRAHIM SALEM
# Start time February 02, 2022
# Bismillah
###########################
import os
import numpy as np
import pandas as pd
from pathlib import Path
from download import Download


"""
An HDF5 file contains datasets & groups
	- datasets, which are array-like collections of data, 
	- groups, which are folder-like containers that hold datasets and other groups.
	--- Attribute: to store metadata in HDF5.
"""


class Downloader:
    def __init__(self, csv_path, path, token, account, password):
        self.path = Path(path)
        self.in_file = Path(csv_path)
        self.token = token
        self.account = account
        self.password = password

    def download_l2(self, from_index=0):
        in_df = pd.read_csv(self.in_file)
        downloader = Download(self.token, self.account, self.password)
        for i in range(from_index, len(in_df)):
            print("downloading row # %d" % i)
            row = in_df.iloc[i]
            rrs_link = str(row["l2_rrs_gportal_link"])
            prod_link = str(row["l2_prod_gportal_link"])
            if rrs_link == 'nan' or prod_link == 'nan':
                continue
            try:
                rrs_name = rrs_link.split("/")[-1]
                prod_name = prod_link.split("/")[-1]
                downloader.get(rrs_link, os.path.join(self.path, rrs_name), 20)
                downloader.get(prod_link, os.path.join(
                    self.path, prod_name), 20)
            except:
                print("cannot download!")
                continue
            print("download complete for row # %d" % i)

    def download_l1B(self, from_index=0):
        in_df = pd.read_csv(self.in_file)
        downloader = Download(self.token, self.account, self.password)
        for i in range(from_index, len(in_df)):
            print("downloading row # %d" % i)
            row = in_df.iloc[i]
            vnrdq_link = str(row["l1b_gportal_link"])
            if vnrdq_link == 'nan':
                continue
            try:
                vnrdq_name = vnrdq_link.split("/")[-1]
                irsdq_link = vnrdq_link.replace("VNRDQ", "IRSDQ")
                irsdq_name = irsdq_link.split("/")[-1]
                print("VNRDQ")
                vnrdq_path = os.path.join(self.path, vnrdq_name)
                if not os.path.exists(vnrdq_path):
                    downloader.get(vnrdq_link, vnrdq_path, 10)
                else:
                    print("already exist")
                print("IRSDQ")
                irsdq_path = os.path.join(self.path, irsdq_name)
                if not os.path.exists(irsdq_path):
                    downloader.get(irsdq_link, irsdq_path, 10)
                else:
                    print("already exist")
            except:
                print("cannot download!")
                continue
            print("download complete for row # %d" % i)
