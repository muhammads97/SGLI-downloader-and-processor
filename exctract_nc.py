import numpy as np
import netCDF4 as nc
import pandas as pd
import os
from pathlib import Path
FLAGS = ['ATMFAIL', 'LAND', 'PRODWARN', 'HIGLINT', 'HILT', 'HISATZEN', 'COASTZ', 'SPARE', 'STRAYLIGHT', 'CLDICE', 'COCCOLITH', 'TURBIDW', 'HISOLZEN', 'SPARE', 'LOWLW', 'CHLFAIL', 'NAVWARN', 'ABSAER', 'SPARE', 'MAXAERITER', 'MODGLINT', 'CHLWARN', 'ATMWARN', 'SPARE', 'SEAICE', 'NAVFAIL', 'FILTER', 'SPARE', 'BOWTIEDEL', 'HIPOL', 'PRODFAIL', 'SPARE']
INVALID = ["ATMFAIL", "LAND", "HIGLINT", "HILT", "STRAYLIGHT", "CLDICE", "LOWLW", "NAVFAIL", "NAVWARN"]
COLUMNS = ['MATCHUP_VALID','lat', 'lon', 'Kd_490', 'Rrs_380', 'Rrs_412', 'Rrs_443', 'Rrs_490', 'Rrs_529', 'Rrs_566', 'Rrs_672', 'a_380_qaa', 'a_412_qaa', 'a_443_qaa', 'a_490_qaa', 'a_566_qaa', 'a_672_qaa', 'adg_380_qaa', 'adg_412_qaa', 'adg_443_qaa', 'adg_490_qaa', 'adg_529_qaa', 'adg_566_qaa', 'adg_672_qaa', 'angstrom', 'aot_867', 'aph_380_qaa', 'aph_412_qaa', 'aph_443_qaa', 'aph_490_qaa', 'aph_529_qaa', 'aph_566_qaa', 'aph_672_qaa', 'chlor_a', 'l2_flags', 'in_situ_lat', 'in_situ_lon', 'Global_ID', 'Date', 'l1b_gportal_id']
class ExtractorSeaDAS:
    def __init__(self, input_csv, output_csv):
        self.out_file = Path(output_csv)
        self.input_csv = Path(input_csv)

    def get_nc_file(self, path):
        print("reading file: %s" % path)
        ds = nc.Dataset(path)
        return ds

    def get_lat_lon_data(self, nc_ds):
        lat = nc_ds["navigation_data"]["latitude"][:].data
        lon = nc_ds["navigation_data"]["longitude"][:].data
        return lat, lon

    def distance_sqr(self, lat, df_lat, lon, df_lon):
        lat_diff = (df_lat - lat)
        lon_diff = (df_lon - lon)
        return lat_diff * lat_diff + lon_diff * lon_diff

    def find_entry(self, lat_mat, lon_mat, lat, lon):
        dist = self.distance_sqr(lat, lat_mat, lon, lon_mat)
        row, col = np.where(dist == np.min(dist))
        return row, col, dist[row, col]
    
    def get_product(self, row, column, nc_ds, product_name):
        p = nc_ds["geophysical_data"][product_name][:]
        if product_name == "l2_flags":
            return p.data[row, column]
        if p.mask[row, column]:
            return -1
        else:
            return p.data[row, column]
        
    def get_p_flags(self, value):
        f = []
        order = 0
        while value > 0:
            rem = value % 2
            value //= 2
            if rem:
                f.append(FLAGS[order])
            order += 1
        return f
    
    def validate_p(self, value):
        f = self.get_p_flags(value)
        return np.intersect1d(f, INVALID).shape[0] == 0
        
    def validate_matchup(self, row, column, nc_ds):
        print(" r, c", row, column)
        flgs = nc_ds["geophysical_data"].variables["l2_flags"][:].data[row-1: row+2, column-1:column+2]
        vld = []
        for i in range(3):
            vld.append([])
            for j in range(3):
                vld[i].append(self.validate_p(flgs[i, j]))
        vld = np.array(vld,dtype=np.bool)
        if vld[1, 1] == False: return False
        n_vld = np.sum(vld)
        if n_vld < 5: return False
        rrs_443 = nc_ds["geophysical_data"].variables["Rrs_443"][:].data[row-1: row+2, column-1:column+2]
        cv = rrs_443.std()/rrs_443.mean()
        if cv > 0.15: return False
        return True



    def start(self, start_index=0):
        in_df = pd.read_csv(self.input_csv)
        print(len(in_df))
        out_df = pd.DataFrame(columns=COLUMNS)
        n = 0
        vld = 0
        invld = 0
        for i in range(start_index, in_df.shape[0]):
            row = in_df.iloc[i]
            print("processing file #: ", i)
            if type(row["l1b_gportal_id"]) != str:
                continue
            vnr_id = row["l1b_gportal_id"]
            nc_path = "/home/shared/Data/SGLI/seadas_processed_large_with_land/"+vnr_id[0:25] + "_" + str(row["Global_ID"]) + ".nc"
            try:
                nc_ds = self.get_nc_file(nc_path)
            except:
                continue
            lat_mat, lon_mat = self.get_lat_lon_data(nc_ds)
            r, c, d = self.find_entry(lat_mat, lon_mat, row["lat"], row["lon"])
            #check matchup valid 5x5 flags
            r = r[0]
            c = c[0]
            print("found entry at %d and %d with dist = %f"%(r, c, d))
            try:
                is_valid = self.validate_matchup(r, c, nc_ds)
            except:
                is_valid = False
            if not is_valid:
                print("invalid matchup")
                invld += 1
            else:
                print("VALID!")
                vld += 1
            l2 = {
                "lat": lat_mat[r, c],
                "lon": lon_mat[r, c],
                "Kd_490": self.get_product(r, c, nc_ds, "Kd_490"),
                "Rrs_380": self.get_product(r, c, nc_ds, "Rrs_380"),
                "Rrs_412": self.get_product(r, c, nc_ds, "Rrs_412"),
                "Rrs_443": self.get_product(r, c, nc_ds, "Rrs_443"),
                "Rrs_490": self.get_product(r, c, nc_ds, "Rrs_490"),
                "Rrs_529": self.get_product(r, c, nc_ds, "Rrs_529"),
                "Rrs_566": self.get_product(r, c, nc_ds, "Rrs_566"),
                "Rrs_672": self.get_product(r, c, nc_ds, "Rrs_672"),
                "a_380_qaa": self.get_product(r, c, nc_ds, "a_380_qaa"),
                "a_412_qaa": self.get_product(r, c, nc_ds, "a_412_qaa"),
                "a_443_qaa": self.get_product(r, c, nc_ds, "a_443_qaa"),
                "a_490_qaa": self.get_product(r, c, nc_ds, "a_529_qaa"),
                "a_566_qaa": self.get_product(r, c, nc_ds, "a_566_qaa"),
                "a_672_qaa": self.get_product(r, c, nc_ds, "a_672_qaa"),
                "adg_380_qaa": self.get_product(r, c, nc_ds, "adg_380_qaa"),
                "adg_412_qaa": self.get_product(r, c, nc_ds, "adg_412_qaa"),
                "adg_443_qaa": self.get_product(r, c, nc_ds, "adg_443_qaa"),
                "adg_490_qaa": self.get_product(r, c, nc_ds, "adg_490_qaa"),
                "adg_529_qaa": self.get_product(r, c, nc_ds, "adg_529_qaa"),
                "adg_566_qaa": self.get_product(r, c, nc_ds, "adg_566_qaa"),
                "adg_672_qaa": self.get_product(r, c, nc_ds, "adg_672_qaa"),
                "angstrom": self.get_product(r, c, nc_ds, "angstrom"),
                "aot_867": self.get_product(r, c, nc_ds, "aot_867"),
                "aph_380_qaa": self.get_product(r, c, nc_ds, "aph_380_qaa"),
                "aph_412_qaa": self.get_product(r, c, nc_ds, "aph_412_qaa"),
                "aph_443_qaa": self.get_product(r, c, nc_ds, "aph_443_qaa"),
                "aph_490_qaa": self.get_product(r, c, nc_ds, "aph_490_qaa"),
                "aph_529_qaa": self.get_product(r, c, nc_ds, "aph_529_qaa"),
                "aph_566_qaa": self.get_product(r, c, nc_ds, "aph_566_qaa"),
                "aph_672_qaa": self.get_product(r, c, nc_ds, "aph_672_qaa"),
                "chlor_a": self.get_product(r, c, nc_ds, "chlor_a"),
                "l2_flags": self.get_product(r, c, nc_ds, "l2_flags"),
                "in_situ_lat": row["lat"],
                "in_situ_lon": row["lon"],
                "Global_ID": row["Global_ID"],
                "Date": row["Date"],
                "l1b_gportal_id": row["l1b_gportal_id"],
                "MATCHUP_VALID": is_valid
            }
            
            out_df.loc[n] = l2
            n += 1
            if n % 10 == 0:
                out_df.to_csv(self.out_file)
        out_df.to_csv(self.out_file)
        print("VALID", vld, "INVALID", invld)

# in_csv = "/home/muhammad/process_sgli/output_no_rrs.csv"
# out_csv = "/home/muhammad/process_sgli/output_seadas_no_rrs.csv"
# p = Process(in_csv, out_csv)
# p.start()