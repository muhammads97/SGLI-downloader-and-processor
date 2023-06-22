import numpy as np
import h5py
import pandas as pd
import os
from pathlib import Path
COLUMNS = ["MATCHUP_VALID", 'lat', 'lon', 'Rrs_380', 'Rrs_412', 'Rrs_443', 'Rrs_490', 'Rrs_530', 'Rrs_565', 'Rrs_672', 'AOD_380', 'AOD_412', 'AOD_443', 'AOD_490', 'AOD_530', 'AOD_565', 'AOD_672', 'adg_380', 'adg_412', 'adg_443', 'adg_490', 'adg_530', 'adg_565', 'adg_672', 'ap_380', 'ap_412', 'ap_443', 'ap_490', 'ap_530', 'ap_565', 'ap_672', 'aph_380', 'aph_412', 'aph_443', 'aph_490', 'aph_530', 'aph_565', 'aph_672', 'bbp_380', 'bbp_412', 'bbp_443', 'bbp_490', 'bbp_530', 'bbp_565', 'bbp_672', 'bp_380', 'bp_412', 'bp_443', 'bp_490', 'bp_530', 'bp_565', 'bp_672', 'Chla_oci', 'Chla_yoc', 'TSM_yoc', 'l2_flags', 'in_situ_lat', 'in_situ_lon', 'Global_ID', 'Date', 'l1b_gportal_id']
class OCSMARTExtractor:
    def __init__(self, input_csv, output_csv):
        self.out_file = Path(output_csv)
        self.input_csv = Path(input_csv)

    def get_h5_file(self, path):
        print("reading file: %s" % path)
        f = h5py.File(path, "r")
        return f

    def get_lat_lon_data(self, h5):
        lat = h5["Latitude"][:]
        lon = h5["Longitude"][:]
        return lat, lon

    def distance_sqr(self, lat, df_lat, lon, df_lon):
        lat_diff = (df_lat - lat)
        lon_diff = (df_lon - lon)
        return lat_diff * lat_diff + lon_diff * lon_diff

    def find_entry(self, lat_mat, lon_mat, lat, lon):
        dist = self.distance_sqr(lat, lat_mat, lon, lon_mat)
        row, col = np.where(dist == np.min(dist))
        return row, col, dist[row, col]
    
    def get_product(self, row, column, h5, product_name, group=None):
        if group == None:
            p = h5[product_name][:]
        else:
            p = h5[group][product_name][:]
        return p[row, column]
    
    def validate_p(self, value):
        return value == 0
    
    def validate_matchup(self, row, column, h5file):
        print(" r, c", row, column)
        flgs = h5file["L2_flags"][:][row-1: row+2, column-1:column+2]
        vld = []
        for i in range(3):
            vld.append([])
            for j in range(3):
                vld[i].append(self.validate_p(flgs[i, j]))
        vld = np.array(vld,dtype=np.bool)
        if vld[1, 1] == False: return False
        n_vld = np.sum(vld)
        if n_vld < 5: return False
        rrs_443 = h5file["Rrs"]["Rrs_443nm"][:][row-1: row+2, column-1:column+2]
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
            h5_path = "/home/shared/Data/SGLI/ocsmart_processed/" + vnr_id + "_ocsmart_" + str(row["Global_ID"]) + ".h5"
            try:
                h5 = self.get_h5_file(h5_path)
            except:
                continue
            lat_mat, lon_mat = self.get_lat_lon_data(h5)
            r, c, d = self.find_entry(lat_mat, lon_mat, row["lat"], row["lon"])
            r = r[0]
            c = c[0]
            print("found entry at %d and %d with dist = %f"%(r, c, d))
            try:
                is_valid = self.validate_matchup(r, c, h5)
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
                "Rrs_380": self.get_product(r, c, h5, "Rrs_380nm", "Rrs"),
                "Rrs_412": self.get_product(r, c, h5, "Rrs_412nm", "Rrs"),
                "Rrs_443": self.get_product(r, c, h5, "Rrs_443nm", "Rrs"),
                "Rrs_490": self.get_product(r, c, h5, "Rrs_490nm", "Rrs"),
                "Rrs_530": self.get_product(r, c, h5, "Rrs_530nm", "Rrs"),
                "Rrs_565": self.get_product(r, c, h5, "Rrs_565nm", "Rrs"),
                "Rrs_672": self.get_product(r, c, h5, "Rrs_672nm", "Rrs"),
                "AOD_380": self.get_product(r, c, h5, "AOD_380nm", "AOD"),
                "AOD_412": self.get_product(r, c, h5, "AOD_412nm", "AOD"),
                "AOD_443": self.get_product(r, c, h5, "AOD_443nm", "AOD"),
                "AOD_490": self.get_product(r, c, h5, "AOD_490nm", "AOD"),
                "AOD_530": self.get_product(r, c, h5, "AOD_530nm", "AOD"),
                "AOD_565": self.get_product(r, c, h5, "AOD_565nm", "AOD"),
                "AOD_672": self.get_product(r, c, h5, "AOD_672nm", "AOD"),
                "adg_380": self.get_product(r, c, h5, "adg_380nm", "adg"),
                "adg_412": self.get_product(r, c, h5, "adg_412nm", "adg"),
                "adg_443": self.get_product(r, c, h5, "adg_443nm", "adg"),
                "adg_490": self.get_product(r, c, h5, "adg_490nm", "adg"),
                "adg_530": self.get_product(r, c, h5, "adg_530nm", "adg"),
                "adg_565": self.get_product(r, c, h5, "adg_565nm", "adg"),
                "adg_672": self.get_product(r, c, h5, "adg_672nm", "adg"),
                "ap_380": self.get_product(r, c, h5, "ap_380nm", "ap"),
                "ap_412": self.get_product(r, c, h5, "ap_412nm", "ap"),
                "ap_443": self.get_product(r, c, h5, "ap_443nm", "ap"),
                "ap_490": self.get_product(r, c, h5, "ap_490nm", "ap"),
                "ap_530": self.get_product(r, c, h5, "ap_530nm", "ap"),
                "ap_565": self.get_product(r, c, h5, "ap_565nm", "ap"),
                "ap_672": self.get_product(r, c, h5, "ap_672nm", "ap"),
                "aph_380": self.get_product(r, c, h5, "aph_380nm", "aph"),
                "aph_412": self.get_product(r, c, h5, "aph_412nm", "aph"),
                "aph_443": self.get_product(r, c, h5, "aph_443nm", "aph"),
                "aph_490": self.get_product(r, c, h5, "aph_490nm", "aph"),
                "aph_530": self.get_product(r, c, h5, "aph_530nm", "aph"),
                "aph_565": self.get_product(r, c, h5, "aph_565nm", "aph"),
                "aph_672": self.get_product(r, c, h5, "aph_672nm", "aph"),
                "bbp_380": self.get_product(r, c, h5, "bbp_380nm", "bbp"),
                "bbp_412": self.get_product(r, c, h5, "bbp_412nm", "bbp"),
                "bbp_443": self.get_product(r, c, h5, "bbp_443nm", "bbp"),
                "bbp_490": self.get_product(r, c, h5, "bbp_490nm", "bbp"),
                "bbp_530": self.get_product(r, c, h5, "bbp_530nm", "bbp"),
                "bbp_565": self.get_product(r, c, h5, "bbp_565nm", "bbp"),
                "bbp_672": self.get_product(r, c, h5, "bbp_672nm", "bbp"),
                "bp_380": self.get_product(r, c, h5, "bp_380nm", "bp"),
                "bp_412": self.get_product(r, c, h5, "bp_412nm", "bp"),
                "bp_443": self.get_product(r, c, h5, "bp_443nm", "bp"),
                "bp_490": self.get_product(r, c, h5, "bp_490nm", "bp"),
                "bp_530": self.get_product(r, c, h5, "bp_530nm", "bp"),
                "bp_565": self.get_product(r, c, h5, "bp_565nm", "bp"),
                "bp_672": self.get_product(r, c, h5, "bp_672nm", "bp"),
                "Chla_oci": self.get_product(r, c, h5, "chlor_a(oci)"),
                "Chla_yoc": self.get_product(r, c, h5, "chlor_a(yoc)"),
                "TSM_yoc": self.get_product(r, c, h5, "tsm(yoc)"),
                "l2_flags": self.get_product(r, c, h5, "L2_flags"),
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

# in_csv = ["/home/muhammad/process_sgli/output_train.csv", "/home/muhammad/process_sgli/output_test.csv", "/home/muhammad/process_sgli/output_no_rrs.csv"]
# out_csv = ["/home/muhammad/process_sgli/output_ocsmart_train.csv", "/home/muhammad/process_sgli/output_ocsmart_test.csv","/home/muhammad/process_sgli/output_ocsmart_no_rrs.csv"]
# for i in range(3):
#     print("==========================###========================")
#     p = Process(in_csv[i], out_csv[i])
#     p.start()