import os
import shutil
import pandas as pd


"""
An HDF5 file contains datasets & groups
	- datasets, which are array-like collections of data, 
	- groups, which are folder-like containers that hold datasets and other groups.
	--- Attribute: to store metadata in HDF5.
"""


class OCSMARTProcessor:
    def __init__(self, l1b_path, l2_path, ocsmart_path, temp_l1b_path, temp_geo_path, input_csv, start_index = 0):
        self.l1b_path = l1b_path
        self.l2_path = l2_path
        self.ocsmart_path = ocsmart_path
        self.temp_l1b_path = temp_l1b_path
        self.temp_geo_path = temp_geo_path
        self.index = start_index
        self.input_csv = input_csv

    def get_list_of_files(self):
        in_df = pd.read_csv(self.input_csv)
        files = []
        for i in range(self.index, in_df.shape[0]):
            row = in_df.iloc[i]
            vnr_id = row["l1b_gportal_id"]
            vnr_file = os.path.join(self.l1b_path, vnr_id+".h5")
            irs_id = vnr_id.replace("VNRDQ", "IRSDQ")
            irs_file = os.path.join(self.l1b_path, irs_id+".h5")
            lat = row["lat"]
            lon = row["lon"]
            if not os.path.exists(irs_file) or not os.path.exists(vnr_file):
                continue
            files.append({
                "l1b_path": vnr_file,
                "l1b": vnr_id + ".h5",
                "geo_path": irs_file,
                "geo": irs_id + ".h5",
                "lat": lat,
                "lon": lon
            })
        return files   

    def write_config(self):
        current = self.files[self.index]
        config_file = os.path.join(self.ocsmart_path, "OCSMART_Input.txt")
        l1b_path = "l1b_path = /home/muhammad/process_sgli/l1b/\n"
        geo_path = "l1b_path = /home/muhammad/process_sgli/l1b/\n"
        l2_path = "l2_path = /home/shared/Data/SGLI/ocsmart_processed/\n"
        sl_l = "solz_limit = 70.0\n"
        sn_l = "senz_limit = 70.0\n"
        lat_center = "latitude_center=%f\n"%current["lat"]
        lon_center = "longitude_center=%f\n"%current["lon"]
        box_w = "box_width=101\n"
        box_h = "box_height=101\n"
        with open(config_file, "w") as f:
            f.write(l1b_path)
            f.write(geo_path)
            f.write(l2_path)
            f.write(sl_l)
            f.write(sn_l)
            f.write(lat_center)
            f.write(lon_center)
            f.write(box_w)
            f.write(box_h)

    
    def prepare_current(self):
        current = self.files[self.index]
        try:
            shutil.copy(current["l1b_path"], os.path.join(self.temp_l1b_path, current["l1b"]))
            shutil.copy(current["geo_path"], os.path.join(self.temp_geo_path, current["geo"]))
            self.write_config()
            return True
        except:
            self.clear_temp()
            return False

    def clear_temp(self):
        os.system("rm -r %s/*"%self.temp_l1b_path)
        os.system("rm -r %s/*"%self.temp_geo_path)

    def next(self):
        self.index = self.index + 1
        if self.index < len(self.files):
            return True
        else:
            return False

    def start(self):
        self.get_list_of_files()
        self.clear_temp()
        while(True):
            print("processing # %d of %d"%(self.index, len(self.files)))
            if self.prepare_current():
                command = "python -u OCSMART.py"
                exit_status = os.system(command)
                if exit_status == 0:
                    print("processing was successful!!")
                else:
                    print("failed!")
                self.clear_temp()
            if not self.next():
                break

    
    
p = ProcessOCSMART("/home/shared/Data/SGLI/sgli-lvl1/", "/home/shared/Data/SGLI/ocsmart_processed/", "/home/shared/ocsmart/Python_Linux/", "/home/muhammad/process_sgli/l1b", "/home/muhammad/process_sgli/geo")
p.start()

    