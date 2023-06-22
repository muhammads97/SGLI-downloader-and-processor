############################
# Code developed by SALEM IBRAHIM SALEM
# Start time February 02, 2022
# Bismillah
###########################
import os
import h5py
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
COLUMNS = [
            "Global_ID",
            "lat",
            "lon",
            "Date",
            "l1b_gportal_id",
            "l2_rrs_gportal_id",
            "l2_prod_gportal_id",
            "Rrs_380",
            "Rrs_412",
            "Rrs_443",
            "Rrs_490",
            "Rrs_530",
            "Rrs_565",
            "Rrs_670",
            "Rrs_765",
            "Rrs_flags",
            "prod_flags",
            "Chla",
            "TSM",
            "ag_443",
            "MATCHUP_VALID"
        ]

FLAGS = ['NODATA', 'LAND', 'ATMFAIL', 'CLDICE', 'CLDICEWARN', 'STRAYLIGHT', 'HIGLINT', 'MODGLINT', 'HISOLZEN', 'HIAIRSOLTHK', 'LOWLW', 'TURBIDW', 'SHALLOW', 'CDOMFAIL', 'CHLFAIL']
INVALID = ["ATMFAIL", "LAND", "HIGLINT", "STRAYLIGHT", "CLDICE", "NODATA"]
class SGLIL2Extractor:
    def __init__(self, input_csv, output_csv):
        self.out_file = Path(output_csv)
        self.in_file = Path(input_csv)

        
    def get_h5_file(self, path):
        print("reading file: %s" % path)
        f = h5py.File(path, 'r')
        return f

    def DN_to_Reflectance_L2(self, h5_file, prod_name):
        # Get Rrs data
        real_prod_name = prod_name.replace('Rrs', 'NWLR')
        dset = h5_file['Image_data/' + real_prod_name]

        # Validate
        data = dset[:].astype(np.float32)
        if 'Error_DN' in dset.attrs:
            data[data == dset.attrs['Error_DN'][0]] = np.NaN
        with np.warnings.catch_warnings():
            np.warnings.filterwarnings(
                'ignore', r'invalid value encountered in (greater|less)')
            if 'Maximum_valid_DN' in dset.attrs:
                data[data > dset.attrs['Maximum_valid_DN'][0]] = np.NaN
            if 'Minimum_valid_DN' in dset.attrs:
                data[data < dset.attrs['Minimum_valid_DN'][0]] = np.NaN

        # Convert DN to physical value
        Slope = dset.attrs['Rrs_slope'][0]
        Offset = dset.attrs['Rrs_offset'][0]
        data = data * Slope + Offset
        print("Band: ", prod_name, " >> Slope= ", Slope, " Offset= ", Offset)

        return data

    def get_product_data(self, h5_file, prod_name):
        dset = h5_file['Image_data/' + prod_name]

        # Return uint16 type data if the product is QA_flag or Line_tai93
        if 'QA_flag' == prod_name or 'Line_tai93' == prod_name:
            return dset[:]

        # Validate
        data = dset[:].astype(np.float32)
        if 'Error_DN' in dset.attrs:
            data[data == dset.attrs['Error_DN'][0]] = np.NaN
        with np.warnings.catch_warnings():
            np.warnings.filterwarnings(
                'ignore', r'invalid value encountered in (greater|less)')
            if 'Maximum_valid_DN' in dset.attrs:
                data[data > dset.attrs['Maximum_valid_DN'][0]] = np.NaN
            if 'Minimum_valid_DN' in dset.attrs:
                data[data < dset.attrs['Minimum_valid_DN'][0]] = np.NaN

        # Convert DN to physical value
        Slope = dset.attrs['Slope'][0]
        Offset = dset.attrs['Offset'][0]
        data = data * Slope + Offset
        print("Band: ", prod_name, " >> Slope= ", Slope, " Offset= ", Offset)

        return data

    # lon_mode is False if not given
    def bilin_2d(self, data: np.ndarray, interval: int, lon_mode=False):
        data = data.copy()

        if lon_mode is True:
            max_diff = np.nanmax(np.abs(data[:, :-1] - data[:, 1:]))
            if max_diff > 180.:
                data[data < 0] = 360. + data[data < 0]

        data = np.concatenate((data, data[-1].reshape(1, -1)), axis=0)
        data = np.concatenate((data, data[:, -1].reshape(-1, 1)), axis=1)

        ratio_horizontal = np.tile(np.linspace(0, (interval - 1) / interval, interval, dtype=np.float32),
                                   (data.shape[0] * interval, data.shape[1] - 1))
        ratio_vertical = np.tile(np.linspace(0, (interval - 1) / interval, interval, dtype=np.float32).reshape(-1, 1),
                                 (data.shape[0] - 1, (data.shape[1] - 1) * interval))
        repeat_data = np.repeat(data, interval, axis=0)
        repeat_data = np.repeat(repeat_data, interval, axis=1)

        horizontal_interp = (1. - ratio_horizontal) * \
            repeat_data[:, :-interval] + \
            ratio_horizontal * repeat_data[:, interval:]
        ret = (1. - ratio_vertical) * \
            horizontal_interp[:-interval, :] + \
            ratio_vertical * horizontal_interp[interval:, :]

        if lon_mode is True:
            ret[ret > 180.] = ret[ret > 180.] - 360.

        return ret

    def get_geometry_data(self, h5_file, data_name):

        dset = h5_file['Geometry_data/' + data_name]
        data = dset[:]
        if 'Latitude' != data_name and 'Longitude' != data_name:
            data = dset[:].astype(np.float32)
            if 'Error_DN' in dset.attrs:
                data[data == dset.attrs['Error_DN'][0]] = np.NaN
            with np.warnings.catch_warnings():
                np.warnings.filterwarnings(
                    'ignore', r'invalid value encountered in (greater|less)')
                if 'Maximum_valid_DN' in dset.attrs:
                    data[data > dset.attrs['Maximum_valid_DN'][0]] = np.NaN
                if 'Minimum_valid_DN' in dset.attrs:
                    data[data < dset.attrs['Minimum_valid_DN'][0]] = np.NaN

            data = data * dset.attrs['Slope'][0] + dset.attrs['Offset'][0]

        # Interpolate raw data
        # e.g., 10   for latitude & longitude
        interp_interval = dset.attrs['Resampling_interval'][0]

        lon_mode = False
        if 'Longitude' == data_name:
            lon_mode = True
        if interp_interval > 1:
            data = self.bilin_2d(data, interp_interval, lon_mode)

        # Trim away the excess pixel/line
        (data_size_lin, data_size_pxl) = data.shape

        img_attrs = h5_file['Image_data'].attrs
        img_n_pix = img_attrs['Number_of_pixels'][0]   # e.g., 5000
        img_n_lin = img_attrs['Number_of_lines'][0]    # e.g., 7820

        if (img_n_lin <= data_size_lin) and (img_n_pix <= data_size_pxl):
            data = data[:img_n_lin, :img_n_pix]

        return data

    def HH_to_HHMMSS(self, data_HH):
        HH = data_HH.astype(int)
        MM = ((data_HH*60) % 60).astype(int)
        SS = ((data_HH*3600) % 60).astype(int)
        return (HH, MM, SS)

    def distance_sqr(self, lat, df_lat, lon, df_lon):
        lat_diff = (df_lat - lat)
        lon_diff = (df_lon - lon)
        return lat_diff * lat_diff + lon_diff * lon_diff

    def find_entry(self, lat_mat, lon_mat, lat, lon):
        dist = self.distance_sqr(lat, lat_mat, lon, lon_mat)
        row, col = np.where(dist == np.min(dist))
        return row, col, dist[row, col]
    
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
    
    def validate_matchup(self, row, column, h5_rrs):
        print(" r, c", row, column)
        flgs = h5_rrs["Image_data"]["QA_flag"][:][row-1: row+2, column-1:column+2]
        vld = []
        for i in range(3):
            vld.append([])
            for j in range(3):
                vld[i].append(self.validate_p(flgs[i, j]))
        vld = np.array(vld,dtype=np.bool)
        if vld[1, 1] == False: return False
        n_vld = np.sum(vld)
        if n_vld < 5: return False
        rrs_443 = self.DN_to_Reflectance_L2(h5_rrs, 'Rrs_443')[row-1: row+2, column-1:column+2]
        cv = rrs_443.std()/rrs_443.mean()
        if cv > 0.15: return False
        return True

    def get_p_data(self, row, rrs_h5, prod_h5):
        lat = row['lat']
        lon = row['lon']
        lat_mat = self.get_geometry_data(rrs_h5, "Latitude")
        lon_mat = self.get_geometry_data(rrs_h5, "Longitude")
        print("finding entry ..")
        r, c, d = self.find_entry(lat_mat, lon_mat, lat, lon)
        r = r[0]
        c = c[0]
        print("found matching point at %d, %d with distance^2 = %f" % (r, c, d))
        try:
            is_valid = self.validate_matchup(r, c, rrs_h5)
        except:
            is_valid = False

        print("Valid: ", is_valid)
        return {
            "Global_ID": row["Global_ID"],
            "lat": lat,
            "lon": lon,
            "Date": row["Date"],
            "l1b_gportal_id": row["l1b_gportal_id"],
            "l2_rrs_gportal_id": row["l2_rrs_gportal_id"],
            "l2_prod_gportal_id": row["l2_prod_gportal_id"],
            "Rrs_380": self.DN_to_Reflectance_L2(rrs_h5, "Rrs_380")[r, c],
            "Rrs_412": self.DN_to_Reflectance_L2(rrs_h5, "Rrs_412")[r, c],
            "Rrs_443": self.DN_to_Reflectance_L2(rrs_h5, "Rrs_443")[r, c],
            "Rrs_490": self.DN_to_Reflectance_L2(rrs_h5, "Rrs_490")[r, c],
            "Rrs_530": self.DN_to_Reflectance_L2(rrs_h5, "Rrs_530")[r, c],
            "Rrs_565": self.DN_to_Reflectance_L2(rrs_h5, "Rrs_565")[r, c],
            "Rrs_670": self.DN_to_Reflectance_L2(rrs_h5, "Rrs_670")[r, c],
            "Rrs_765": np.nan,
            "Rrs_flags": rrs_h5["Image_data"]["QA_flag"][r, c],
            "prod_flags": prod_h5["Image_data"]["QA_flag"][r, c],
            "Chla": self.get_product_data(prod_h5, 'CHLA')[r, c],
            "TSM": self.get_product_data(prod_h5, 'TSM'),
            "ag_443": self.get_product_data(prod_h5, 'CDOM'),
            "MATCHUP_VALID": is_valid
        }, is_valid

    def start(self, from_index=0):
        in_df = pd.read_csv(self.in_file)
        print(len(in_df))
        out_df = pd.DataFrame(columns=COLUMNS)
        n = 0
        vld = 0
        invld = 0
        for i in range(from_index, len(in_df)):
            print("processing row # %d" % i)
            row = in_df.iloc[i]
            rrs_id = str(row["l2_rrs_gportal_id"])
            prod_id = str(row["l2_prod_gportal_id"])
            if rrs_id == 'nan' or prod_id == 'nan':
                continue
            rrs_h5_path = "/home/shared/Data/SGLI/sgli-lvl2/" + rrs_id + ".h5"
            prod_h5_path = "/home/shared/Data/SGLI/sgli-lvl2/" + prod_id + ".h5"
            try:
                rrs_h5 = self.get_h5_file(rrs_h5_path)
                prod_h5 = self.get_h5_file(prod_h5_path)
            except:
                continue
            pixel, is_valid = self.get_p_data(row, rrs_h5, prod_h5)
            if is_valid:
                vld += 1
            else:
                invld += 1

            out_df.loc[n] = pixel
            n += 1
            if n % 10 == 0:
                out_df.to_csv(self.out_file)
        out_df.to_csv(self.out_file)
        print("VALID", vld, "INVALID", invld)
