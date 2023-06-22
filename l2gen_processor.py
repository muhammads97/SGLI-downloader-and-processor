import os
import h5py
import numpy as np
import pandas as pd
from pathlib import Path


"""
An HDF5 file contains datasets & groups
	- datasets, which are array-like collections of data, 
	- groups, which are folder-like containers that hold datasets and other groups.
	--- Attribute: to store metadata in HDF5.
"""


class L2genProcessor:
    def __init__(self, directory, input_csv, output_csv):
        self.out_file = Path(output_csv)
        self.out_file.touch(exist_ok=True)
        self.in_dir = Path(directory)
        self.input_csv = Path(input_csv)
        self.out_colums = [
            "Global_ID",
            "pixel_lat",
            "pixel_lon",
            "in_situ_lat",
            "in_situ_lon",
            "Date",
            "l1b_gportal_id",
            "Rrs_380",
            "Rrs_412",
            "Rrs_443",
            "Rrs_490",
            "Rrs_530",
            "Rrs_565",
            "Rrs_673",
            "Rrs_765",
            "l2_flags",
            "Chla",
            "TSM",
            "angstrom",
            "kd_490",
            "ag_443"
        ]

    def get_h5_file(self, path):
        print("reading file: %s" % path)
        f = h5py.File(path, 'r')
        # print("###############", "Main Groups:", list(f.keys()), sep="\n")
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

    def construct_df(self, h5_rrs, h5_prod):
        return pd.DataFrame({
                            'Rrs_380':  self.DN_to_Reflectance_L2(h5_rrs, 'Rrs_380').flatten(),
                            'Rrs_412':  self.DN_to_Reflectance_L2(h5_rrs, 'Rrs_412').flatten(),
                            'Rrs_443':  self.DN_to_Reflectance_L2(h5_rrs, 'Rrs_443').flatten(),
                            'Rrs_490':  self.DN_to_Reflectance_L2(h5_rrs, 'Rrs_490').flatten(),
                            'Rrs_530':  self.DN_to_Reflectance_L2(h5_rrs, 'Rrs_530').flatten(),
                            'Rrs_565':  self.DN_to_Reflectance_L2(h5_rrs, 'Rrs_565').flatten(),
                            'Rrs_670':  self.DN_to_Reflectance_L2(h5_rrs, 'Rrs_670').flatten(),

                            'Rrs_flags':  self.get_product_data(h5_rrs, 'QA_flag').flatten(),
                            'prod_flags':  self.get_product_data(h5_prod, 'QA_flag').flatten(),

                            'TSM':  self.get_product_data(h5_prod, 'TSM').flatten(),
                            'ag_443':  self.get_product_data(h5_prod, 'CDOM').flatten(),
                            'Chla':  self.get_product_data(h5_prod, 'CHLA').flatten(),

                            'lat':  self.get_geometry_data(h5_rrs, 'Latitude').flatten(),
                            'lon':  self.get_geometry_data(h5_rrs, 'Longitude').flatten(),
                            "time":  self.get_geometry_data(h5_rrs, 'Longitude').flatten(),
                            })

    def distance_sqr(self, lat, df_lat, lon, df_lon):
        lat_diff = (df_lat - lat)
        lon_diff = (df_lon - lon)
        return lat_diff * lat_diff + lon_diff * lon_diff

    def find_entry(self, lat_mat, lon_mat, lat, lon):
        dist = self.distance_sqr(lat, lat_mat, lon, lon_mat)
        row, col = np.where(dist == np.min(dist))
        return row, col, dist[row, col]

    def update_out_df(self, l1_row, l2_df, out_df):
        lat = l1_row['lat']
        lon = l1_row['lon']
        print("finding entry ..")
        l2_row, d = self.find_entry(l2_df, lat, lon)
        print("found matching point with distance^2 = %f" % d)
        return out_df.append({
            "index": l1_row["index"],
            "lat": lat,
            "lon": lon,
            "Date": l1_row["Date"],
            "l1b_gportal_id": l1_row["l1b_gportal_id"],
            "l1b_gportal_link": l1_row["l1b_gportal_link"],
            "l1b_gportal_size": l1_row["l1b_gportal_size"],
            "l2_rrs_gportal_id": l1_row["l2_rrs_gportal_id"],
            "l2_rrs_gportal_link": l1_row["l2_rrs_gportal_link"],
            "l2_rrs_gportal_size": l1_row["l2_rrs_gportal_size"],
            "l2_prod_gportal_id": l1_row["l2_prod_gportal_id"],
            "l2_prod_gportal_link": l1_row["l2_prod_gportal_link"],
            "l2_prod_gportal_size": l1_row["l2_prod_gportal_size"],
            "Rrs_380": l2_row["Rrs_380"],
            "Rrs_412": l2_row["Rrs_412"],
            "Rrs_443": l2_row["Rrs_443"],
            "Rrs_490": l2_row["Rrs_490"],
            "Rrs_530": l2_row["Rrs_530"],
            "Rrs_565": l2_row["Rrs_565"],
            "Rrs_670": l2_row["Rrs_670"],
            "Rrs_765": np.nan,
            "Rrs_flags": l2_row["Rrs_flags"],
            "prod_flags": l2_row["prod_flags"],
            "Chla": l2_row["Chla"],
            "TSM": l2_row["TSM"],
            "ag_443": l2_row["ag_443"]
        }, ignore_index=True)

    def start(self, start_index=0):
        in_df = pd.read_csv(self.input_csv)
        # out_df = pd.read_csv(self.out_file)
        out_df = pd.DataFrame(columns=self.out_colums)
        for i in range(start_index, in_df.shape[0]):
            row = in_df.iloc[i]
            out_row = {
                "Global_ID": row["Global_ID"],
                "in_situ_lat": row["lat"],
                "in_situ_lon": row["lon"],
                "Date": row["Date"],
                "l1b_gportal_id": row["l1b_gportal_id"],
            }
            if type(row["l1b_gportal_id"]) != str:
                continue
            print("processing file #: ", i)
            print("id: ", row["l1b_gportal_id"])
            vnr_id = row["l1b_gportal_id"]
            vnr_file = os.path.join(self.in_dir, vnr_id+".h5")
            irs_id = vnr_id.replace("VNRDQ", "IRSDQ")
            irs_file = os.path.join(self.in_dir, irs_id+".h5")
            if not os.path.exists(irs_file):
                continue
            try:
                irs = self.get_h5_file(irs_file)
            except:
                continue
            lat = irs["Geometry_data"]["Latitude"][:]
            lon = irs["Geometry_data"]["Longitude"][:]
            x, y, dist = self.find_entry(lat, lon, row["lat"], row["lon"])
            x2 = x * 10
            y2 = y * 10
            print("best match is at: %f, %f, with dist: %f, for lat: %f, lon: %f, found: %f, %f" % (
                x, y, dist, row["lat"], row["lon"], lat[x, y], lon[x, y]))
            print("GID: ", row["Global_ID"])
            ofile = "/home/shared/Data/SGLI/seadas_processed_large_with_land/"+vnr_id[0:25] + "_" + str(row["Global_ID"]) + ".nc"
            command = "l2gen ifile=%s l2prod=Kd_490,Rrs_vvv,a_vvv_qaa,adg_vvv_qaa,angstrom,aot_867,aph_vvv_qaa,chlor_a aer_opt=-10 iop_opt=3 ofile=%s spixl=%d epixl=%d sline=%d eline=%d proc_land=1" % (
                vnr_file, ofile, y2-1000, y2+1000, x2-1000, x2+1000)
            print("command : ", command)
            exit_status = os.system(command)
            print("processings done with exit code: ", exit_status)
        #     if exit_status != 0:
        #         continue
        #     os.system(
        #         "h4toh5convert /Users/salah/workspace/temp_l2gen_out.h4 /Users/salah/workspace/temp.h5")
        #     l2_h5 = self.get_h5_file(
        #         "/Users/salah/workspace/temp.h5")
        #     lat = l2_h5["Navigation Data"]["latitude"][:]
        #     lon = l2_h5["Navigation Data"]["longitude"][:]
        #     r, c, dist = self.find_entry(lat, lon, row["lat"], row["lon"])
        #     out_row["pixel_lat"] = lat[r, c][0]
        #     out_row["pixel_lon"] = lon[r, c][0]
        #     slope = l2_h5["Geophysical Data"]["Rrs_380"].attrs["slope"]
        #     out_row["Rrs_380"] = l2_h5["Geophysical Data"]["Rrs_380"][:][r, c][0] * slope
        #     slope = l2_h5["Geophysical Data"]["Rrs_412"].attrs["slope"]
        #     out_row["Rrs_412"] = l2_h5["Geophysical Data"]["Rrs_412"][:][r, c][0] * slope
        #     slope = l2_h5["Geophysical Data"]["Rrs_443"].attrs["slope"]
        #     out_row["Rrs_443"] = l2_h5["Geophysical Data"]["Rrs_443"][:][r, c][0] * slope
        #     slope = l2_h5["Geophysical Data"]["Rrs_490"].attrs["slope"]
        #     out_row["Rrs_490"] = l2_h5["Geophysical Data"]["Rrs_490"][:][r, c][0] * slope
        #     slope = l2_h5["Geophysical Data"]["Rrs_529"].attrs["slope"]
        #     out_row["Rrs_530"] = l2_h5["Geophysical Data"]["Rrs_529"][:][r, c][0] * slope
        #     slope = l2_h5["Geophysical Data"]["Rrs_566"].attrs["slope"]
        #     out_row["Rrs_565"] = l2_h5["Geophysical Data"]["Rrs_566"][:][r, c][0] * slope
        #     slope = l2_h5["Geophysical Data"]["Rrs_672"].attrs["slope"]
        #     out_row["Rrs_673"] = l2_h5["Geophysical Data"]["Rrs_672"][:][r, c][0] * slope
        #     out_row["l2_flags"] = l2_h5["Geophysical Data"]["l2_flags"][:][r, c][0]
        #     out_row["Chla"] = l2_h5["Geophysical Data"]["chlor_a"][:][r, c][0]
        #     slope = l2_h5["Geophysical Data"]["angstrom"].attrs["slope"]
        #     out_row["angstrom"] = l2_h5["Geophysical Data"]["angstrom"][:][r, c][0] * slope
        #     slope = l2_h5["Geophysical Data"]["Kd_490"].attrs["slope"]
        #     out_row["kd_490"] = l2_h5["Geophysical Data"]["Kd_490"][:][r, c][0] * slope
        #     out_df = out_df.append(out_row, ignore_index=True)
        #     os.system("rm /Users/salah/workspace/temp_l2gen_out.h4")
        #     os.system("rm /Users/salah/workspace/temp.h5")
        #     if i % 10 == 0:
        #         print("saving .. ")
        #         out_df.to_csv(self.out_file)
        # out_df.to_csv(self.out_file)
