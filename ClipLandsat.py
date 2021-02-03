from rasterio.plot import show
from bs4 import BeautifulSoup
import rioxarray as rxr
import geopandas as gpd
import rasterio as rio
from glob import glob
import pandas as pd
import numpy as np
import os, shutil
import requests
import shapely
import pyproj
import pygeos
import rtree

import shutil 

from shapely.geometry import box
from shapely.geometry import mapping

import fiona
import rasterio
import rasterio.mask

import argparse




def delfiles(directory):
	
	"""
			Delete all of the Landsat images in a directory

			Input:
					- Directory path
	"""
	
	count = 1
	files = os.listdir(directory)
	
	for file in os.listdir(directory):
		print('Deleting file ', count, ' of ', len(files))

		if file.startswith("."):
			pass
		else:
			os.remove(os.path.join(directory, file))
		
		count += 1
		
	print('Done Deleting Files.')
	
	
	
	
	
def grabMTL(mtlFile, val):
    """
        Opens and MTL (metadata) file and return the desired value

        Inputs:
            - mtlFile: Path to an MTL file
            - val: The desired value (ex. 'WRS_PATH')
        
        Outputs:
            - The desired value as a float
    """
    mtl = open(mtlFile).read()
    mtl = mtl.splitlines()
    mtl = [v for v in mtl if val in v]
    mtl = float(mtl[0].split("=")[1])
    return mtl



def ClipLandsat(dfPath, GeomCol, LandsatDir, ClipDir, StartingUTM):

	# Read in boxes shapefile and set the geometry column
	df = pd.read_csv(dfPath)
	df['geometry'] = df[GeomCol].apply(shapely.wkt.loads)
	df = gpd.GeoDataFrame(df, geometry='geometry')

	# Project the boxes shapefile
	utm_proj = pyproj.CRS.from_proj4(StartingUTM)
	df.geometry.crs = utm_proj
	df.geometry = df.geometry.to_crs(utm_proj)

	done_landsats = []

	# For each row in the dataframe
	for i, row in df.iterrows():

		if row.productId in done_landsats:
			print(row.productId, " already downloaded. Skipping.")
			continue

		else:

			# Append the name of the Landsat image to the list to mark it as done
			done_landsats.append(row.productId)

			# Request the html text of the download_url from the amazon server. 
			response = requests.get(row.download_u)

			# If the response status code is fine (200)
			if response.status_code == 200:

				# Import the html to beautiful soup
				html = BeautifulSoup(response.content, 'html.parser')

				# Create the dir where we will put this image files.
				entity_dir = os.path.join(LandsatDir, str(row.data_id))
				os.makedirs(entity_dir)

				print('Downloading ', row.productId, " into folder: ", row.data_id)
				total_images = html.find_all('li')
				image_count = 1

				# Second loop: for each band of this image that we find using the html <li> tag
				for li in html.find_all('li'):

					print("  Downloading file ", image_count, " of ", len(total_images))

					# Get the href tag
					file = li.find_next('a').get('href')

	#				print('  Downloading: {}'.format(file))

					response = requests.get(row.download_u.replace('index.html', file), stream=True)

					with open(os.path.join(entity_dir, file), 'wb') as output:
							shutil.copyfileobj(response.raw, output)
					del response

					image_count += 1


			# Grab the names of the iamges you just downloaded
			direc = os.path.join(LandsatDir, str(row.data_id))
			files = os.listdir(direc)

			# Find the .MTL file and grab the UTM Zone for the projection string
			for k in files:
				if "_MTL" in k:
					zone = str(int(grabMTL(os.path.join(direc, k), "UTM_ZONE")))

			# Project the boxes 
			utm_string = "+proj=utm +zone=" + zone + " +datum=WGS84 +units=m +no_defs +ellps=WGS84 +towgs84=0,0,0"
			print("Projecting to UTM Zone: ", zone)
			utm_proj = pyproj.CRS.from_proj4(utm_string)
			df.geometry = df.geometry.to_crs(utm_proj)

			# For every file you just downloaded...
			for f in files:

				# If it's band 2, 3 or 4...
				if (('B2.TIF' in f) or ('B3.TIF' in f) or ('B4.TIF' in f)) and (".ovr" not in f):

					# Get image information
					band_num = int(f[-5:-4])
					file_path = os.path.join(direc, f)
#					print(band_num, file_path)

					# Open the image
					with rasterio.open(file_path) as src:

						# Get the bounds of the image
						bound_gdf = gpd.GeoDataFrame({"id":1,"geometry":[box(*src.bounds)]})
						bound_gdf.geometry.crs = utm_proj
						bound_gdf.geometry = bound_gdf.geometry.to_crs(utm_proj)

						# Intersect the image with the boxes shapefile
						inp, res = bound_gdf.sindex.query_bulk(df.geometry, predicate = 'intersects')
						df['intersects'] = np.isin(np.arange(0, len(df)), inp)

						# Clip the box that intersects with the image and get length
						int_boxes = df[df['intersects'] == True]
						num_intersect = len(int_boxes)

					to_clip = rxr.open_rasterio(file_path, masked=True).squeeze()
					tmp = df[df['intersects'] == True]

					boxes_count = 1
					# For each box that intersects, clip it to the image
					for i in range(0, num_intersect):

						cur_box = pd.DataFrame(int_boxes.iloc[i]).T
						
						print("  Clipping ", cur_box['data_id'].to_list()[0], ". Box ", boxes_count, " out of ", num_intersect)

						out_image = to_clip.rio.clip(tmp.geometry.apply(mapping), tmp.crs)

						data_id = cur_box['data_id'].to_list()[0]
						im_name = str(data_id) + "_B" + str(band_num) + '.tif'
						clip_path = os.path.join(ClipDir, im_name)

						out_image.rio.to_raster(clip_path)
						
						boxes_count += 1

			delfiles(direc)
			print("\n")
	
	
	
if __name__ == "__main__":
	
	parser = argparse.ArgumentParser()
	parser.add_argument("dfPath", help="Path to CSV with box coordinates & Landsat URL's")
	parser.add_argument("LandsatDir", help="Directory path to Download Landsat imagery into.")
	parser.add_argument("ClipDir", help="Directory path to save Landsat clips to.")
	parser.add_argument("GeomCol", help="Column of the dfPath CSV containing geometry info")
	parser.add_argument("StartingUTM", help="Projection to set the boxes shapefile to.")
	
	args = parser.parse_args()
	print(args)
	
	dfPath, LandsatDir, ClipDir, GeomCol, StartingUTM = args.dfPath, args.LandsatDir, args.ClipDir, args.GeomCol, args.StartingUTM
	
#	dfPath = "./data/AttackBoxes.csv"
#	LandsatDir = "./data/imagery5/"
#	ClipDir = "./data/clipped_imagery4"
#	GeomCol = 'box'
#	StartingUTM = "+proj=utm +zone=38 +datum=WGS84 +units=m +no_defs +ellps=WGS84 +towgs84=0,0,0"
	
	ClipLandsat(dfPath, GeomCol, LandsatDir, ClipDir, StartingUTM)