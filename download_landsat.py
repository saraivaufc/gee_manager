import ee
from taskmanager import TaskManager
from utils import brdf, landsat

ee.Initialize()

BRDFTools = brdf.BRDFTools()
LandsatTools = landsat.LandsatTools()

t_manager = TaskManager(export_class=ee.batch.Export.image.toAsset, max_tasks=20, interval=1, max_errors=3)


collectionT1 = ee.ImageCollection("LANDSAT/LC08/C01/T1_TOA").filterDate("2018-01-01", "2019-01-01")
collectionT2 = ee.ImageCollection("LANDSAT/LC08/C01/T2_TOA").filterDate("2018-01-01", "2019-01-01")

tiles = ee.FeatureCollection("users/saraiva_ufc_gee/layers/LANDSAT_WRS_BRAZIL")

for feature in tiles.getInfo()["features"]:

	geometry = feature["geometry"]["coordinates"]
	path = feature["properties"]["PATH"]
	row = feature["properties"]["ROW"]
	filename = "{path}_{row}".format(path=path, row=row)

	print(path, row)

	collectionFiltered = collectionT1\
		.filterMetadata("WRS_PATH", "equals", int(path))\
		.filterMetadata("WRS_ROW", "equals", int(row))

	collectionFiltered2 = collectionT2\
		.filterMetadata("WRS_PATH", "equals", int(path))\
		.filterMetadata("WRS_ROW", "equals", int(row))

	def maskCollection(image):
		cloudMask = ee.Image(LandsatTools.cloudMask(image, "L8"))
		image_masked = image.updateMask(cloudMask);
		image_corrected = ee.Image(BRDFTools.applyCorrection(image_masked))
		return image_corrected
	
	collectionMasked = collectionFiltered.map(maskCollection).select("B2", "B3", "B4", "B5", "B6", "B7")

	collectionMasked2 = collectionFiltered2.map(maskCollection).select("B2", "B3", "B4", "B5", "B6", "B7")

	bestImage = ee.Image(collectionMasked.limit(1, "CLOUD_COVER").first())

	medianImage1 = ee.Image(collectionMasked\
		.filterMetadata("CLOUD_COVER", "less_than", 50)\
		.limit(3, "CLOUD_COVER")\
		.median())

	medianImage2 = ee.Image(collectionMasked\
		.filterMetadata("CLOUD_COVER", "less_than", 50)\
		.limit(5, "CLOUD_COVER")\
		.median())

	medianImage3 = ee.Image(collectionMasked\
		.filterMetadata("CLOUD_COVER", "less_than", 50)\
		.limit(10, "CLOUD_COVER")\
		.median())

	medianImage4 = ee.Image(collectionMasked.merge(collectionMasked2)\
		.median())

	medianImage5 = ee.Image(collectionFiltered\
		.filterMetadata("CLOUD_COVER", "less_than", 50)\
		.limit(3, "CLOUD_COVER")\
		.select("B2", "B3", "B4", "B5", "B6", "B7")\
		.median())

	medianImage6 = ee.Image(collectionFiltered\
		.filterMetadata("CLOUD_COVER", "less_than", 50)\
		.limit(5, "CLOUD_COVER")\
		.select("B2", "B3", "B4", "B5", "B6", "B7")\
		.median())

	medianImage6 = ee.Image(collectionFiltered\
		.filterMetadata("CLOUD_COVER", "less_than", 50)\
		.limit(10, "CLOUD_COVER")\
		.select("B2", "B3", "B4", "B5", "B6", "B7")\
		.median())

	medianImage7 = ee.Image(collectionFiltered\
		.select("B2", "B3", "B4", "B5", "B6", "B7")\
		.median())

	roi = ee.Geometry.Polygon(geometry)

	joinedImage = ee.ImageCollection([
		medianImage1, 
		medianImage2, 
		medianImage3, 
		medianImage4, 
		medianImage5,
		medianImage6,
		medianImage7
	]).reduce(ee.Reducer.firstNonNull())\
	.clip(roi)\
	.set("system:footprint", geometry)\
	.select(["B2_first", "B3_first", "B4_first", "B5_first", "B6_first"], ["B2", "B3", "B4", "B5", "B6"])

	specifications = {
		'image': joinedImage.multiply(10000).int16(),
		'description': filename,
		'assetId': "users/saraiva_ufc_gee/layers/LANDSAT_8_BRASIL_2018/" + filename,
		'scale': 30,
		'maxPixels': 1.0E13,
	}
	
	t_manager.add_task(filename, specifications)

t_manager.start()
t_manager.join()