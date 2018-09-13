import ee
from taskmanager import TaskManager

ee.Initialize()

t_manager = TaskManager(export_class=ee.batch.Export.image.toCloudStorage, max_tasks=100, interval=3, max_errors=3)

#mosaic = ee.ImageCollection("projects/nexgenmap/AGRICULTURE/REFERENCES/mosaics").mosaic()
mosaic = ee.ImageCollection("projects/nexgenmap/MOSAIC/production-1").filterMetadata("cadence", "equals", "monthly").filterDate("2017-08-01").mosaic()


gridSamples = ee.FeatureCollection("ft:1wSa4gXWW9W8rlPl6pi7nowqcn1tZxDX_Z0aZMcc8")

count = 1000

samples = gridSamples.filterMetadata("classe", "equals", 1)

for feature in samples.getInfo()["features"]:
	feature_geometry = ee.Geometry.Polygon(feature["geometry"]["coordinates"])

	roi = feature_geometry.buffer(150).bounds()

	image = mosaic.clip(roi).set("system:footprint", roi)

	filename  = "pivot_{0}".format(count)
	print(filename)

	specifications = {
		'image': image.select("B", "G", "R", "N").int16(),
		'deion': filename,
		'bucket': "agrosatelite-mapbiomas",
		'fileNamePrefix': "nexgenmap_pivots/" + filename,
		'scale': 4,
		'maxPixels': 1.0E13,
	}
	
	t_manager.add_task(filename, specifications)

	count += 1

t_manager.start()
t_manager.join()
