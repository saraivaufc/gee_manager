import ee
from taskmanager import TaskManager

ee.Initialize()

t_manager = TaskManager(export_class=ee.batch.Export.image.toCloudStorage, max_tasks=20, interval=4, max_errors=3)

samples = ee.FeatureCollection("ft:1ZuL9YA6zkArwQVET3nmY3AzRVTiEqKKo1nmjtYQF")
grids = ee.FeatureCollection("ft:1QoO2aSSoNnsSbqJOf-DVG8l5-6cOYE2suRSkQB5h")
references = ee.ImageCollection("projects/nexgenmap/AGRICULTURE/REFERENCES/mosaics");

grid_name = "SF-23-Y-C"

grid = grids.filterMetadata("name", "equals", grid_name)
gridReference = references.filterMetadata("grid_name", "equals", grid_name).mosaic().clip(grid.geometry())

gridSamples = samples.filterBounds(grid.geometry()).limit(1000)

pivot_count = 2000
infra_count = 2000
for feature in gridSamples.getInfo()["features"]:

	roi = ee.Geometry.Polygon(feature["geometry"]["coordinates"])
	roi = roi.buffer(150).bounds()
	
	image = gridReference.clip(roi).set("system:footprint", roi)
	
	if feature["properties"]["classe"] == 1:
		label = "pivot"
		filename  = label + "_" + str(pivot_count)
		pivot_count += 1
	else:
		label = "infra"
		filename  = label + "_" + str(infra_count)
		infra_count += 1

	specifications = {
		'image': image.select("B", "G", "R", "N").int16(),
		'description': filename,
		'bucket': "agrosatelite-mapbiomas",
		'fileNamePrefix': "nexgenmap/" + filename,
		'scale': 4,
		'maxPixels': 1.0E13,
	}

	t_manager.add_task(filename, specifications)


t_manager.start()
t_manager.join()