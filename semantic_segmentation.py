import ee
from taskmanager import TaskManager
from shapely.geometry import shape

ee.Initialize()

t_manager = TaskManager(export_class=ee.batch.Export.image.toCloudStorage, max_tasks=10, interval=3, max_errors=3)

mosaic = ee.ImageCollection("projects/nexgenmap/AGRICULTURE/REFERENCES/mosaics").mosaic()
gridSamples = ee.FeatureCollection("ft:1HbycFItKP3OVqniiaqCbVWBe-40XgKkuM_3smosq")

count = 1

for feature in gridSamples.getInfo()["features"]:

	geometry = shape(feature["geometry"])

	minx, miny, maxx, maxy = geometry.bounds

	bottom_left  = [minx, miny]
	top_left     = [minx, maxy]
	bottom_right = [maxx, miny]
	top_right    = [maxx, maxy]

	centroid = [geometry.centroid.x, geometry.centroid.y]

	for coord in [bottom_left, top_left, bottom_right, top_right, centroid]:
		point = ee.Geometry.Point(coord)
		
		roi = point.buffer(380, ee.ErrorMargin(1)).bounds()

		image = mosaic.clip(roi)

		labels = gridSamples.filterBounds(roi).reduceToImage(["classe"], ee.Reducer.first()).unmask(None).rename("L")

		image = image.addBands(labels)\
			.clip(roi)\
			.set("system:footprint", roi)

		filename  = "sample_{0}".format(count)
		
		print(filename)

		specifications = {
			'image': image.select("B", "G", "R", "L").int16(),
			'description': filename,
			'bucket': "agrosatelite-mapbiomas",
			'fileNamePrefix': "unet_samples/" + filename,
			'scale': 3,
			'maxPixels': 1.0E13,
		}
		
		t_manager.add_task(filename, specifications)
		count += 1

t_manager.start()
t_manager.join()