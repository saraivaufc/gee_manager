import math
import ee

qaBits57 = [
	[0, 0, 0],
	[1, 1, 0],
	[4, 4, 0],
	[5, 6, 1],
	[7, 8, 1]
]

qaBits8 = [
	[0, 0, 0],
	[1, 1, 0],
	[4, 4, 0],
	[5, 6, 1],
	[7, 8, 1],
	[11, 12, 1]
]

class LandsatTools(object):
	def cloudMask(self, image, satellite):
		"""
		satellite: "L5", "L7", "L8"
		"""
		bqa = ee.Image(image.select("BQA"))
		cloud_mask_image = ee.Image(1).rename(["MASK"])

		qaBits = qaBits57 if satellite in ["L5", "L7"] else qaBits8

		for start, end, desired in qaBits:
			pattern = 0
			for i in xrange(start, end + 1):
				pattern = int(pattern + math.pow(2, i))
			blueprint = bqa.bitwiseAnd(pattern)
			blueprint = blueprint.rightShift(start)
			blueprint = blueprint.eq(desired)
			cloud_mask_image = cloud_mask_image.updateMask(blueprint)
		return cloud_mask_image