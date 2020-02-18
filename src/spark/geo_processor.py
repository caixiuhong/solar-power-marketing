import pygeohash as pgh
import shapely
from shapely import wkt
from uszipcode import SearchEngine
import pgeocode
import geohash2



class GeoProcessor:
	def __init__(self):

		self.search=None  # to store the search engine for uszipcode, avoid to run search engine every time in worker nodes
	
	def GeoToZip(self, latitude,longitude):
		'''
		use uszipcode to transfer latitude and longitude to zipcode 
		'''
		#option: google map api, need googlemap api key. has request limit
		#geoinfo=Geocoder.reverse_geocode(latitude, longitude) 
		#return geoinfo.postal_code


		if not self.search:
			self.search = SearchEngine()
			result = search.by_coordinates(latitude, longitude, returns=1)
	
		return result[0].zipcode  if result else None

	
	def ZipToGeohash(self, zipcode, precision=4):
		'''
		Use uszipcode to transfer latitude and longitude to zipcode
		Use pygeohash to transfer zipcode to geohash
		precision: define the precision of geohash 
		'''
		
		if not self.search:
			self.search = SearchEngine()
		result = search.by_zipcode(zipcode)
	
		res=pgh.encode(result.lat,result.lng, precision=precision)
	
		return res 

	
	def ZipToGeohash2(self, zipcode, precision=4):
		'''
		Use pgeocode to transfer latitude and longitude to zipcode
		Use geohash2 to transfer zipcode to geohash
		precision: define the precision of geohash 
		'''

		nomi = pgeocode.Nominatim('us')
		info=nomi.query_postal_code(zipcode)
		latitude, longitude=info.latitude, info.longitude
		try:
			geohash=geohash2.encode(info.latitude, info.longitude, precision)
		except:
			geohash=None      #in case there are no zip code for input data
		return geohash

	
	def GeoToGeohash(self, latitude,longitude, precision=4):
		'''
		Use pygeohash to transfer latitude and longitude to goehash
		precision: define the precision of geohash 
		'''

		result=pgh.encode(latitude,longitude, precision=precision)

		return result 
	
	def GeomToGeohash(self, the_geom_4326, zip, precision=4):
		'''
		transfer geometry of EPSG 4326 to geohash
		use shapely to get the centroid of geometry
		use pygeohash to transfere latitude and longitude of centroid to geohash
		if geometry to geohash is not successful, use zip code instead
		'''

		try:   # transfer geometry to geohash
			geometry=shapely.wkt.loads(the_geom_4326)
			if geometry.geom_type == 'MultiPolygon':    # special case for MultiPolygon type geometry
				latitude,longitude=geometry.bounds[1], geometry.bounds[0]
			else:
				#find the centroid of geometry
				latitude, longitude= geometry.centroid.y, geometry.centroid.x
			res=pgh.encode(latitude,longitude, precision=precision)
		except:  # if geometry not successful, use zip code instead
			res=self.ZipToGeohash2(zip,precision=precision)

		return res

	def GeomToLatlong(self,the_geom_4326, precision=4):
		'''
		transfer geometry of EPSG 4326 to latitude, longitude
		use shapely to output centroid of geometry
		'''
		geometry=shapely.wkt.loads(the_geom_4326)
		latitude, longitude= geometry.centroid.y, geometry.centroid.x
		return latitude, longitude



