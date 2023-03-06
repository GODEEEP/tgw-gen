import pytz
from timezonefinder import TimezoneFinder
from datetime import datetime
from tqdm import tqdm

# this takes a while to load, so load it once and use globally
tf = TimezoneFinder()


def get_tz_offset(lat, lon):
  """
  Get timezone offset from utc, rev requires this field be in the 
  metadata but its unclear if it actually affects the wind or solar 
  calcs. I did some tests and the differences between setting the 
  timezone offset properly and setting it to 0 came down to rounding 
  error. out of an abundance of caution I'm setting it properly anyway
  since it doesnt take much time at all to compute (~.1 ms).
  """
  tz = tf.timezone_at(lng=lon, lat=lat)
  timezone = pytz.timezone(tz)
  dt = datetime.utcnow()
  return timezone.utcoffset(dt).total_seconds()/60/60


def get_tz_offset_grid(xlat, xlon):
  s = xlat.shape
  offset = xlat.copy(deep=True).rename('tz_offset')

  for i in range(s[0]):
    for j in range(s[1]):
      lat = xlat[i, j].values[()]
      lon = xlon[i, j].values[()]
      offset[i, j] = get_tz_offset(lat, lon)
  return offset


def get_tz_offset2(latitude, longitude):
  """
  Get the UTC offset for a list of lat/lon points.

  """
  tf = TimezoneFinder()  # reuse

  # query_points = [(13.358, 52.5061), (-120,42)]
  offset = []
  for lon, lat in tqdm(zip(longitude, latitude), total=len(longitude)):
    tz = tf.timezone_at(lng=lon, lat=lat)
    timezone = pytz.timezone(tz)
    dt = datetime.datetime.utcnow()
    offset.append(timezone.utcoffset(dt).total_seconds()/60/60)

  return offset
