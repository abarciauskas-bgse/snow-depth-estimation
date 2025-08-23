# search for atl06 granules using bounding box

https://icesat-2.gsfc.nasa.gov//sites/default/files/page_files/ICESat2_ATL06_ATBD_r006.pdf

q: h_li Standard land-ice segment height determined by land ice algorithm - will this capture snow?

# find coincident HLS data using bounding box and cloud cover and date of atl06

curl 'https://cmr.earthdata.nasa.gov/search/granules.json' \
  --compressed \
  -X POST \
  --data-raw 'echo_collection_id=C2021957295-LPCLOUD^&page_num=1^&page_size=20^&bounding_box[]=-121.51448,46.9577,-121.48631,46.95804^&sort_key=-start_date'

# find coincident hls grid cells with ATL06 photon data

this is our training data

inputs = hls band, max and min elevation



# mosaic granules if there are multiple required for coverage

e.g. 2 granules for the same date

# clip to bounding box

# get icesat-2 snowdepth data for the same site

# add snowdepth to pixels

pixels should have some other features - perhaps from other datasets, like DEM, latitude/longitude?

# train a model on labelled pixels


theres going to be very few if any coincident ski resort with atl06 data, we should just train a model on atl06 coincident with cloud-free HLS regardless of bounding box
e.g. find "snowy" areas to train from that are covered by atl06 granules
... we can use the ski resorts to find "snowy" areas but not subset to just the ski resort, but the surrounding area of 1 degree