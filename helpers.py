from earthaccess import DataGranule
import fsspec
from pyproj import Transformer
import rioxarray
import requests
from datetime import datetime
from dataclasses import dataclass

SNOTEL_API = "https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/data"

@dataclass
class SnotelHlsData:
    fs: fsspec.AbstractFileSystem
    granule: DataGranule
    lat: float
    lon: float
    station_triplet: str

    def __post_init__(self):
        self.bands_to_files = self.bands_to_files()
        self.granule_id = self.granule["meta"]["concept-id"]
        self.set_date()
        self.set_3857_latlon()
        self.shared_params = {
            'stationTriplets': self.station_triplet,
            'elements': 'SNWD',
            'duration': 'DAILY',
            'periodRef': 'END',
        }
        # Initialize default values
        self.snow_depth = 0
        self.B01 = -9999
        self.B02 = -9999
        self.B03 = -9999
        self.B04 = -9999
        self.B05 = -9999
        self.B06 = -9999
        self.B07 = -9999
        self.B08 = -9999
        self.B09 = -9999
        self.B10 = -9999
        self.is_snow = -9999

    def set_date(self):
        # all HLS granules have start and end date on the same day, so for daily snotel data we can use either.
        temporal_data = self.granule["umm"]["TemporalExtent"]["RangeDateTime"]
        _, end_date = temporal_data["BeginningDateTime"], temporal_data["EndingDateTime"]
        self.date = end_date

    def set_3857_latlon(self):
        dataset = rioxarray.open_rasterio(self.fs.open(self.bands_to_files['B01']))
        transformer = Transformer.from_crs("EPSG:4326", dataset.rio.crs, always_xy=True)
        self.lon_3857, self.lat_3857 = transformer.transform(self.lon, self.lat)

    def set_snow_depth(self):
        # Parameters for the request
        params = {
            **self.shared_params,
            'beginDate': datetime.strptime(self.date, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d %H:%M"),
            'endDate': datetime.strptime(self.date, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d %H:%M"),
        }

        # TODO: file bug? Why is the endDate returned in the year 2100?
        response = requests.get(SNOTEL_API, params=params)
        snotel_data = response.json()
        self.snow_depth = snotel_data[0]['data'][0]['values'][0]["value"]

    def bands_to_files(self) -> dict[str, str]:
        return {link.split('/')[-1].split('.')[-2]: link for link in self.granule.data_links()}

    def set_hls_band_data(self) -> dict[str, float]:
        band_data = {f'B{i:02d}': [-9999] for i in range(1, 10)}
        band_data['is_snow'] = [None]

        for band, file in self.bands_to_files.items():
            if not band.startswith('B') and not band == 'Fmask':
                continue
            dataset = rioxarray.open_rasterio(self.fs.open(file))
            value = dataset.sel(x=self.lon_3857, y=self.lat_3857, method='nearest').values[0]
            if band.startswith('B'):
                setattr(self, band, value)
            elif band == ('Fmask'):
                self.is_snow = (value & 16) > 0
        return band_data

def for_parquet_insert(snotel_hls_items: list[SnotelHlsData]) -> dict[str, list]:
    return {
        'date': [item.date for item in snotel_hls_items],
        'snow_depth': [item.snow_depth for item in snotel_hls_items],
        'B01': [item.B01 for item in snotel_hls_items],
        'B02': [item.B02 for item in snotel_hls_items],
        'B03': [item.B03 for item in snotel_hls_items],
        'B04': [item.B04 for item in snotel_hls_items],
        'B05': [item.B05 for item in snotel_hls_items],
        'B06': [item.B06 for item in snotel_hls_items],
        'B07': [item.B07 for item in snotel_hls_items],
        'B08': [item.B08 for item in snotel_hls_items],
        'B09': [item.B09 for item in snotel_hls_items],
        'B10': [item.B10 for item in snotel_hls_items],
        'is_snow': [item.is_snow for item in snotel_hls_items],
        'granule_id': [item.granule_id for item in snotel_hls_items],
        'station_triplet': [item.station_triplet for item in snotel_hls_items],
    }