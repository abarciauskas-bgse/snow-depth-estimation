from abc import ABC, abstractmethod
from earthaccess import DataGranule
import fsspec
from pyproj import Transformer
import rioxarray
import xarray as xr
import requests
import numpy as np
from datetime import datetime
from dataclasses import dataclass
from shapely.geometry import Polygon
import geopandas as gpd
from typing import Optional, Dict, Any, Union

@dataclass
class HlsDataBase(ABC):
    """Base class for HLS data processing"""
    fs: fsspec.AbstractFileSystem
    granule: DataGranule
    
    def __post_init__(self):
        self.bands_to_files = self.bands_to_files()
        self.granule_id = self.granule["meta"]["concept-id"]
        self.set_date()
        self.initialize_values()
    
    def set_date(self):
        """Extract date from granule metadata"""
        temporal_data = self.granule["umm"]["TemporalExtent"]["RangeDateTime"]
        _, end_date = temporal_data["BeginningDateTime"], temporal_data["EndingDateTime"]
        self.date = end_date
    
    def bands_to_files(self) -> Dict[str, str]:
        """Map band names to file URLs"""
        return {link.split('/')[-1].split('.')[-2]: link for link in self.granule.data_links()}
    
    @abstractmethod
    def initialize_values(self):
        """Initialize values - implemented differently for point vs polygon"""
        pass
    
    @abstractmethod
    def process_hls_data(self):
        """Process HLS data - implemented differently for point vs polygon"""
        pass


@dataclass
class SnotelHlsData(HlsDataBase):
    """Point-based HLS data with SNOTEL station"""
    lat: float
    lon: float
    station_triplet: str
    
    def __post_init__(self):
        super().__post_init__()
        self.set_3857_latlon()
        self.shared_params = {
            'stationTriplets': self.station_triplet,
            'elements': 'SNWD',
            'duration': 'DAILY',
            'periodRef': 'END',
        }
        self.snow_depth = 0
    
    def initialize_values(self):
        """Initialize point-based band values"""
        for i in range(1, 11):
            setattr(self, f'B{i:02d}', None)
        self.is_snow = None
    
    def set_3857_latlon(self):
        """Convert lat/lon to dataset CRS"""
        dataset = rioxarray.open_rasterio(self.fs.open(self.bands_to_files['B01']))
        transformer = Transformer.from_crs("EPSG:4326", dataset.rio.crs, always_xy=True)
        self.lon_3857, self.lat_3857 = transformer.transform(self.lon, self.lat)
    
    def set_snow_depth(self):
        """Get SNOTEL snow depth data"""
        params = {
            **self.shared_params,
            'beginDate': datetime.strptime(self.date, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d %H:%M"),
            'endDate': datetime.strptime(self.date, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d %H:%M"),
        }
        
        response = requests.get("https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/data", params=params)
        snotel_data = response.json()
        self.snow_depth = snotel_data[0]['data'][0]['values'][0]["value"]
    
    def process_hls_data(self):
        """Extract HLS data at point location"""
        for band, file in self.bands_to_files.items():
            if not band.startswith('B') and not band == 'Fmask':
                continue
            dataset = rioxarray.open_rasterio(self.fs.open(file))
            value = dataset.sel(x=self.lon_3857, y=self.lat_3857, method='nearest').values[0]
            if band.startswith('B'):
                setattr(self, band, value)
            elif band == 'Fmask':
                self.is_snow = (value & 16) > 0


@dataclass
class HlsPolygonMergedData(HlsDataBase):
    """Polygon-based HLS data with merged xarray dataset"""
    polygon: Polygon
    polygon_id: str
    
    def __post_init__(self):
        super().__post_init__()
        self.setup_polygon()
        
        # Initialize outputs
        self.merged_dataset = None

    def initialize_values(self):
        pass
    
    def setup_polygon(self):
        """Convert polygon to appropriate CRS and calculate area"""
        # Get CRS from first band
        sample_dataset = rioxarray.open_rasterio(self.fs.open(self.bands_to_files['B01']))
        self.target_crs = sample_dataset.rio.crs
        
        gdf = gpd.GeoDataFrame([1], geometry=[self.polygon], crs='EPSG:4326')
        self.polygon_projected = gdf.to_crs(self.target_crs).geometry.iloc[0]

    def polygon_fully_covered(self, dataset: xr.Dataset) -> bool:
        """Check if polygon is fully covered by HLS data"""
        from shapely.geometry import box
        ds_bounds = dataset.rio.bounds()
        ds_box = box(ds_bounds[0], ds_bounds[1], ds_bounds[2], ds_bounds[3])
        return ds_box.contains(self.polygon_projected)

    def create_merged_dataset(self) -> xr.Dataset | None:
        """Create merged xarray dataset with all HLS bands"""
        band_arrays = {}
        polygon_fully_covered = True
        
        # Get band names in order (B01-B10)
        band_names = [f'B{i:02d}' for i in range(1, 11)] + ['Fmask']
        available_bands = [band for band in band_names if band in self.bands_to_files]
        
        print(f"Processing {len(available_bands)} bands for merging...")
        
        for band in available_bands:
            if band not in self.bands_to_files:
                continue
            if not polygon_fully_covered:
                continue

            file_url = self.bands_to_files[band]
            
            try:
                # Open the dataset
                dataset = rioxarray.open_rasterio(self.fs.open(file_url))
                
                # check if polygon is fully covered
                # we can't calculate the total volume of snow if the polygon is not fully covered
                # so we return None
                polygon_fully_covered = self.polygon_fully_covered(dataset)
                if not polygon_fully_covered:
                    return None

                dataset = dataset.rio.clip([self.polygon_projected], self.target_crs, drop=True)
                
                # Remove the band dimension (squeeze) since each file has only one band
                dataset = dataset.squeeze('band', drop=True)
                
                if band.startswith('B'):
                    band_arrays[band] = dataset
                elif band == 'Fmask':
                    band_arrays[band] = dataset
                    
            except Exception as e:
                print(f"Error loading band {band}: {e}")
                continue
        
        # Create merged dataset
        merged_dataset = xr.Dataset(band_arrays)
        merged_dataset.rio.write_crs(self.target_crs, inplace=True)
        merged_dataset = merged_dataset.expand_dims({'time': [self.date]})
           
        return merged_dataset
    
    def process_hls_data(self):
        """Main processing workflow"""
        print(f"Processing polygon {self.polygon_id} for date {self.date}")
        
        # Create merged dataset
        self.merged_dataset = self.create_merged_dataset()
        
        if self.merged_dataset is None:
            print("No dataset returned")
            return None
        else:
            print(f"Merged dataset shape: {self.merged_dataset.dims}")


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