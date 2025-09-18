from abc import ABC, abstractmethod
import earthaccess
import pystac
import fsspec
from pyproj import Transformer
import rioxarray
import xarray as xr
import numpy as np
from datetime import datetime
from dataclasses import dataclass, field
import requests
from shapely.geometry import Polygon, Point
import geopandas as gpd
from typing import Optional, Dict, Any, Union, List, Tuple
import pandas as pd


@dataclass
class SatelliteDataPoint:
    """Represents satellite data at a single location with optional ground truth"""
    lat: float
    lon: float
    date: str
    item_id: str
    band_values: Dict[str, float] = field(default_factory=dict)
    snow_depth: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for easy serialization/storage"""
        return {
            'lat': self.lat,
            'lon': self.lon,
            'date': self.date,
            'item_id': self.item_id,
            'snow_depth': self.snow_depth,
            'metadata': self.metadata,
            **self.band_values,
        }
    
    def has_ground_truth(self) -> bool:
        """Check if this point has ground truth snow depth data"""
        return self.snow_depth is not None


@dataclass
class SatelliteDataExtractor(ABC):
    """Base class for extracting satellite data from different sources"""
    fs: fsspec.AbstractFileSystem
    item: Union[earthaccess.DataGranule, pystac.item.Item]
    
    def __post_init__(self):
        self.item_id = self._extract_item_id()
        self.date = self._extract_date()
        self.bands_to_files = self._map_bands_to_files()
    
    def _extract_item_id(self) -> str:
        """Extract unique identifier from item"""
        if isinstance(self.item, earthaccess.DataGranule):
            return self.item["meta"]["concept-id"]
        elif isinstance(self.item, pystac.item.Item):
            return self.item.id
        else:
            raise ValueError(f"Invalid item type: {type(self.item)}")
    
    def _extract_date(self) -> str:
        """Extract date from item metadata"""
        if isinstance(self.item, earthaccess.DataGranule):
            temporal_data = self.item["umm"]["TemporalExtent"]["RangeDateTime"]
            return temporal_data["EndingDateTime"]
        elif isinstance(self.item, pystac.item.Item):
            return self.item.to_dict()['properties']['datetime']
        else:
            raise ValueError(f"Invalid item type: {type(self.item)}")
    
    @abstractmethod
    def _map_bands_to_files(self) -> Dict[str, str]:
        """Map band names to file URLs - implementation depends on data source"""
        pass
    
    @abstractmethod
    def extract_at_point(self, lat: float, lon: float) -> Dict[str, float]:
        """Extract band values at a specific lat/lon point"""
        pass
    
    def extract_multiple_points(self, points: List[tuple]) -> List[SatelliteDataPoint]:
        """Extract data at multiple lat/lon points"""
        results = []
        for lat, lon in points:
            try:
                band_values, x, y = self.extract_at_point(lat, lon)
                data_point = SatelliteDataPoint(
                    lat=y,
                    lon=x,
                    date=self.date,
                    item_id=self.item_id,
                    band_values=band_values
                )
                results.append(data_point)
            except Exception as e:
                print(f"Failed to extract data at ({lat}, {lon}): {e}")
                raise e
        return results


@dataclass
class HLSDataExtractor(SatelliteDataExtractor):
    """HLS-specific data extractor"""
    
    # Band mappings for different HLS products
    LANDSAT_BANDS = {
        'coastal': 'B01',
        'blue': 'B02', 
        'green': 'B03',
        'red': 'B04',
        'nir08': 'B05',
        'swir16': 'B06',
        'swir22': 'B07',
        'fsca': 'fsca'
    }

    def _map_bands_to_files(self) -> Dict[str, str]:
        """Map band names to file URLs for HLS data"""
        if isinstance(self.item, earthaccess.DataGranule):
            all_bands = {link.split('/')[-1].split('.')[-2]: link for link in self.item.data_links()}
            return {k: v for k, v in all_bands.items() if k in self.LANDSAT_BANDS}
        elif isinstance(self.item, pystac.item.Item):
            # Primary assets
            primary_assets = {
                asset_key: asset.get('alternate', {}).get('s3', {}).get('href') or asset['href']
                for asset_key, asset in self.item.to_dict()['assets'].items()
                if asset.get('alternate', {}).get('s3', {}).get('href') or asset.get('href')
            }
            
            # Filter to only include LANDSAT_BANDS
            filtered_assets = {k: v for k, v in primary_assets.items() if k in self.LANDSAT_BANDS}
            
            # Try to get FSCA (fractional snow cover) data
            try:
                snow_url = self.item.self_href.replace('landsat-c2ard-sr', 'landsat-c2l3-fsca').replace('SR', 'SNOW')
                snow_item = requests.get(snow_url).json()
                if snow_item.get('assets', {}).get('viewable_snow', {}).get('alternate', {}).get('s3', {}).get('href'):
                    filtered_assets['fsca'] = snow_item['assets']['viewable_snow']['alternate']['s3']['href']
            except Exception as e:
                print(f"Could not retrieve FSCA data: {e}")
            
            return filtered_assets
        else:
            raise ValueError(f"Invalid item type: {type(self.item)}")
    
    def _get_target_crs(self):
        """Get target CRS"""
        # Use first available band to get CRS
        first_band = next(iter(self.bands_to_files.values()))
        dataset = rioxarray.open_rasterio(self.fs.open(first_band))
        return dataset.rio.crs or 'EPSG:3857'
    
    def _get_target_crs_and_transform(self, lat: float, lon: float):
        """Get target CRS and coordinate transformation"""
        # Use first available band to get CRS
        target_crs = self._get_target_crs()
        
        transformer = Transformer.from_crs("EPSG:4326", target_crs, always_xy=True)
        x, y = transformer.transform(lon, lat)
        
        return target_crs, x, y
    
    def extract_at_point(self, lat: float, lon: float) -> Tuple[Dict[str, float], float, float]:
        """Extract HLS band values at a specific point"""
        target_crs, x, y = self._get_target_crs_and_transform(lat, lon)
        band_values = {}
        
        for band_name, file_url in self.bands_to_files.items():
            try:
                dataset = rioxarray.open_rasterio(self.fs.open(file_url))
                value = dataset.sel(x=x, y=y, method='nearest').values[0]
                
                # Handle special cases
                if band_name == 'Fmask':
                    # Extract snow flag from Fmask
                    band_values['is_snow_fmask'] = float((value & 16) > 0)
                else:
                    band_values[band_name] = float(value)
                    
            except Exception as e:
                print(f"Error extracting band {band_name}: {e}")
                continue
        
        return band_values, x, y

    def _project_polygon(self, polygon: Polygon, target_crs: str) -> Polygon:
        """Project a polygon to a target CRS"""
        gdf = gpd.GeoDataFrame([1], geometry=[polygon], crs='EPSG:4326')
        return gdf.to_crs(target_crs).geometry.iloc[0]

    # todo: skip datasets where all values are 0 / fcsa is -9999
    def band_dataset_from_polygon(self, polygon: Polygon) -> xr.Dataset:
        """Extract data from a polygon"""
        # clip items to polygon
        target_crs = self._get_target_crs()
        # project polygon to target CRS
        polygon_projected = self._project_polygon(polygon, target_crs)
        bands_datasets = {}

        for band_name, file_url in self.bands_to_files.items():
            try:
                dataset = rioxarray.open_rasterio(self.fs.open(file_url))
                clipped_dataset = dataset.rio.clip([polygon_projected], target_crs, drop=True)
                bands_datasets[band_name] = clipped_dataset
            except Exception as e:
                print(f"Error extracting band {band_name}: {e}")
                continue
        
        merged_dataset = xr.Dataset(bands_datasets)
        merged_dataset = merged_dataset.rio.reproject('EPSG:4326', inplace=True)
        merged_dataset = merged_dataset.expand_dims({'time': [self.date]})
        return merged_dataset

    def get_elevation(self, lat: float, lon: float) -> float:
        response = requests.get(f"https://epqs.nationalmap.gov/v1/json?x={lon}&y={lat}&wkid=4326&units=Feet&includeDate=false")
        return response.json()['value']

    def extract_from_polygon(self, polygon: Polygon) -> List[SatelliteDataPoint]:
        """Extract data at multiple lat/lon points"""
        band_dataset = self.band_dataset_from_polygon(polygon)
        df = band_dataset.to_dataframe()
        # TODO: this hardcoding is a kludge for now. We should have a better way to handle this.
        df = df[df['fsca'] != -9999]
        # for index, row in df.iterrows():
        #     df.loc[index, 'elevation'] = self.get_elevation(index[2], index[1])
        return df

@dataclass 
class GroundTruthProvider(ABC):
    """Base class for providing ground truth data"""
    
    @abstractmethod
    def get_snow_depth(self, lat: float, lon: float, date: str) -> Optional[float]:
        """Get snow depth at location and date"""
        pass


@dataclass
class SNOTELProvider(GroundTruthProvider):
    """SNOTEL ground truth provider"""
    station_triplet: str
    latitude: float
    longitude: float
    elevation: float
    
    def get_snow_depth(self, lat: float, lon: float, date: str) -> Optional[float]:
        """Get SNOTEL snow depth for the station"""
        try:
            params = {
                'stationTriplets': self.station_triplet,
                'elements': 'SNWD',
                'duration': 'DAILY',
                'periodRef': 'END',
                'beginDate': datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d %H:%M"),
                'endDate': datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d %H:%M"),
            }
            
            response = requests.get(
                "https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/data", 
                params=params
            )
            snotel_data = response.json()
            
            if snotel_data and len(snotel_data) > 0 and len(snotel_data[0]['data']) > 0:
                return float(snotel_data[0]['data'][0]['values'][0]["value"])
            
        except Exception as e:
            print(f"Error getting SNOTEL data: {e}")
        
        return None


@dataclass
class SatelliteDataManager:
    """Main class for managing satellite data extraction and ground truth integration"""
    extractor: SatelliteDataExtractor
    ground_truth_provider: Optional[GroundTruthProvider] = None
    
    def extract_training_data(self, points: List[tuple]) -> List[SatelliteDataPoint]:
        """Extract satellite data with ground truth for training"""
        if self.ground_truth_provider is None:
            raise ValueError("Ground truth provider required for training data")
        
        data_points = self.extractor.extract_multiple_points(points)
        
        # Add ground truth data
        for point in data_points:
            snow_depth = self.ground_truth_provider.get_snow_depth(
                point.lat, point.lon, point.date
            )
            point.snow_depth = snow_depth
            point.metadata['station_triplet'] = self.ground_truth_provider.station_triplet
            point.metadata['elevation'] = self.ground_truth_provider.elevation
            
        return data_points
    
    def extract_inference_data(self, polygon: Polygon) -> List[SatelliteDataPoint]:
        """Extract satellite data for inference (no ground truth needed)"""
        return self.extractor.extract_from_polygon(polygon)
    
    def to_dataframe(self, data_points: List[SatelliteDataPoint]) -> pd.DataFrame:
        """Convert data points to pandas DataFrame for easy analysis"""
        return pd.DataFrame([point.to_dict() for point in data_points])
    
    def filter_valid_training_data(self, data_points: List[SatelliteDataPoint]) -> List[SatelliteDataPoint]:
        """Filter to only data points with valid ground truth"""
        return [point for point in data_points if point.has_ground_truth()]

def for_parquet_insert(snotel_hls_items: list[SatelliteDataPoint]) -> dict[str, list]:
    return {
        'date': [item.date for item in snotel_hls_items],
        'snow_depth': [item.snow_depth for item in snotel_hls_items],
        'coastal': [item.band_values.get('coastal', None) for item in snotel_hls_items],
        'blue': [item.band_values.get('blue', None) for item in snotel_hls_items],
        'green': [item.band_values.get('green', None) for item in snotel_hls_items],
        'red': [item.band_values.get('red', None) for item in snotel_hls_items],
        'nir08': [item.band_values.get('nir08', None) for item in snotel_hls_items],
        'swir16': [item.band_values.get('swir16', None) for item in snotel_hls_items],
        'swir22': [item.band_values.get('swir22', None) for item in snotel_hls_items],
        'fsca': [item.band_values.get('fsca', None) for item in snotel_hls_items],
        'item_id': [item.item_id for item in snotel_hls_items],
        'station_triplet': [item.metadata['station_triplet'] for item in snotel_hls_items],
        'latitude': [item.lat for item in snotel_hls_items],
        'longitude': [item.lon for item in snotel_hls_items],
        'elevation': [item.metadata['elevation'] for item in snotel_hls_items],
    }