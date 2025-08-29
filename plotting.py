
import base64
import fsspec
import io
from PIL import Image
import numpy as np
import rasterio
import earthaccess

def get_granule_bounds(granule: earthaccess.DataGranule):
    bounding_points = granule["umm"]["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"]["GPolygons"][0]["Boundary"]["Points"]
    latitudes = [point["Latitude"] for point in bounding_points]
    longitudes = [point["Longitude"] for point in bounding_points]
    return [
        [min(latitudes), min(longitudes)],
        [max(latitudes), max(longitudes)]
    ]

def bands_to_files(granule: earthaccess.DataGranule):
    return {link.split('/')[-1].split('.')[-2]: link for link in granule.data_links()}

def get_band_data(fs: fsspec.AbstractFileSystem, granule: earthaccess.DataGranule, band: str):
    return rasterio.open(fs.open(bands_to_files(granule)[band])).read(1)

def rgb_image_str(red, green, blue):
    rgb = np.stack([red, green, blue], axis=0)

    # Normalize the data for visualization (0-1 range)
    def normalize_band(band):
        band_min = np.nanpercentile(band, 2)  # 2nd percentile
        band_max = np.nanpercentile(band, 98)  # 98th percentile
        return (band - band_min) / (band_max - band_min)

    rgb_normalized = np.stack([
        normalize_band(red),
        normalize_band(green), 
        normalize_band(blue)
    ], axis=0)

    # Clip to valid range
    rgb_normalized = np.clip(rgb_normalized, 0, 1)

    # Convert to 8-bit (0-255)
    rgb_8bit = (rgb_normalized * 255).astype(np.uint8)

    # Convert your rgb_8bit to base64
    rgb_image = Image.fromarray(np.transpose(rgb_8bit, (1, 2, 0)))
    img_buffer = io.BytesIO()
    rgb_image.save(img_buffer, format='PNG')
    img_buffer.seek(0)
    return base64.b64encode(img_buffer.read()).decode()

