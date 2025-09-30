import base64
import folium
import fsspec
import io
from PIL import Image
import numpy as np
import rasterio
import earthaccess
import pystac
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import math
import pandas as pd
from IPython.display import HTML

def get_item_bounds(item: earthaccess.DataGranule | pystac.item.Item):
    if isinstance(item, earthaccess.DataGranule):
        bounding_points = item["umm"]["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"]["GPolygons"][0]["Boundary"]["Points"]
        latitudes = [point["Latitude"] for point in bounding_points]
        longitudes = [point["Longitude"] for point in bounding_points]
        return [
            [min(latitudes), min(longitudes)],
            [max(latitudes), max(longitudes)]
        ]
    elif isinstance(item, pystac.item.Item):
        return [[item.bbox[1], item.bbox[0]], [item.bbox[3], item.bbox[2]]]
    else:
        raise ValueError(f"Invalid item type: {type(item)}")

import pystac

def bands_to_files(item: earthaccess.DataGranule | pystac.item.Item):
    if isinstance(item, earthaccess.DataGranule):
        return {link.split('/')[-1].split('.')[-2]: link for link in item.data_links()}
    elif isinstance(item, pystac.item.Item):
        return {asset_key: asset['alternate']['s3']['href'] if asset.get('alternate', {}).get('s3', {}).get('href', None) else None for asset_key, asset in item.to_dict()['assets'].items()}
    else:
        raise ValueError(f"Invalid item type: {type(item)}")

def get_band_data(fs: fsspec.AbstractFileSystem, item: earthaccess.DataGranule | pystac.item.Item, band: str):
    return rasterio.open(fs.open(bands_to_files(item)[band])).read(1)

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

def plot_landsat_and_station(fs: fsspec.AbstractFileSystem, landsat_item: pystac.item.Item, latitude: float, longitude: float):
    # Create the map
    m = folium.Map(
        location=[latitude, longitude],
        zoom_start=7,
        tiles='OpenStreetMap'
    )
    
    red = get_band_data(fs, landsat_item, 'red')
    green = get_band_data(fs, landsat_item, 'green')
    blue = get_band_data(fs, landsat_item, 'blue')
    folium.raster_layers.ImageOverlay(
        name='True Color Image',
        image=f'data:image/png;base64,{rgb_image_str(red, green, blue)}',
        bounds=get_item_bounds(landsat_item),
        opacity=0.8,
        popup='HLS True Color Image'
    ).add_to(m)
    
    folium.CircleMarker(
        location=[latitude, longitude],
        radius=5,
        color='green',
        fill=True,
        fillOpacity=0.7
    ).add_to(m)
    
    # Display with custom size using HTML
    html = m._repr_html_()
    html_with_size = f'<div style="width: 500px; height: 500px;">{html}</div>'
    return HTML(html_with_size)    


def create_season_maps_grid(df):
    seasons = sorted(df['snow_season'].unique())
    n_seasons = len(seasons)
    
    cols = 3
    rows = math.ceil(n_seasons / cols)
    
    specs = [[{"type": "scattermap"} for _ in range(cols)] for _ in range(rows)]
    
    fig = make_subplots(
        rows=rows, 
        cols=cols,
        subplot_titles=[f'Season {season}' for season in seasons],
        specs=specs,
        horizontal_spacing=0.01,
        vertical_spacing=0.01
    )
    
    # Calculate bounds from all data
    lat_min, lat_max = df['latitude'].min(), df['latitude'].max()
    lon_min, lon_max = df['longitude'].min(), df['longitude'].max()
    
    # Add some padding to bounds
    lat_padding = (lat_max - lat_min) * 0.5
    lon_padding = (lon_max - lon_min) * 0.5
    
    for i, season in enumerate(seasons):
        season_data = df[df['snow_season'] == season]
        
        row = (i // cols) + 1
        col = (i % cols) + 1
        
        fig.add_trace(
            go.Scattermap(
                lat=season_data['latitude'],
                lon=season_data['longitude'],
                mode='markers',
                marker=dict(
                    color=season_data['snow_depth_prediction'],
                    colorscale='Viridis',
                    cmin=350,
                    cmax=650,
                    size=8,
                    showscale=True if i == 0 else False,
                    colorbar=dict(title="Snow Depth Prediction") if i == 0 else None
                ),
                text=[f"Snow Depth: {val}" for val in season_data['snow_depth_prediction']],
                hovertemplate="<b>Season: " + season + "</b><br>" +
                             "Lat: %{lat}<br>" +
                             "Lon: %{lon}<br>" +
                             "%{text}<extra></extra>",
                showlegend=False
            ),
            row=row, col=col
        )
        
        # Set bounds for each map
        map_id = f'map{i+1}' if i > 0 else 'map'
        fig.update_layout(**{
            map_id: dict(
                style="open-street-map",
                bounds=dict(
                    west=lon_min - lon_padding,
                    east=lon_max + lon_padding,
                    south=lat_min - lat_padding,
                    north=lat_max + lat_padding
                )
            )
        })
    
    fig.update_layout(
        title_text="Snow Depth Predictions by Season",
        title_x=0.5,
        height=350 * rows,
        width=1400,
        margin=dict(l=20, r=80, t=80, b=20),
        showlegend=False
    )
    
    fig.show()