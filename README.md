# Snow Depth Estimation

```
uv venv ski-project
source ski-project/bin/activate
uv pip install -r requirements.txt
```

# Short-term TODOs

- organize notebooks + methodology
- figure out if FMask is being interpreted correctly
- find out if there is a way to get longer history of SNOTEL data

# TODOs:

- Use historical landsat data
  - https://registry.opendata.aws/usgs-landsat/
  - STAC tutorial: https://code.usgs.gov/eros-user-services/accessing_landsat_data/tutorials/introduction-to-landsat-stac/-/blob/main/Landsat_STAC_Tutorial.ipynb?ref_type=heads#STAC-API
  - STAC root: https://landsatlook.usgs.gov/stac-server/collections/
  - homepage with tutorial: https://www.usgs.gov/landsat-missions/landsat-commercial-cloud-data-access
- investigate B08 - some scenes have it and some don't