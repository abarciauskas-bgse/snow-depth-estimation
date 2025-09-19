# Snow Depth Estimation

```
uv venv ski-project
source ski-project/bin/activate
uv pip install -r requirements.txt
```

# TODOs

- IN-PROGRESS: run inference on multiple items
- decide methodology for comparison across time
  - goal: understand how much snow volume has decreased over the years
  - we have snow depth predictions, datetime and latitude, longitude
  - there will be some overlapping pixels within a given time frame (day or month) so we need a way to select the right "value" to represent that time frame
  - 1. reduce overlapping pixels: group by day, lat, lon and pick the highest value for that pixel
  - 2. calculate the average pixel value over each month. Make sure the entire polygon is covered by latitude, longitude.
  - 3. calculate the average snow depth volume for each month.
  - 4. aggregate over the season (November through April)
  - 5. plot
- add more data to model
- Understand how to evaluate model
  - feature importances (waterfall plot)
  - MSE
  - OOB Error
  - Pearson
  - partial dependence
- organize notebooks + methodology

# Nice to haves:

- maybe add back in elevation (very slow to add via API)
- find out if there is a way to get longer history of SNOTEL data
- for HLS data
  - figure out if FMask is being interpreted correctly
  - investigate B08 - some scenes have it and some don't