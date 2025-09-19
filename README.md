# Snow Depth Estimation

```
uv venv ski-project
source ski-project/bin/activate
uv pip install -r requirements.txt
```

# TODOs

- IN-PROGRESS: run inference on multiple items
- decide methodology for comparison across time
  - one idea: calculate average snow depth for each month across pixels and compare across season-years
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