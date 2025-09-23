# Snow Depth Estimation

```
uv venv ski-project
source ski-project/bin/activate
uv pip install -r requirements.txt
```

# TODOs

- In progress: organize notebooks + methodology
- Add more model evaluation
  - feature importances (waterfall plot)
  - MSE
  - OOB Error
  - Pearson
  - partial dependence

# Nice to haves:

- maybe add back in elevation (very slow to add via API)
- find out if there is a way to get longer history of SNOTEL data
- for HLS data
  - figure out if FMask is being interpreted correctly
  - investigate B08 - some scenes have it and some don't