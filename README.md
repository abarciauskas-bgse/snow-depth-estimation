# Snow Depth Estimation

```
uv venv ski-project
source ski-project/bin/activate
uv pip install -r requirements.txt
```

# Short-term todos

- Remove elevation column from RF model
- run inference on some landsat items, estimate time
- add more data

# TODOs

- organize notebooks + methodology
- improve how -9999 FSCA is handled

# Nice to haves:

- maybe add back in elevation (very slow to add via API)
- find out if there is a way to get longer history of SNOTEL data
- for HLS data
  - figure out if FMask is being interpreted correctly
  - investigate B08 - some scenes have it and some don't