# Snow Depth Estimation

```
uv venv ski-project
source ski-project/bin/activate
uv pip install -r requirements.txt
```

# Short-term todos

- update existing training dataset with native projection + remove elevation column
- update model with new training dataset/schema
- skip inference on data that has -9999 for FSCA
- run inference...

# TODOs

- organize notebooks + methodology

# Nice to haves:

- maybe add back in elevation (can't use it while data is in native projection)
- find out if there is a way to get longer history of SNOTEL data
- for HLS data
  - figure out if FMask is being interpreted correctly
  - investigate B08 - some scenes have it and some don't