# Snow Depth Estimation

This projects models and estimates snow depth using SNOTEL snow depth as ground truth alongside historical Landsat data. It collects coincident SNOTEL ground truth snow depth with Landsat band data and trains a model which is then applied to historical and spatially extensive Landsat data for estimation.

## Setup

```
uv venv ski-project
source ski-project/bin/activate
uv pip install -r requirements.txt
```

## Methodology

See notebooks, ordered according to methodological order.

## Limitations

At time of writing, training data and predicted area are limited to a few sites around Mt. Rainer in order to evaluate snow depth historically over the Crystal Mountain ski area. 

The model is a random forest model.

In future work, I wish to:
* grow the training data set, both in number of sites and potential data sources
  * for example, elevation data or LiDAR data
* areas of interest for prediction (i.e. other ski resorts or snowy regions)
* models empoloyed (i.e. employ a computer vison model)
