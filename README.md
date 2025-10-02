# Snow Depth Estimation Project

## Project Goals and Objectives

This project aims to develop a machine learning model for **historical snow depth reconstruction**. Point-based [SNOTEL (Snow Telemetry)](https://www.drought.gov/data-maps-tools/snow-telemetry-snotel-snow-depth-and-snow-water-equivalent-product) measurements provides ground truth used to train a model on [satellite imagery (Landsat data)](https://registry.opendata.aws/usgs-landsat/). Inference can than be run over  satellite imagery with spatial and temporal coverage over areas of interest.

The current **case study application** is the Crystal Mountain ski area in Washington State.

## Technical Infrastructure

The project utilizes a technical stack including:
- **Data Sources**: SNOTEL API, Landsat STAC API, OpenSkiMap.org
- **Processing**: Python with pandas, numpy, scikit-learn, xarray
- **Modeling Data Format + Storage**: Parquet files persisted on AWS S3
- **Visualization**: matplotlib, plotly, folium
- **Cloud Compute**: AWS EC2 for compute with AWS in-region access to Landsat data
- **Parallel Processing**: ThreadPoolExecutor for data extraction

## Methodology

### 1. Data Collection and Preparation

**Ground Truth Data (SNOTEL)**:
- Used SNOTEL stations in Washington State as ground truth for snow depth measurements
- Focused on stations around Mount Rainier region (e.g., station triplet "941:WA:SNTL")
- Collected daily snow depth data from 2004-2025 using the AWDB REST API
- Defined snow season as November through April of the following year

**Satellite Data (Landsat)**:
- Used Landsat Collection 2 Analysis Ready Data (ARD) Surface Reflectance products
- Collected Landsat scenes coincident with SNOTEL measurement dates
- Extracted spectral band values (red, green, blue, coastal, NIR08, SWIR16, SWIR22) at SNOTEL station locations
- Also collected Fractional Snow Cover Area (FSCA) data from Landsat Collection 2 Level 3 products

**Training Data Assembly**:
- Created spatially and temporally coincident dataset by matching Landsat scenes with SNOTEL measurements
- Processed Landsat items for training data collection
- Used parallel processing for efficient data extraction
- Stored training data in Parquet format for efficient access

### 2. Model Development

**Feature Engineering**:
- Input features: spectral bands (red, green, blue, coastal, NIR08, SWIR16, SWIR22), FSCA, latitude, longitude, month
- Output: snow depth (inches)
- Applied MinMaxScaler for feature normalization
- Excluded features with high missing data rates (coastal: 47% NaN, FSCA: 36% NaN) in linear model

**Model Training**:
1. **Linear Regression Baseline**: 
   - Used as baseline model for comparison
   - Excluded coastal and FSCA features due to high missing data
   - Achieved Pearson correlation coefficient of 0.74

2. **Random Forest Model**:
   - Included all available features including coastal and FSCA
   - Used 100 estimators with random state for reproducibility
   - Achieved Pearson correlation coefficient of 0.90
   - Mean squared error of 0.011 (improvement over linear model's 0.028)

### 3. Inference and Application

Applied trained random forest model to Crystal Mountain ski area polygon by:
- Processing Landsat scenes from 1982-2024
- Generating snow depth predictions for each pixel within the ski area
- Grouping overlapping pixels by day, latitude, and longitude and selecting the max snow depth per pixel per day
- Verifing spatial coverage (>99% coverage maintained) for each day
- Aggregating via a seasonal sum of monthly averages for visualization

## Key Results and Findings

- Generated historical snow depth estimates for Crystal Mountain ski area
- Created map visuals of snow depth across the ski area
- Demonstrated ability to track snow depth changes over multiple decades (1982-2024)

### Model Performance
- **Random Forest Model**: Achieved strong performance with Pearson correlation of 0.90 and MSE of 0.011
- **Feature Importance**: Month was the most important feature, followed by latitude/longitude, with spectral bands providing additional predictive power
- **Linear Baseline**: Provided reasonable baseline performance (Pearson correlation 0.74) but was outperformed by random forest

### Data Quality Insights
- SNOTEL API data availability limited to 2005+ despite metadata indicating earlier start dates

## Areas for Further Work

### Data Expansion
1. **Training Data**:
   - Expand to more SNOTEL stations across different geographic regions
   - Include additional data sources (elevation, LiDAR, weather station data)
   - Incorporate longer historical periods where data is available

2. **Geographic Coverage**:
   - Apply model to other ski resorts and snowy regions
   - Test model performance across different mountain ranges and climates
   - Develop region-specific models for improved accuracy

### Model Improvements
1. **Machine Learning**:
   - Explore computer vision models (CNNs) and deep learning approaches for temporal sequence modeling

2. **Feature Engineering**:
   - Incorporate topographic features (elevation, slope, aspect)
   - Add meteorological data (temperature, precipitation, wind)
   - Include snow cover fraction and other derived indices

## Conclusion

This project successfully demonstrates the feasibility of using satellite imagery and machine learning to estimate historical snow depth over mountainous regions. The random forest model achieved strong performance metrics and was successfully applied to generate multi-decadal snow depth estimates for Crystal Mountain ski area. The methodology provides a starting point for data, modeling and application enhancements.

## Acknowledgments

This project documentation was prepared with assistance from Claude (Anthropic).
