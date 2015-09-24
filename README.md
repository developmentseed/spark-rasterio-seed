# Spark + Rasterio Boilerplate

Boilerplate for setting up a Spark job to run on Amazon EMR

Based heavily on the work of @mojodna and @lossyrob over at https://github.com/hotosm/oam-server-tiler

## Try it Locally

Install Spark

Install python dependencies:
```
pip install rasterio
pip install boto3
```

Put a GeoTIFF in the `temp` folder within this repo (gitignored).

```
./run-local.sh
```
