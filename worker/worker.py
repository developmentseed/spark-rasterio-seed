import os
import sys
import errno
import json
from urlparse import urlparse


def get_filename(uri):
    return os.path.splitext(os.path.basename(uri))[0]


def mkdir_p(dir):
    try:
        os.makedirs(dir)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(dir):
            pass
        else:
            raise


def vsi_curlify(uri):
    """
    Creates a GDAL-readable path from the given URI
    """
    parsed = urlparse(uri)
    result_uri = ""
    if not parsed.scheme:
        result_uri = uri
    else:
        if parsed.scheme == "s3":
            result_uri = "/vsicurl/http://%s.s3.amazonaws.com%s"
            result_uri = result_uri % (parsed.netloc, parsed.path)
        elif parsed.scheme == "http":
            result_uri = "/vsicurl/%s" % uri
        else:
            raise Exception("Unsupported scheme: %s" % parsed.schem)

    return result_uri


def write_bytes_to_target(target_uri, contents):
    parsed_target = urlparse(target_uri)
    if parsed_target.scheme == "s3":
        client = boto3.client("s3")

        bucket = parsed_target.netloc
        key = parsed_target.path[1:]

        response = client.put_object(
            ACL="public-read",
            Body=bytes(contents),
            Bucket=bucket,
            # CacheControl="TODO",
            ContentType="image/tiff",
            Key=key
        )
    else:
        output_path = target_uri
        mkdir_p(os.path.dirname(output_path))

        with open(output_path, "w") as f:
            f.write(contents)


def copy_image(copy_job):
    (image_uri, dest_uri) = copy_job
    image_uri = vsi_curlify(image_uri)
    creation_options = {
        "driver": "GTiff",
        "tiled": True,
        "compress": "lzw",
        "predictor":   2,
        "sparse_ok": True,
        "blockxsize": 512,
        "blockysize": 512
    }
    with rasterio.open(image_uri, 'r') as src:
        meta = src.meta.copy()
        meta.update(creation_options)
        tmp_path = "/vsimem/" + get_filename(dest_uri)
        with rasterio.open(tmp_path, 'w', **meta) as tmp:
            tmp.write(src.read())

    contents = bytearray(virtual_file_to_buffer(tmp_path))
    write_bytes_to_target(dest_uri, contents)


def run_spark_job():
    from pyspark import SparkConf, SparkContext
    from pyspark.accumulators import AccumulatorParam

    request_uri = sys.argv[1]

    # Read & parse argument, which should be a local path or s3:// uri to a
    # json file
    parsed_request_uri = urlparse(request_uri)
    request = None
    if not parsed_request_uri.scheme:
        request = json.loads(open(request_uri).read())
    else:
        client = boto3.client("s3")
        o = client.get_object(Bucket=parsed_request_uri.netloc,
                              Key=parsed_request_uri.path[1:])
        request = json.loads(o["Body"].read())

    # modify this to suit your argument data
    data = reqest['data']
    output = request['output']

    def make_image_job(image_uri):
        return (image_url, os.path.join(output, get_filename(image_uri)))

    my_rdd = sc.parallelize(data)
    my_rdd.map(make_copy_job)
    .foreach(copy_image)

    print "Done."

if __name__ == "__main__":
    run_spark_job()
