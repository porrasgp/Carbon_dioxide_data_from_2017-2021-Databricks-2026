import os
import tempfile
import ssl
import certifi
import cdsapi
from tenacity import retry, stop_after_attempt, wait_exponential

storage_account = "databrickscopernicus"
raw_container = "raw"

# Databricks uses Managed Identity → NO KEYS
# Databricks handles authentication automatically for abfss://*

os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
ssl_context = ssl.create_default_context(cafile=certifi.where())

REQUESTS = [
    {
        "dataset": "satellite-carbon-dioxide",
        "variable": "mid_tropospheric_columns_of_atmospheric_carbon_dioxide",
        "processing_level": "level_2",
        "sensor_and_algorithm": "iasi_metop_a_nlis",
        "year": ["2017","2018","2019","2020","2021"],
        "month": [f"{i:02d}" for i in range(1,13)],
        "day": [f"{i:02d}" for i in range(1,32)],
        "version": "10_1",
        "format": "zip"
    },
    # ... your remaining requests
]

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=4, max=10))
def download_with_retry(request):
    req_copy = request.copy()

    if req_copy["processing_level"] == "level_3":
        req_copy.pop("day", None)

    fname = f"{req_copy['sensor_and_algorithm']}_l{req_copy['processing_level'][-1]}_v{req_copy['version']}.zip"
    tmp_path = os.path.join(tempfile.gettempdir(), fname)

    print(f"▶️ Downloading: {fname}")
    client = cdsapi.Client()
    client.retrieve(req_copy["dataset"], req_copy).download(tmp_path)

    return tmp_path, fname

def upload_to_adls(local_path, filename, request):
    year = request["year"][0] if isinstance(request["year"], list) else "all"

    dest = f"abfss://{raw_container}@{storage_account}.dfs.core.windows.net/satellite_co2/{request['sensor_and_algorithm']}/{year}/{filename}"
    
    print(f"⬆️ Uploading to ADLS: {dest}")
    dbutils.fs.cp(f"file:{local_path}", dest)

def process_request(request):
    local_path, filename = download_with_retry(request)
    upload_to_adls(local_path, filename, request)
    os.remove(local_path)
    print(f"✔️ Completed: {filename}")

for req in REQUESTS:
    process_request(req)

print("🎉 RAW ingestion completed via Managed Identity")
