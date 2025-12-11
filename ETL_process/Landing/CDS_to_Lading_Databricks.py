import os
import tempfile
import logging
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv
import cdsapi
import certifi
import ssl

# Azure SDK
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import AzureError

# ------------------------------------------------------
# LOGGING CONFIG
# ------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ------------------------------------------------------
# SSL CONFIG (CDS / ECMWF requirement)
# ------------------------------------------------------
os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
ssl_context = ssl.create_default_context(cafile=certifi.where())

# ------------------------------------------------------
# ENV VARIABLES
# ------------------------------------------------------
load_dotenv()

AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME", "raw")
AZURE_ACCOUNT_KEY = os.getenv("AZURE_ACCOUNT_KEY")

if not AZURE_CONNECTION_STRING and not AZURE_ACCOUNT_NAME:
    raise ValueError("🚨 Missing AZURE_STORAGE_CONNECTION_STRING or AZURE_STORAGE_ACCOUNT_NAME")

# ------------------------------------------------------
# INIT BLOB CLIENT
# ------------------------------------------------------
try:
    if AZURE_CONNECTION_STRING:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    else:
        from azure.identity import DefaultAzureCredential
        account_url = f"https://{AZURE_ACCOUNT_NAME}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(account_url=account_url, credential=DefaultAzureCredential())

    logger.info("✅ Azure Blob Storage client initialized.")

except Exception as e:
    raise ValueError(f"❌ Error initializing Azure Blob Storage client: {e}")

# Ensure container exists
try:
    blob_service_client.create_container(AZURE_CONTAINER_NAME, fail_on_exist=False)
    logger.info(f"📦 Container '{AZURE_CONTAINER_NAME}' ready.")
except AzureError as e:
    logger.warning(f"⚠️ Container may already exist: {e}")

# ------------------------------------------------------
# SENSOR CONFIGURATIONS
# ------------------------------------------------------
DATASET = os.getenv("CDS_DATASET", "satellite-carbon-dioxide")

SENSORS_CONFIG = {
    "IASI_Metop-A_NLIS": {
        "variable": "co2",
        "sensor": "iasi_metop_a_nlis",
        "years": ["2017", "2018", "2019", "2020", "2021"],
        "version": "10_1",
        "level": "level_2",
        "months": [f"{m:02d}" for m in range(1, 13)],
        "days": [f"{d:02d}" for d in range(1, 32)]
    },
    "IASI_Metop-B_NLIS": {
        "variable": "co2",
        "sensor": "iasi_metop_b_nlis",
        "years": ["2017", "2018", "2019", "2020", "2021"],
        "version": "10_1",
        "level": "level_2",
        "months": [f"{m:02d}" for m in range(1, 13)],
        "days": [f"{d:02d}" for d in range(1, 32)]
    },
    "IASI_Metop-C_NLIS": {
        "variable": "co2",
        "sensor": "iasi_metop_c_nlis",
        "years": ["2019", "2020", "2021"],
        "version": "10_1",
        "level": "level_2",
        "months": [f"{m:02d}" for m in range(1, 13)],
        "days": [f"{d:02d}" for d in range(1, 32)]
    },
    "TANSO-FTS_OCFP": {
        "variable": "xco2",
        "sensor": "tanso_fts_ocfp",
        "years": ["2017", "2018", "2019", "2020", "2021"],
        "version": "7_3",
        "level": "level_2",
        "months": [f"{m:02d}" for m in range(1, 13)],
        "days": [f"{d:02d}" for d in range(1, 32)]
    },
    "TANSO-FTS_SRFP": {
        "variable": "xco2",
        "sensor": "tanso_fts_srfp",
        "years": ["2017", "2018", "2019", "2020", "2021"],
        "version": "2_3_8",
        "level": "level_2",
        "months": [f"{m:02d}" for m in range(1, 13)],
        "days": [f"{d:02d}" for d in range(1, 32)]
    },
    "TANSO2-FTS_SRFP": {
        "variable": "xco2",
        "sensor": "tanso2_fts_srfp",
        "years": ["2019", "2020", "2021"],
        "version": "2_1_0",
        "level": "level_2",
        "months": [f"{m:02d}" for m in range(1, 13)],
        "days": [f"{d:02d}" for d in range(1, 32)]
    },
    "MERGED_EMMA": {
        "variable": "xco2",
        "sensor": "merged_emma",
        "years": ["2017", "2018", "2019", "2020", "2021"],
        "version": "4_5",
        "level": "level_2",
        "months": [f"{m:02d}" for m in range(1, 13)],
        "days": [f"{d:02d}" for d in range(1, 32)]
    },
    "MERGED_OBS4MIPS": {
        "variable": "xco2",
        "sensor": "merged_obs4mips",
        "years": [str(y) for y in range(2017, 2022)],
        "version": "4_5",
        "level": "level_3",
        "months": [f"{m:02d}" for m in range(1, 13)],
        "days": [f"{d:02d}" for d in range(1, 32)]
    }
}

# ------------------------------------------------------
# CDS DOWNLOAD (RETRY HANDLER)
# ------------------------------------------------------
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def download_data(client, request):
    logger.info(f"Iniciando descarga: {request}")
    return client.retrieve(request["dataset"], request)

# ------------------------------------------------------
# AZURE UPLOAD
# ------------------------------------------------------
def upload_to_azure_blob(file_path, blob_name):
    try:
        blob_client = blob_service_client.get_blob_client(container=AZURE_CONTAINER_NAME, blob=blob_name)
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        logger.info(f"✅ Archivo subido: {blob_name}")

    except AzureError as e:
        logger.error(f"❌ Azure upload error: {e}")
        raise

# ------------------------------------------------------
# SENSOR PROCESSING
# ------------------------------------------------------
def process_sensor(sensor_name, config):
    client = cdsapi.Client()

    for year in config["years"]:
        request = {
            "dataset": DATASET,
            "processing_level": config["level"],
            "variable": config["variable"],
            "sensor_and_algorithm": config["sensor"],
            "year": year,
            "month": config["months"],
            "day": config["days"],
            "version": config["version"],
            "format": "zip"
        }

        tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
        tmp_path = tmp_file.name
        tmp_file.close()

        try:
            logger.info(f"🛰️ {sensor_name} - Año {year}")
            result = download_data(client, request)
            result.download(tmp_path)

            blob_name = f"climate-data/{sensor_name}/{year}/data.zip"
            upload_to_azure_blob(tmp_path, blob_name)

        except Exception as e:
            logger.error(f"⛔ Error en {sensor_name} {year}: {e}")

        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
                logger.info(f"🧹 Temp eliminado: {tmp_path}")

# ------------------------------------------------------
# MAIN
# ------------------------------------------------------
def main():
    logger.info("🚀 Iniciando pipeline de CDS → Azure Blob")

    for sensor_name, config in SENSORS_CONFIG.items():
        logger.info(f"\n{'═'*60}\n Procesando sensor: {sensor_name}\n{'═'*60}")
        process_sensor(sensor_name, config)

    logger.info("🎉 Pipeline completado exitosamente.")

    final_url = AZURE_ACCOUNT_NAME or "unknown"
    logger.info(f"📁 Datos en: https://{final_url}.blob.core.windows.net/{AZURE_CONTAINER_NAME}/climate-data/")

if __name__ == "__main__":
    main()
