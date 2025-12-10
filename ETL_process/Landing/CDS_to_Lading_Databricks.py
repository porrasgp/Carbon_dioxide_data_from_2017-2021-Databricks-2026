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

# Configuración del logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Configuración SSL global
os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
ssl_context = ssl.create_default_context(cafile=certifi.where())

# Cargar variables de entorno
load_dotenv()

# 🔵 Azure Blob Storage Config
AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME", "raw")

# Validar configuración de Azure
if not AZURE_CONNECTION_STRING and not AZURE_ACCOUNT_NAME:
    raise ValueError("🚨 Se requiere AZURE_STORAGE_CONNECTION_STRING o AZURE_STORAGE_ACCOUNT_NAME en .env")

# Inicializar cliente de Azure Blob Storage
try:
    if AZURE_CONNECTION_STRING:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    else:
        # Fallback: Use DefaultAzureCredential (requires `azure-identity`)
        from azure.identity import DefaultAzureCredential
        account_url = f"https://{AZURE_ACCOUNT_NAME}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(account_url=account_url, credential=DefaultAzureCredential())
    logger.info("✅ Cliente de Azure Blob Storage inicializado.")
except Exception as e:
    raise ValueError(f"❌ Error al inicializar el cliente de Azure Blob: {e}")

# Asegurar que el contenedor exista
try:
    blob_service_client.create_container(AZURE_CONTAINER_NAME, fail_on_exist=False)
    logger.info(f"📦 Contenedor '{AZURE_CONTAINER_NAME}' listo.")
except AzureError as e:
    logger.warning(f"⚠️ No se pudo crear el contenedor (puede ya existir): {e}")

# Definir DATASET (ajustar según el dataset deseado)
DATASET = os.getenv("CDS_DATASET", "satellite-carbon-dioxide")

# Configuración centralizada de sensores (igual que antes)
SENSORS_CONFIG = {
    "IASI_Metop-A_NLIS": {
        "variable": "co2",
        "sensor": "iasi_metop_a_nlis",
        "years": ["2017", "2018", "2019", "2020", "2021"],
        "version": "10_1",
        "level": "level_2",
        "months": ["{:02d}".format(m) for m in range(1, 13)],  # "01" a "12"
        "days": ["{:02d}".format(d) for d in range(1, 32)]     # "01" a "31"
    },
    "IASI_Metop-B_NLIS": {
        "variable": "co2",
        "sensor": "iasi_metop_b_nlis",
        "years": ["2017", "2018", "2019", "2020", "2021"],
        "version": "10_1",
        "level": "level_2",
        "months": ["{:02d}".format(m) for m in range(1, 13)],
        "days": ["{:02d}".format(d) for d in range(1, 32)]
    },
    "IASI_Metop-C_NLIS": {
        "variable": "co2",
        "sensor": "iasi_metop_c_nlis",
        "years": ["2019", "2020", "2021"],
        "version": "10_1",
        "level": "level_2",
        "months": ["{:02d}".format(m) for m in range(1, 13)],
        "days": ["{:02d}".format(d) for d in range(1, 32)]
    },
    "TANSO-FTS_OCFP": {
        "variable": "xco2",
        "sensor": "tanso_fts_ocfp",
        "years": ["2017", "2018", "2019", "2020", "2021"],
        "version": "7_3",
        "level": "level_2",
        "months": ["{:02d}".format(m) for m in range(1, 13)],
        "days": ["{:02d}".format(d) for d in range(1, 32)]
    },
    "TANSO-FTS_SRFP": {
        "variable": "xco2",
        "sensor": "tanso_fts_srfp",
        "years": ["2017", "2018", "2019", "2020", "2021"],
        "version": "2_3_8",
        "level": "level_2",
        "months": ["{:02d}".format(m) for m in range(1, 13)],
        "days": ["{:02d}".format(d) for d in range(1, 32)]
    },
    "TANSO2-FTS_SRFP": {
        "variable": "xco2",
        "sensor": "tanso2_fts_srfp",
        "years": ["2019", "2020", "2021"],
        "version": "2_1_0",
        "level": "level_2",
        "months": ["{:02d}".format(m) for m in range(1, 13)],
        "days": ["{:02d}".format(d) for d in range(1, 32)]
    },
    "MERGED_EMMA": {
        "variable": "xco2",
        "sensor": "merged_emma",
        "years": ["2017", "2018", "2019", "2020", "2021"],
        "version": "4_5",
        "level": "level_2",
        "months": ["{:02d}".format(m) for m in range(1, 13)],
        "days": ["{:02d}".format(d) for d in range(1, 32)]
    },
    "MERGED_OBS4MIPS": {
        "variable": "xco2",
        "sensor": "merged_obs4mips",
        "years": [str(y) for y in range(2017, 2022)],
        "version": "4_5",
        "level": "level_3",
        "months": ["{:02d}".format(m) for m in range(1, 13)],
        "days": ["{:02d}".format(d) for d in range(1, 32)]
    }
}

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def download_data(client, request):
    logger.info(f"Iniciando descarga para la solicitud: {request}")
    result = client.retrieve(request["dataset"], request)
    return result

def upload_to_azure_blob(file_path, blob_name):
    """
    Sube un archivo local a Azure Blob Storage.
    :param file_path: Ruta local del archivo
    :param blob_name: Ruta dentro del contenedor (e.g., 'climate-data/sensor/year/data.zip')
    """
    try:
        blob_client = blob_service_client.get_blob_client(container=AZURE_CONTAINER_NAME, blob=blob_name)
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        logger.info(f"✅ Archivo subido a blob: {blob_name}")
    except AzureError as e:
        logger.error(f"❌ Error al subir a Azure Blob ({blob_name}): {e}")
        raise

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

        with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp_file:
            tmp_file_path = tmp_file.name

        try:
            logger.info(f"🛰️ Procesando {sensor_name} - {year}")
            result = download_data(client, request)
            result.download(tmp_file_path)

            # 🔵 Nuevo: nombre de blob en lugar de s3_key
            blob_name = f"climate-data/{sensor_name}/{year}/data.zip"
            upload_to_azure_blob(tmp_file_path, blob_name)

        except Exception as e:
            logger.error(f"⛔ Error crítico en {sensor_name} {year}: {e}")
        finally:
            if os.path.exists(tmp_file_path):
                os.remove(tmp_file_path)
                logger.info(f"🧹 Archivo temporal {tmp_file_path} eliminado.")

def main():
    logger.info("🚀 Iniciando pipeline unificado de CDS a Azure Blob Storage (Gen2)")
    for sensor_name, config in SENSORS_CONFIG.items():
        logger.info(f"\n{'═'*50}\n🔧 Procesando sensor: {sensor_name}\n{'═'*50}")
        process_sensor(sensor_name, config)
    
    logger.info("✅ Todos los sensores procesados exitosamente!")
    account_name = AZURE_ACCOUNT_NAME or (
        "extracted-from-conn-str" if AZURE_CONNECTION_STRING else "unknown"
    )
    logger.info(f"📁 Estructura final: https://{account_name}.blob.core.windows.net/{AZURE_CONTAINER_NAME}/climate-data/")

if __name__ == "__main__":
    main()
