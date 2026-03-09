import datetime
import json
import logging
import os
from logging.handlers import RotatingFileHandler

import requests
from pickledb import PickleDB
from zk import ZK
import colorama
from colorama import Fore, Style
colorama.init()

if not os.path.exists("logs"):
    os.makedirs("logs")


def setup_logger(name, log_file, level=logging.INFO, formatter=None):

    if not formatter:
        formatter = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")

    handler = RotatingFileHandler(log_file, maxBytes=10000000, backupCount=50)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.hasHandlers():
        logger.addHandler(handler)

    return logger


error_logger = setup_logger(
    "error_logger", "/".join(["logs", "error.log"]), logging.ERROR
)
info_logger = setup_logger("info_logger", "/".join(["logs", "logs.log"]))
status = PickleDB("/".join(["logs", "status.json"]))

def get_dump_file_name_and_directory(device_id, device_ip):

    return (
        "logs"
        + "/"
        + device_id
        + "_"
        + device_ip.replace(".", "_")
        + "_last_fetch_dump.json"
    )


def get_all_attendance_from_device(
    ip, port=4370, timeout=30, device_id=None, clear_from_device_on_fetch=False
):

    zk = ZK(ip, port=port, timeout=timeout)
    conn = None
    attendances = []
    try:
        conn = zk.connect()
        x = conn.disable_device()
        # device is disabled when fetching data
        info_logger.info("\t".join((ip, "Device Disable Attempted. Result:", str(x))))
        attendances = conn.get_attendance()
        info_logger.info("\t".join((ip, "Attendances Fetched:", str(len(attendances)))))
        status.set(f"{device_id}_push_timestamp", None)
        status.set(f"{device_id}_pull_timestamp", str(datetime.datetime.now()))
        status.save()

        if len(attendances):
            dump_file_name = get_dump_file_name_and_directory(device_id, ip)

            with open(dump_file_name, "w+") as f:
                f.write(
                    json.dumps(
                        list(map(lambda x: x.__dict__, attendances)),
                        default=datetime.datetime.timestamp,
                    )
                )
        x = conn.enable_device()
        info_logger.info("\t".join((ip, "Device Enable Attempted. Result:", str(x))))
    except:
        error_logger.exception(str(ip) + " exception when fetching from device...")
        return "failed"
    finally:
        if conn:
            conn.disconnect()
    return "success"

def upload_fingerprint_records(devices, url, session):
    """
    Upload fingerprint/attendance dump files to ERPNext.
    Deletes existing server-side attachment for the same docname+fieldname before uploading.
    
    Args:
        devices: list of device dicts with 'id' and 'ip'
        url: ERPNext site URL
        session: authenticated requests.Session
        target_doctype: The DocType name that device['id'] belongs to (e.g., "Device", "Attendance Log")
    """
    target_doctype="Fingerprint"
    for device in devices:
        try:
            file_path = f"logs/{device['id']}_{device['ip'].replace('.','_')}_last_fetch_dump.json"
            
            # Check if local file exists
            if not os.path.exists(file_path):
                info_logger.info(f"Local file not found, skipping: {file_path}")
                print(Fore.YELLOW + f"File not found: {file_path}" + Style.RESET_ALL)
                continue

            fieldname = f"attach_{device['id']}_data".lower().replace(" ","_")
            docname = 'eg2g83k1ar'

            # === STEP 1: DELETE EXISTING FILE ON SERVER ===
            try:
                # Build filters as JSON string for Frappe API
                filters = json.dumps([
                    ["attached_to_doctype", "=", target_doctype],
                    ["attached_to_name", "=", docname],
                    ["attached_to_field", "=", fieldname]
                ])
                
                # Query existing files
                resp = session.get(
                    f"{url}/api/resource/File",
                    params={"filters": filters},
                    timeout=30
                )
                if resp.status_code == 200:
                    existing_files = resp.json().get("data", [])
                    for file_doc in existing_files:
                        file_name = file_doc.get("name")  # File document name (ID)
                        delete_resp = session.delete(
                            f"{url}/api/resource/File/{file_name}",
                            timeout=30
                        )
                        if delete_resp.status_code in [200, 202]:
                            info_logger.info(f"Deleted existing server file: {file_name}")
                        else:
                            error_logger.warning(
                                f"Failed to delete server file {file_name}: {delete_resp.status_code} - {delete_resp.text}"
                            )
                else:
                    error_logger.warning(
                        f"Could not query existing files: {resp.status_code} - {resp.text}"
                    )
            except Exception as e:
                error_logger.exception(f"Error during server file cleanup for {device['ip']}: {e}")
                # Continue anyway - upload might still succeed

            # === STEP 2: UPLOAD NEW FILE ===
            info_logger.info(f"Uploading file: {file_path}")
            with open(file_path, "rb") as f:
                files = {
                    "file": (
                        f"{device['id']}_{device['ip'].replace('.','_')}_last_fetch_dump.json",
                        f,
                        "application/json",
                    )
                }
                data = {
                    "is_private": 1,
                    "fieldname": fieldname,
                    "docname": docname,
                    "doctype": target_doctype,  # Critical for proper linking
                }

                response = session.post(
                    f"{url}/api/method/upload_file", 
                    files=files, 
                    data=data,
                    timeout=60
                )

            # === STEP 3: HANDLE UPLOAD RESULT ===
            if response.status_code == 200:
                result = response.json()
                if result.get("message"):
                    file_url = result["message"]["file_url"]
                    info_logger.info(f"File uploaded successfully: {file_url}")
                    print(Fore.GREEN + f"Uploaded: {device['ip']}" + Style.RESET_ALL)
                    
                    # Optional: delete local file after successful server upload
                    # os.remove(file_path)
                else:
                    error_logger.error(f"Upload failed (no message): {result}")
                    print(Fore.RED + f"Upload failed: {device['ip']}" + Style.RESET_ALL)
            else:
                error_logger.error(f"HTTP {response.status_code}: {response.text}")
                print(Fore.RED + f"HTTP {response.status_code} for {device['ip']}" + Style.RESET_ALL)
                
        except FileNotFoundError:
            error_logger.exception(f"Local file not found: {file_path}")
            print(Fore.RED + f"File not found: {file_path}" + Style.RESET_ALL)
        except Exception as e:
            error_logger.exception(f"Unexpected error uploading {device['ip']}")
            print(Fore.RED + f"Error uploading {device['ip']}" + Style.RESET_ALL)


url = "https://moi-mis.gov.sy"
username = "USERNAME"
password = "PASSWORD"
ips = "IPs"
company = "COMPANY"

# Login to get session cookies
session = requests.Session()
login_response = session.post(
    f"{url}/api/method/login", json={"usr": username, "pwd": password}
)

# Check if login was successful
if login_response.json().get("message") != "Logged In":
    info_logger.info("Login failed")
    exit()

devices_ips = ips.split(',')
number_of_devices = len(devices_ips)
devices = []
for device_number, device_ip in enumerate(devices_ips, 1):
    devices.append({"ip": device_ip, "id": f"{company}_{device_number}"})


all_success = True  # Flag to track success
print(f"{devices_ips} | {company}")
for device in devices:
    try:
        res = get_all_attendance_from_device(
            device["ip"],
            port=4370,
            timeout=30,
            device_id=device["id"],
            clear_from_device_on_fetch=False,
        )
        if res == "success":
            continue
        else:
            info_logger.info(f"Records fetching failed for {device['ip']}")
            print(Fore.RED + f"Records fetching failed for {device['ip']}" + Style.RESET_ALL)
            all_success = False
            break  # Optional: stop loop on first failure
    except Exception as e:
        info_logger.exception(f"Error fetching records from {device['ip']}: {e}")
        print(Fore.RED + f"Error fetching records from {device['ip']}" + Style.RESET_ALL)
        all_success = False
        break

# Only run upload if all devices succeeded
if all_success:
    upload_fingerprint_records(devices, url, session)
else:
    print(Fore.YELLOW + "Upload skipped due to device fetch failure." + Style.RESET_ALL)






