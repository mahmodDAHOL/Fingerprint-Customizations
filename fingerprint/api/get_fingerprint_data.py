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


    for device in devices:
        try:
            # File to upload
            file_path = (
                f"logs/{device['id']}_{device['ip'].replace('.','_')}_last_fetch_dump.json"
            )
            info_logger.info(file_path)
            # Upload the file
            with open(file_path, "rb") as f:
                files = {
                    "file": (
                        f"{device['id']}_{device['ip'].replace('.','_')}_last_fetch_dump.json",
                        f,
                        "application/json",
                    )
                }
                data = {
                    "is_private": 1,  # 1 = private, 0 = public
                    "fieldname": f"attach_{device['id']}_data",  # The field in the doctype that holds the attachment
                }

                response = session.post(
                    f"{url}/api/method/upload_file", files=files, data=data
                )

            # Check result
            if response.status_code == 200:
                result = response.json()
                if result.get("message"):
                    file_url = result["message"]["file_url"]
                    
                    info_logger.info(f"File uploaded successfully: {file_url}")
                else:
                    info_logger.info("Upload failed:", result)
            else:
                info_logger.info("HTTP Error:", response.status_code, response.text)
                
            info_logger.info(f" records uploaded successfully from {device['ip']}")
            print(f" records uploaded successfully from {device['ip']}")
        except Exception as e:
            info_logger.info(f"failed to upload records from {device['ip']}")
            print(Fore.RED + f"failed to upload records from {device['ip']}" + Style.RESET_ALL)



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
print(f"IPs: {devices_ips} | Company: {company}")
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






