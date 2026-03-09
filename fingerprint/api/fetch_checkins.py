import frappe
import datetime
import json
import os
import logging
from pickledb import PickleDB
from logging.handlers import RotatingFileHandler
# from hrms.hr.doctype.employee_checkin.employee_checkin import add_log_based_on_employee_field
from collections import defaultdict
from frappe.utils import cint, get_datetime


def add_log_based_on_employee_field(
	employee_field_value,
	timestamp,
	device_id=None,
	log_type=None,
	skip_auto_attendance=0,
	employee_fieldname="attendance_device_id",
	latitude=None,
	longitude=None,
    over_night=None
):
    """Finds the relevant Employee using the employee field value and creates a Employee Checkin."""

    if not employee_field_value or not timestamp:
        frappe.throw(_("'employee_field_value' and 'timestamp' are required."))

    employee = frappe.db.get_values(
        "Employee",
        {employee_fieldname: employee_field_value},
        ["name", "employee_name", employee_fieldname],
        as_dict=True,
    )
    if employee:
        employee = employee[0]
    else:
        frappe.throw(
            _("No Employee found for the given employee field value. '{}': {}").format(
                employee_fieldname, employee_field_value
            )
        )

    doc = frappe.new_doc("Employee Checkin")
    doc.employee = employee.name
    doc.employee_name = employee.employee_name
    doc.time = timestamp
    doc.device_id = device_id
    doc.log_type = log_type
    doc.latitude = latitude
    doc.longitude = longitude
    doc.custom_over_night = over_night
    if cint(skip_auto_attendance) == 1:
        doc.skip_auto_attendance = "1"
    doc.insert()
    return doc

def setup_logger(name, log_file, level=logging.INFO, formatter=None):
    
    if not formatter:
        formatter = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')

    handler = RotatingFileHandler(log_file, maxBytes=10000000, backupCount=50)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.hasHandlers():
        logger.addHandler(handler)

    return logger


LOGS_DIRECTORY = 'logs'
if not os.path.exists(LOGS_DIRECTORY):
    os.makedirs(LOGS_DIRECTORY)

error_logger = setup_logger('error_logger', '/'.join([LOGS_DIRECTORY, 'error.log']), logging.ERROR)
info_logger = setup_logger('info_logger', '/'.join([LOGS_DIRECTORY, 'logs.log']))
status = PickleDB('/'.join([LOGS_DIRECTORY, 'status.json']))
full_site_path = frappe.get_site_path()
full_site_path = os.path.abspath(frappe.get_site_path())


def edit_attendance(record):
    record['timestamp'] = datetime.datetime.fromtimestamp(record['timestamp']) + datetime.timedelta(hours=3)
    return record

def get_shift_date(log):
    OVERNIGHT_CUTOFF_HOUR = 4  # i.e., 00:00 ≤ time < 04:00

    ts = log['timestamp']
    if 0 <= ts.hour < OVERNIGHT_CUTOFF_HOUR:
        # Between 00:00 and 02:59:59 inclusive
        minutes_past_midnight = ts.hour * 60 + ts.minute
        log['overnight'] = minutes_past_midnight
        log['timestamp'] = datetime.datetime.combine(
        ts.date() - datetime.timedelta(days=1),
        datetime.time(23, 59, 59)
        )
    else:
        log['overnight'] = 0  # or None / omit key
    log['shift_date'] = log['timestamp'].date()
    return log

def add_punch_direction(device_attendance_logs):
    # Step 1: Assign 'shift_date' to every log
    for log in device_attendance_logs:
        # log['timestamp'] = log['timestamp'] + datetime.timedelta(hours=10) # test over night shift type
        log = get_shift_date(log)

    # Step 2: Group logs by (user_id, shift_date)
    # key: (user_id, shift_date), value: list of logs
    groups = defaultdict(list)
    for log in device_attendance_logs:

        key = (log['user_id'], log['shift_date'])
        groups[key].append(log)
    # Step 3: For each (user, shift_day) group, mark first as 'IN', last as 'OUT'
    for day_logs in groups.values():

        # Sort by actual timestamp
        day_logs_sorted = sorted(day_logs, key=lambda x: x['timestamp'])
        
        # Default: mark all as 'OTHER' first
        # for log in day_logs_sorted:
        #     log['log_type'] = 'OTHER'

        # if len(day_logs_sorted)>2:
        #     print(day_logs_sorted)
        if len(day_logs_sorted) == 1:
            day_logs_sorted[0]['log_type'] = 'IN'
        elif len(day_logs_sorted) > 1:
            day_logs_sorted[0]['log_type'] = 'IN'
            day_logs_sorted[-1]['log_type'] = 'OUT'
        # if log['user_id'] == '792':
        #     breakpoint()
        #     xx = 1

    # Optional: sort entire list chronologically for consistency
    device_attendance_logs.sort(key=lambda x: x['timestamp'])

    # # Filter: all keys where first element (user_id) == '608'
    # user_608_entries = []
    # for device_attendance_log in device_attendance_logs:
    #     if device_attendance_log['user_id'] == '482':
    #         user_608_entries.append(device_attendance_log)

    # Return the now-modified input list
    return device_attendance_logs

def process_device_attendance_logs(device_attendance_logs, company, chunk_size=100):
    total = len(device_attendance_logs)
    if total == 0:
        frappe.msgprint(_("No logs to process."))
        return

    processed = 0
    errors = 0

    frappe.publish_realtime(
        "msgprint", 
        _("Starting processing of {0} attendance logs...").format(total),
        user=frappe.session.user
    )

    for i, device_attendance_log in enumerate(device_attendance_logs):
        try:
            add_log_based_on_employee_field(
                user_id=device_attendance_log['user_id'],
                timestamp=device_attendance_log['timestamp'],
                company=company,
                log_type=device_attendance_log['log_type'],
                over_night=device_attendance_log.get('overnight', 0)
            )
            processed += 1

        except Exception as e:
            errors += 1
            frappe.log_error(
                title="Attendance Log Processing Error",
                message=f"Log: {device_attendance_log}\nError: {str(e)}\nTraceback: {frappe.get_traceback()}"
            )
            # Continue to next log — don't break

        # 🟢 Commit in chunks & update progress
        if (i + 1) % chunk_size == 0 or i == total - 1:
            frappe.db.commit()  # Save this chunk
            
            # Update progress (with ETA estimation)
            percent_complete = int((i + 1) / total * 100)
            frappe.publish_progress(
                percent=percent_complete,
                title=_("Processing Attendance Logs"),
                description=_("Processed {0}/{1} logs ({2} errors)").format(i + 1, total, errors)
            )

    # ✅ Final summary
    summary = _("✅ Completed: {0}/{1} logs processed").format(processed, total)
    if errors:
        summary += _(" | ⚠️ {0} failed (see Error Log)").format(errors)

    frappe.msgprint(summary, title=_("Attendance Import"), indicator="green" if not errors else "orange")
    frappe.publish_realtime(
        "list_update", 
        {"doctype": "Employee Checkin"},  # refresh Employee Checkin list
        user=frappe.session.user
    )

def merge_json_files(file_paths):
    merged_data = []

    for file_path in file_paths:
        # Skip if file doesn't exist
        if not os.path.isfile(file_path):
            continue

        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            if not content:
                continue

            data = json.loads(content)
            # Ensure data is a list (for safe concatenation)
            if isinstance(data, list):
                merged_data.extend(data)
            elif isinstance(data, dict):
                # Wrap dict in list if single object
                merged_data.append(data)
            else:
                frappe.log_error(
                    f"Unexpected JSON root type in {file_path}: {type(data)}",
                    "Merge JSON Files"
                )


    return merged_data

def pull_process_and_push_data(fingerprint_file_paths, import_start_date, import_end_date, company):
    
    """ Takes a single device config as param and pulls data from that device.

    params:
    device: a single device config object from the local_config file
    device_attendance_logs: fetching from device is skipped if this param is passed. used to restart failed fetches from previous runs.
    """
    # try:
    #     content = open(file_path, 'r').read()
    # except:
    #     frappe.msgprint(f"file {file_path} is not exist")
    # attendances = json.loads(content)
    attendances = merge_json_files(fingerprint_file_paths)
    updated_attendances = [edit_attendance(att) for att in attendances]
    attendances = sorted(updated_attendances, key=lambda x: x['timestamp'])
    device_attendance_logs = attendances
    if not device_attendance_logs:
        return

    import_start_date = datetime.datetime.strptime(import_start_date, '%Y-%m-%d')

    import_end_date = datetime.datetime.strptime(import_end_date, '%Y-%m-%d')

    index_of_start = None
    index_of_end = None

    # Find start index
    for i, x in enumerate(device_attendance_logs):
        if x['timestamp'] >= import_start_date:
            index_of_start = i
            break

    # Find end index
    for i, x in enumerate(device_attendance_logs):
        if x['timestamp'] > import_end_date:
            index_of_end = i
            break
    # If end date not found, include all remaining logs
    if index_of_end is None:
        index_of_end = len(device_attendance_logs)

    # Process logs between start and end date
    device_attendance_logs = add_punch_direction(device_attendance_logs[index_of_start:index_of_end])
    for device_attendance_log in device_attendance_logs:
        try:

            add_log_based_on_employee_field(
                device_attendance_log['user_id'], device_attendance_log['timestamp'], company, device_attendance_log['log_type'], over_night=device_attendance_log['overnight']
            )
            # frappe.msgprint(f"{device_attendance_log['user_id']}  {device_attendance_log['timestamp']}")
            print(f"{device_attendance_log['user_id']} saved at {device_attendance_log['timestamp']}")

        except Exception as e:
            frappe.msgprint(f"Error fetching user {device_attendance_log['user_id']}: {e}")
            print(f"Error fetching user {device_attendance_log['user_id']}: {e}")
            pass
    frappe.db.commit()


# @frappe.whitelist()
# def fetch_checkins(import_start_date, import_end_date, company='Ministry of Information'):
#     frappe.msgprint(import_start_date)
#     req_files_found = False
#     for file_name in os.listdir(full_site_path+'/private/files'):
#         # if file_name.endswith(f"_last_fetch_dump.json") and company.replace(' ', '_').lower() in file_name:
#         if file_name.endswith(f"_last_fetch_dump.json") and company in file_name:
#             file_path = os.path.join(full_site_path+'/private/files', file_name)
            
#             req_files_found = True
#             info_logger.info("Processing File: "+ file_path)
#             try:
#                 pull_process_and_push_data(file_path, import_start_date, import_end_date, company)
#                 info_logger.info("Successfully processed File: "+ file_path)

#                 # ✅ Delete the file after successful processing
#                 # os.remove(file_path)
#                 info_logger.info(f"Deleted file after processing: {file_path}")
#                 info_logger.info("Mission Accomplished!")
#             except:
#                 frappe.msgprint('exception when calling pull_process_and_push_data function for file '+ file_path)
#     if not req_files_found:
#         frappe.throw("""No files found for bio devices of your company, you should upload required json file,
#                      to do that click 'Fetch & Upload' button then uncompress downloaded file,
#                      then double click 'run_python.bat' file after connect your device with bio devices,
#                      then wait until message appear that indicate files are uploaded.
#                      """)
@frappe.whitelist()
def fetch_checkins(import_start_date, import_end_date, company='Ministry of Information'):
    frappe.msgprint(import_start_date)
    req_files_found = False
    files_dir = os.path.join(full_site_path, 'private', 'files')
    company_slug = company.replace(' ', '_').lower()

    fingerprint_file_paths = [
        os.path.join(files_dir, f)
        for f in os.listdir(files_dir)
        if "_last_fetch_dump" in f and company in f
    ]

    req_files_found = True
    pull_process_and_push_data(fingerprint_file_paths, import_start_date, import_end_date, company)

    if not req_files_found:
        frappe.throw("""No files found for bio devices of your company, you should upload required json file,
                     to do that click 'Fetch & Upload' button then uncompress downloaded file,
                     then double click 'run_python.bat' file after connect your device with bio devices,
                     then wait until message appear that indicate files are uploaded.
                     """)
        
@frappe.whitelist()
def get_app_info():
    """Return fingerprint app path and info"""
    # Get the absolute path to the fingerprint app directory
    app_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    
    return {
        "app_path": app_path,  # e.g., /home/user/frappe-bench/apps/fingerprint
        "app_name": "fingerprint"
    }