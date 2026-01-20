import frappe
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, time
from frappe.core.doctype.user.user import timedelta

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

info_logger = setup_logger('info_logger', '/'.join(['logs', 'logs.log']))


def add_absence_to_attendances(process_attendance_after, last_sync_of_checkin):
    
    # Step 1: Fetch all check-ins
    checkins = frappe.db.sql("""
        SELECT employee_name, employee, time, log_type
        FROM `tabEmployee Checkin`
        ORDER BY employee ASC, time ASC
    """, as_dict=True)
    
    # Step 1: Build base employee_sessions from logs
    employee_sessions = {}
    # all_dates = set()

    for log in checkins:
        emp = log.employee
        log_date = log.time.date()

        # all_dates.add(log_date)

        if emp not in employee_sessions:
            employee_sessions[emp] = {}

        if log_date not in employee_sessions[emp]:
            employee_sessions[emp][log_date] = []

        employee_sessions[emp][log_date].append(log)

    min_date = datetime.strptime(process_attendance_after, "%Y-%m-%d").date()
    max_date = datetime.strptime(last_sync_of_checkin, "%Y-%m-%d %H:%M:%S").date()
    full_date_range = [min_date + timedelta(days=x) for x in range((max_date - min_date).days + 1)]

    # Step 3: Add missing dates to each employee with empty list
    session = employee_sessions.copy()
    for emp in employee_sessions:
        for date in full_date_range:
            if date not in session[emp]:
                session[emp][date] = []

    # Step 3: Process logs
    for employee, daily_logs in session.items():
        employee_name = frappe.db.get_value("Employee", employee, "employee_name")
        for log_date, logs in daily_logs.items():
            sorted_logs = sorted(logs, key=lambda x: 0 if x['log_type'] == 'IN' else 1)

            if len(sorted_logs)==0: # vacation                
                # Step 1: Get employee's holiday list
                holiday_list = frappe.db.get_value("Employee", employee, "holiday_list")

                date_str = log_date.strftime("%Y-%m-%d")
                # Step 2: Check if the date is a holiday in that list
                result = frappe.db.sql(f"""SELECT name FROM `tabHoliday` WHERE parent = '{holiday_list}' AND holiday_date = '{date_str}' LIMIT 1""")
                is_holiday = bool(result)

                # save_or_insert({
                #     "employee": employee,
                #     "employee_name": employee_name,
                #     "attendance_date": log_date,
                #     "in_time": '',
                #     "out_time": '',
                #     "working_hours": '',
                #     "custom_late_entry_in_minutes": '',
                #     "custom_early_exit_in_minutes": '',
                #     "status":'Absent',
                #     "custom_holiday": 1 if is_holiday else 0
                # })
            else:
                calculate_early_exit_and_late_entry(employee, sorted_logs)

                    
def calculate_early_exit_and_late_entry(employee, sorted_logs):
    checkin_time = None
    employee_name = frappe.db.get_value("Employee", employee, "employee_name")
    for log in sorted_logs:
        if log.log_type == "IN" and checkin_time is None:
            checkin_time = log.time
            work_start = datetime.combine(checkin_time.date(), time(8, 30))
            delay_enter = max((checkin_time - work_start).total_seconds() / 60, 0)
            save_or_insert({
                "employee": employee,
                "employee_name": employee_name,
                "attendance_date": checkin_time.date(),
                "in_time": checkin_time.time(),
                "out_time": None,
                "working_hours": None,
                "custom_late_entry_in_minutes": round(delay_enter, 1),
                "custom_early_exit_in_minutes": None,
            })
        elif log.log_type == "OUT":
            checkout_time = log.time
            
            if checkin_time and checkout_time > checkin_time:
                work_start = datetime.combine(checkin_time.date(), time(8, 30))
                work_end = datetime.combine(checkin_time.date(), time(15, 0))
                working_hours = round((checkout_time - checkin_time).total_seconds() / 3600, 2)
                delay_enter = max((checkin_time - work_start).total_seconds() / 60, 0)
                early_exit = max((work_end - checkout_time).total_seconds() / 60, 0)

                save_or_insert({
                    "employee": employee,
                    "employee_name": employee_name,
                    "attendance_date": checkin_time.date(),
                    "in_time": checkin_time.time(),
                    "out_time": checkout_time.time(),
                    "working_hours": working_hours,
                    "custom_late_entry_in_minutes": round(delay_enter, 1),
                    "custom_early_exit_in_minutes": round(early_exit, 1),
                })
                
                checkin_time = None
            elif not checkin_time:
                # No IN before OUT — save only checkout
                work_end = datetime.combine(checkout_time.date(), time(15, 0))
                early_exit = max((work_end - checkout_time).total_seconds() / 60, 0)
                save_or_insert({
                    "employee": employee,
                    "employee_name": employee_name,
                    "attendance_date": checkout_time.date(),
                    "in_time": None,
                    "out_time": checkout_time.time(),
                    "working_hours": None,
                    "custom_late_entry_in_minutes": None,
                    "custom_early_exit_in_minutes": round(early_exit, 1),
                })
    if checkin_time:
        # Final unmatched IN
        work_start = datetime.combine(checkin_time.date(), time(8, 30))
        delay_enter = max((checkin_time - work_start).total_seconds() / 60, 0)
        save_or_insert({
                    "employee": employee,
            "employee_name": employee_name,
            "attendance_date": checkin_time.date(),
            "in_time": checkin_time.time(),
            "out_time": None,
            "working_hours": None,
            "custom_late_entry_in_minutes": round(delay_enter, 1),
            "custom_early_exit_in_minutes": None,
        })

def fetch_for_specific_shift_type(shift, process_attendance_after, last_sync_of_checkin):
    doc = frappe.get_doc("Shift Type", shift)
    doc.process_attendance_after = process_attendance_after
    doc.last_sync_of_checkin = last_sync_of_checkin
    doc.enable_auto_attendance = True
    doc.process_auto_attendance()

def save_or_insert(data):
    """Insert or update a time sheet based on employee and date."""
    existing = frappe.get_all("Attendance",
        filters={
            "employee_name": data["employee_name"],
            "attendance_date": data["attendance_date"]
        },
        fields=["name"]
    )

    if existing:
        attendance = frappe.get_doc("Attendance", existing[0].name)
        attendance.in_time = data.get("in_time")
        attendance.out_time = data.get("out_time")
        attendance.working_hours = data.get("working_hours")
        attendance.custom_holiday = data.get("custom_holiday")
        # attendance.status = data.get("status")
        # attendance.delay_enter = data.get("custom_late_entry_in_minutes")
        # attendance.early_exit = data.get("custom_early_exit_in_minutes")
        attendance.save(ignore_permissions=True)
    else:
        attendance = frappe.get_doc({
            "doctype": "Attendance",
            **data
        })
        attendance.insert(ignore_permissions=True)

    frappe.db.commit()
    
@frappe.whitelist()
def process_auto_attendance_for_all_shifts(shift_type, process_attendance_after, last_sync_of_checkin):
    """Called from hooks"""
    # try:
    shift_list = frappe.get_all("Shift Type", pluck="name")
    if shift_type not in shift_list:
        for shift in shift_list:
            fetch_for_specific_shift_type(shift, process_attendance_after, last_sync_of_checkin)
            # add_absence_to_attendances(process_attendance_after, last_sync_of_checkin)
            
    else:
        fetch_for_specific_shift_type(shift_type, process_attendance_after, last_sync_of_checkin)
        # add_absence_to_attendances(process_attendance_after, last_sync_of_checkin)
    # except Exception as e:
    #     frappe.throw(str(e))
