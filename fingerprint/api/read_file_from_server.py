import frappe
import os
from frappe import _
from frappe.utils import cstr

@frappe.whitelist()
def read_server_file(file_path=None):

    if not file_path:
        frappe.throw(_("File path is required"))

    file_path = cstr(file_path).strip()


    if not os.path.exists(file_path):
        frappe.throw(_("File not found: {0}").format(file_path))

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
        return {"content": content, "file_name": os.path.basename(file_path)}
    except Exception as e:
        frappe.throw(_("Error reading file: {0}").format(str(e)))
        