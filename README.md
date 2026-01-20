# Fingerprint Integration for ERPNext 

## Overview 

The Fingerprint Integration App  is a custom ERPNext application designed to automate the process of fetching attendance data from fingerprint devices and storing it in the ERPNext system. This app bridges the gap between biometric devices (ZKTeco) and ERPNext's HRMS module, enabling seamless management of employee attendance records. 
Key Features 

Automated Data Fetching : Automatically retrieves attendance data (check-ins and check-outs) from fingerprint devices.
Integration with HRMS : Inserts fetched data directly into ERPNext's attendance records via the HRMS module.
Customizable Configuration : Allows administrators to configure device IPs, import start date, and other settings through a user-friendly interface.
Scalable Design : Supports multiple fingerprint devices and can be extended for additional features.
    

Table of Contents 

    Prerequisites 
    Installation 
    Configuration 
    Usage 
    Dependencies 
    Contributing 
    License 
     

Prerequisites 

Before installing and using this application, ensure the following prerequisites are met: 

ERPNext Version : Compatible with ERPNext v14 or later.
Frappe Framework Version : Requires Frappe v14 or later.
HRMS Module : The HRMS module must be installed on your ERPNext site.
Python Version : Python 3.10 or higher.
Redis : Ensure Redis is installed and running on your server.
MariaDB/MySQL : Database server compatible with ERPNext.
Fingerprint Devices : Ensure the fingerprint devices are connected and accessible via IP.
    

## Installation 

To install the Fingerprint Integration App , follow these steps: 
1. Clone the Repository 

Clone the app repository into your bench environment: 

 
```bash
bench get-app fingerprint https://github.com/mahmodDAHOL/fingerprint.git
``` 
 
2. Install the App 

Install the app on your ERPNext site: 

 
```bash
bench --site <site-name> install-app fingerprint
```
 
3. Restart Bench 

Restart the bench to apply changes: 
 
```bash
bench restart
```
 
## Configuration 

After installing the app, you need to configure it to connect to your fingerprint devices. 
1. Access the Configuration Form 

    Write in search bar "fingerprint_config".
    Fill in the required fields.
         
     

2. Save and Test 

    Save the configuration then you will see all attendence data in check-in doctype.

## License

[MIT](https://choosealicense.com/licenses/mit/)