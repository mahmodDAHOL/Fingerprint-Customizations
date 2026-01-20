@echo off
:: Check if Python is installed
where python >nul 2>&1
if %ERRORLEVEL% equ 0 (
    echo Python is already installed.
) else (
    echo Python not found. Installing Python...

    :: Download Python installer
    curl -L --output python-installer.exe "https://www.python.org/ftp/python/3.12.5/python-3.12.5-amd64.exe"

    echo Installing Python...
    :: Run installer silently with Add to PATH and pip
    python-installer.exe /quiet InstallAllUsers=1 PrependPath=1 Include_pip=1 Include_test=0

    echo Python installed successfully.

    :: Wait for PATH to update
    timeout /t 15 >nul
)

:: Check if pip is available
where pip >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo Pip not found. Installing pip manually...

    curl -L https://bootstrap.pypa.io/get-pip.py -o get-pip.py
    python get-pip.py

    echo Pip installed successfully.
)

:: Ensure pip is up to date and install required packages
echo Installing required packages...
python -m pip install --upgrade pip
pip install --user pickledb pyzk

echo All packages installed.

:: Run the Python script from the same directory as the .bat file
echo Running Python script...
cd /d "%~dp0%"
python "%~dp0%get_fingerprint_data.py"

pause
