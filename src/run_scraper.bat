@echo off

REM Activate the virtual environment
call "C:\Users\edward_b\OneDrive - Institute for Fiscal Studies\Work\Brazil social insurance\venv\Scripts\activate.bat"

REM Empty the folder C:/temp
del /q /f "C:\temp\*.*"
for /d %%i in ("C:\temp\*") do rd /s /q "%%i"


REM Run your script (adjust path if needed)
python "C:\Users\edward_b\OneDrive - Institute for Fiscal Studies\Work\Brazil social insurance\MEI_scraper\src\main.py"