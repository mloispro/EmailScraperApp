@echo off
REM Change directory to the project folder.
cd /d "C:\Projects\MH\EmailScraperApp"

REM Activate the virtual environment.
call venv\Scripts\activate

REM Run the Python script with the input CSV.
python src\email_spider.py input_data\clinics.csv

REM Pause so the window remains open after execution.
pause
