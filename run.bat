@echo off
setlocal

echo ------------------------------------------------------------
echo Email Scraper Configuration (Press 'Enter' to accept defaults)
echo ------------------------------------------------------------
rem Prompt for output CSV path, default is output\scraped_emails.csv
set /p outFile="Output CSV path [output\\scraped_emails.csv]: "
if "%outFile%"=="" set "outFile=output\\scraped_emails.csv"

rem Prompt for number of workers, default is 5
set /p workers="Number of workers [5]: "
if "%workers%"=="" set "workers=5"

rem Prompt for force re-scrape
set /p force="Force full re-scrape? (y/N) [N]: "
if /I "%force%"=="y" (
    set "forceFlag=-f"
) else (
    set "forceFlag="
)

echo.
echo Configuration:
echo   Output CSV: %outFile%
echo   Workers   : %workers%
echo   Force     : %force%
echo ------------------------------------------------------------

@REM rem Confirm before proceeding
@REM set /p confirm="Proceed with scraping? (y/N): "
@REM if /I not "%confirm%"=="y" (
@REM     echo Operation canceled.
@REM     goto :eof
@REM )

rem Run the Python scraper
python src\\email_spider.py input_data\\clinics.csv -o "%outFile%" -w %workers% %forceFlag%

pause