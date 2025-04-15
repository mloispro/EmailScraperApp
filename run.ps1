# run.ps1
# This script launches the Email Scraper.
# Folder structure:
#   src\email_spider.py
#   input_data\clinics.csv
#   output\scraped_emails.csv (generated output)
\ = Split-Path -Parent \System.Management.Automation.InvocationInfo.MyCommand.Definition
\ = Join-Path \ "src\email_spider.py"
\     = Join-Path \ "input_data\clinics.csv"
\    = Join-Path \ "output\scraped_emails.csv"

Write-Host "Running the Email Scraper..."
python \ \

if (Test-Path \) {
    Write-Host "Scraping complete. Output saved to: \"
} else {
    Write-Host "Scraping complete, but output file not found."
}

Write-Host "Press any key to exit..."
\System.Management.Automation.Internal.Host.InternalHost.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown") | Out-Null
