Email Scraper App - README
============================

Overview:
---------
This Email Scraper App aggregates clinic data from a CSV file and then uses Selenium to navigate provider websites in order to extract email addresses. It searches both the base pages and (if needed) attempts to navigate to a contact page. The output is written as a CSV file with the following columns:
  - clinic_name
  - website
  - address
  - phone
  - email_addresses

Folder Structure:
-----------------
EmailScraperApp\
├── src\
│   └── email_spider.py         # Python script containing the email scraper code.
├── input_data\
│   └── clinics.csv             # CSV file with clinic entries (your input data).
├── output\
│   └── scraped_emails.csv      # Output CSV file containing the scraped email addresses.
└── run.ps1                     # PowerShell script to launch the app.


Obtaining the Input CSV Using Instant Data Scraper:
----------------------------------------------------
To get the input CSV file, follow these steps using the Instant Data Scraper Chrome extension:

1. **Install Instant Data Scraper:**
   - Open Chrome and install the [Instant Data Scraper](https://chrome.google.com/webstore/detail/instant-data-scraper/oolgaifhlefebgkikmcichifhlfkhncc) extension if you haven't already.

2. **Navigate to Google Maps:**
   - Open [Google Maps](https://maps.google.com) in Chrome.
   - In the search bar, type **"mental health edina"** and press Enter.

3. **Activate Instant Data Scraper:**
   - Click on the Instant Data Scraper icon in your Chrome toolbar.
   - If the first table displayed is not showing the listings on the right side (the clinic listings), click the **"Try another table"** button repeatedly until the extension selects the correct table containing the listings.

4. **Adjust Infinite Scrolling (if necessary):**
   - Make sure that the extension is set to "Infinite Scroll" so that it can load all available listings.
   - If required, scroll down manually to help trigger loading of additional results.

5. **Start Crawling:**
   - Once the correct table is displayed with all the clinic listings, click **"Start Crawling"** in the Instant Data Scraper interface.  
     Allow the extension to load all listings.

6. **Download the CSV:**
   - When the crawl is complete, click the **"Download CSV"** button.  
     Save the CSV file as `clinics.csv`.

7. **Place the CSV File:**
   - Move or copy the downloaded CSV file into the **input_data** folder of the EmailScraperApp project, renaming it to `clinics.csv` if it isn’t already named that.

Using the App:
--------------
1. **Prepare the Input File:**
   - Confirm that your CSV file (`input_data\clinics.csv`) contains at least 14 columns, with the provider website expected in column 14 (index 13).  
   - Edit the CSV if necessary.

2. **Run the App:**
   - To launch the application, simply double-click the `run.ps1` script if you create a shortcut, or run it directly in PowerShell.
   - Alternatively, create a batch file (`run.bat`) with the following content to run by double-clicking:
   
       @echo off
       powershell.exe -ExecutionPolicy Bypass -File "%~dp0\run.ps1"
       pause

3. **Output:**
   - After the script completes, check the **output** folder for `scraped_emails.csv`.  
   - Open the CSV with your favorite spreadsheet application or text editor to review the results.

Troubleshooting:
----------------
- **No Emails Found or False Positives:**  
  - Verify that the target provider websites display email addresses in a plain text format. Many sites may use contact forms or obfuscate email addresses.
  - Review the PowerShell console output for detailed log messages. The script logs every page loaded, including the length of the HTML, any emails found via regex or `mailto:` links, and whether it attempted to navigate to a contact page.
  - You may need to adjust the regex (in `src\email_spider.py`) to filter out false positives such as image filenames.
  
- **Selenium / Driver Issues:**  
  - Ensure that ChromeDriver is in your PATH and that its version matches your installed Chrome browser.
  - Increase the sleep delays in the script if pages are taking longer to load.

- **CSV Structure Issues:**  
  - Make sure that the input CSV has at least 14 columns. The provider website must be in column 14 (0-indexed 13), or adjust the script accordingly.

Additional Modifications:
-------------------------
- You can modify the Python scraper (located at `src\email_spider.py`) to adjust crawl depth, wait times, or extraction techniques.
- The script currently tries to click any link that contains the word "contact" if no emails are found on the base page. You may want to improve this logic based on your target sites.

Contact:
--------
For further assistance, review the code comments in `src\email_spider.py` or refer to this README.

Enjoy using your Email Scraper App!
