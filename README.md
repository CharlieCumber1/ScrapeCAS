# Webscrape of CAS Explorer

A simple script for scraping CAS Explorer data tables into CSV files formated for import into Amundsen Data Explorer. This project uses Selenium WebDriver and Firefox web browser.

## Setup Required

1. Download and install Firefox from [`here`](https://www.mozilla.org/en-US/firefox/download/)
2. Download Geckodriver from [`here`](https://github.com/mozilla/geckodriver/releases) and add the executable file to the root of the project. Ensure you have selected a [`compatable version`]('https://firefox-source-docs.mozilla.org/testing/geckodriver/Support.html') relative to your Firefox install.
3. Install Selenium with the following command:
``` Bash
$ pip install selenium
```

## Running the Script

Run the script with the following command:

```Bash
$ python scrape.py
```