# Kerala Local Body Election Data Scraper (2015–2025)

This repository contains comprehensive datasets and scraping scripts for the Local Body Elections in Kerala, India, covering the years **2015, 2020, and 2025**. 

The project automates the extraction of election results from the State Election Commission's official results portals, consolidating data across all 14 districts into a unified, analyzable format.

**Target Website**: https://sec.kerala.gov.in/public/onser/trend

## 📂 Dataset Overview

The repository hosts the complete election results with over **67,000+ rows** of data.

| Year | Total Records | Status | Description |
| :--- | :--- | :--- | :--- |
| **2015** | 21,823 | ✅ Complete | Full coverage of all 14 districts. |
| **2020** | 21,783 | ✅ Complete | Full coverage of all 14 districts. |
| **2025** | 23,531 | ✅ Complete | Full coverage including newly delimited wards. |
| **MASTER**| **67,137** | **Verified** | Combined dataset for longitudinal analysis. |

**Data Fields Include:**
- `Year`, `District`, `Local Body Type` (Grama, Block, District Panchayat, Municipality, Corporation)
- `Local Body Name`, `Ward Number`, `Ward Name`
- `Candidate Name` (in Malayalam), `Party`, `Front` (LDF, UDF, NDA, Others), `Votes`

## 🚀 Features & Methodology

### 1. Robust Scraping Logic
- **Dynamic Payload Handling:** The scripts automatically adapt request payloads based on the server's response structure (handling differences between 2015 JSON structures and 2025's modern API).
- **Hybrid Urban/Rural Scraper:** Uses distinct logic for Panchayats vs. Municipalities/Corporations to ensure zero data loss in complex urban bodies.
- **UTF-8 Encoding Support:** Properly handles Malayalam character encoding for candidate and ward names.

### 2. Performance Optimization
- **Parallel Execution for 2015:** To handle the slower legacy server response times for the 2015 data, the scraping process was split into two parallel stages:
    - **Part 1:** Thiruvananthapuram (01) to Palakkad (09)
    - **Part 2:** Malappuram (10) to Kasaragod (14)
- This strategy significantly reduced the total execution time.

## 📁 Repository Structure

```text
├── Results/
│   ├── Kerala_LocalBodies_Election_2015_MASTER.csv
│   ├── Kerala_LocalBodies_Election_2020_MASTER.csv
│   ├── Kerala_LocalBodies_Election_2025_MASTER.csv
│   └── Kerala_Election_Complete_Master.csv  <-- (The Golden Dataset)
│
├── Source_Code/
│   ├── 2015_Scapping_Code-Part1.ipynb   # Scripts for South/Central Kerala
│   ├── 2015_Scapping_Code-Part2.ipynb   # Scripts for North Kerala
│   ├── 2020_Scapping_Code.ipynb         # Hybrid scraper for 2020
│   └── 2025_Scapping_code.ipynb         # Universal scraper for 2025
│
├── Sample_100rows_Kerala_LocalBodies_Election.csv    # Sample scapped data ( 100 rows )
│
├── Trail and Error.ipynb                             # Code used to identify website structure
│
└── requirements.txt
