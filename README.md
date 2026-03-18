# Claims Letter Automation Platform

An end-to-end claims automation system that integrates third-party claims data, Azure SQL, and Microsoft Power Platform to generate standardized claim correspondence (e.g., denial, acknowledgment letters).

---

## Architecture

![Architecture](architecture.png)

---

## Overview

This project automates the process of generating claim-related letters for adjusters by integrating data pipelines, database logic, and low-code applications.

Adjusters can input a claim number and select a letter type through a Power App interface. The system retrieves claim and policy data, generates a formatted letter, and allows users to download or send it via email automatically.

---

## Business Problem

Claims adjusters previously had to:
- manually gather claim and policy data
- draft letters individually
- risk inconsistencies and errors

This system was built to:
- standardize claim correspondence
- reduce manual effort
- minimize human error
- improve operational efficiency

---

## Key Features

### Automated Data Pipeline
- Extracts claim and insured data from third-party systems
- Uses PowerShell scripts to schedule and transfer data
- Loads data into Azure SQL database

### Azure SQL Processing Layer
- Stored procedures generate structured claim datasets
- Complex joins across claims, policies, vehicles, and coverage data
- Supports multiple letter types (denial, acknowledgment, etc.)

### Letter Generation Logic
- Dynamically constructs letter content using SQL logic
- Includes:
  - insured information
  - coverage limits
  - claim details
  - policy language

### Power Platform Integration
- Power Apps: frontend interface for adjusters
- Power Automate:
  - triggers stored procedures
  - generates documents
  - sends emails automatically

### End-to-End Automation
- Input: Claim Number + Letter Type
- Output:
  - generated letter
  - downloadable file
  - optional email delivery

---

## Architecture Flow

User (Adjuster)  
↓  
Power Apps (Frontend UI)  
↓  
Power Automate (Workflow Trigger)  
↓  
Azure SQL Stored Procedure  
↓  
Claims & Policy Data (Azure SQL)  
↓  
Generated Letter Output  
↓  
Download / Email Delivery  

---

## Data Pipeline

Third-Party Data Source  
↓  
PowerShell Scheduled Scripts  
↓  
Azure SQL Database  

---

## Example SQL Logic

The system uses complex SQL queries and stored procedures to assemble claim-level datasets.

Example components include:
- claim details (loss date, vehicle, driver)
- policy coverage limits (BI, PD, deductibles)
- insured contact information
- dynamic coverage descriptions based on state rules

(See `/sql/` folder for sample queries)

---

## Repository Structure

claims-letter-automation/
├── scripts/
│   ├── sync_query.ps1
│   └── sync_denial.ps1
├── sql/
│   ├── acknowledgment.sql
│   └── denial.sql
├── README.md

---

## Tech Stack

- PowerShell
- Azure SQL
- SQL Server (T-SQL, Stored Procedures)
- Power Automate
- Power Apps
- Excel / Email integration

---

## Business Impact

- Eliminated manual letter drafting
- Reduced processing time significantly
- Standardized communication across adjusters
- Reduced operational errors
- Improved turnaround time for claim handling

---

## Future Improvements

- Add more letter templates
- Integrate document storage (SharePoint / Blob)
- Add audit logging and tracking
- Expand to additional claim workflows
