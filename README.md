# Snowflake Data Loading and Error Management

This project demonstrates how to efficiently load, validate, and manage structured and semi-structured data files (CSV, JSON, XML) from various storage locations (local filesystem, Azure Blob Storage, AWS S3) into Snowflake tables. The solution includes robust error-handling mechanisms to capture and handle bad data during or after the loading process, ensuring data quality.

---

## **Project Objective**
The goal of this project is to:
- Load daily transactional files into Snowflake using stages.
- Transform semi-structured data (JSON, XML) for compatibility with relational target tables.
- Capture, log, and manage bad data or errors during file loading.
- Provide error feedback to the source team by unloading errors as files to external stages.

---

## **Technologies Used**
- **Database**: Snowflake
- **Storage**: AWS S3 (External Stage), Snowflake Internal Stages
- **File Formats**: CSV, JSON, XML
- **Programming Languages**: SQL (for Snowflake DDL/DML)
- **Cloud Integration**: AWS IAM, Snowflake Storage Integrations

---

## **Solution Overview**

1. **File Staging**
   - Files from different sources (local filesystem, Azure Blob Storage, AWS S3) are staged using Snowflake's **permanent named internal stages** and **external stages**.

2. **Data Loading**
   - CSV files are loaded directly into Snowflake tables using the `COPY` command.
   - JSON and XML files are transformed into relational format before loading.

3. **Error Handling**
   - **Post-Load Validation**: Errors encountered after loading are captured using the `VALIDATE` function.
   - **Pre-Load Validation**: Errors in files are identified using `VALIDATION_MODE=RETURN_ALL_ERRORS` during the `COPY` command.
   - Errors are stored in dedicated error tables and unloaded to external stages for feedback.

4. **Error Feedback**
   - Error records are written to files in the external stage using the `GET` command. These files are shared with the source team for corrections.

---

## **Snowflake Features Used**
- **Storage Integration**: Securely access AWS S3 buckets.
- **Stages**: Internal and external stages for file management.
- **File Formats**: Handling CSV file headers, delimiters, and encodings.
- **Validation**: `VALIDATE` function and `VALIDATION_MODE` for identifying errors.
- **Cloning**: Creating error tables using `CTAS` (Create Table As Select).
- **Data Transformation**: Flattening semi-structured data.

---

## **Project Workflow**
### **1. File Staging**
   - External Stage:
     - Create an external stage linked to an S3 bucket using a Snowflake storage integration.
   - Internal Stage:
     - Upload local files to a Snowflake internal stage.

### **2. Data Loading**
   - Use the `COPY` command to load files into target tables.

### **3. Error Management**
   - Post-load errors are captured using `VALIDATE` with `_last` job ID.
   - Pre-load errors are captured using `VALIDATION_MODE`.

### **4. Error Feedback**
   - Errors are stored in error tables and unloaded as files into external stages.

---

## **Example Script**
Refer to the [full SQL script](./Snowflake_Worksheet.sql) for step-by-step implementation, including:
- Creating storage integrations.
- Staging files.
- Loading and validating data.
- Managing errors and providing feedback.

---

## **Setup Instructions**

### **1. Prerequisites**
   - Snowflake account with appropriate roles and permissions.
   - Access to AWS S3 for external stage setup.
   - Source files in CSV, JSON, and XML formats.

### **2. Steps**
   - Clone this repository:
     ```bash
     git clone https://github.com/yourusername/snowflake-data-loading-error-management.git
     ```
   - Execute the SQL script in Snowflake:
     ```sql
     -- Run each section in sequence
     ```
   - Upload sample files to your S3 bucket or Snowflake internal stage.

### **3. Output**
   - Target table with successfully loaded data.
   - Error table with bad data or validation issues.
   - Error feedback files in the external stage.

---

## **Sample Files**
- `emp01262025.csv`
- `sample_json_file.json`
- `sample_xml_file.xml`

---

## **Improvements and Next Steps**
- Automate the process using **Snowflake Tasks** and **Streams** for continuous data loading.
- Implement data quality rules (e.g., null checks, duplicate removal) during the loading process.

---

## **License**
This project is licensed under the MIT License. See the [LICENSE](./LICENSE) file for details.

---

## **Author**
- **Naveen Madala**
- **LinkedIn**: [LinkedIn Profile](https://www.linkedin.com/in/naveen-madala9/)
- **GitHub**: [GitHub Profile](https://github.com/navDataEng)
