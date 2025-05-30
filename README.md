# RTV-Technical-Assessment
## Introduction
This assessment evaluates your proficiency in designing and implementing end-to-end data engineering solutions using RTV household survey data. You will demonstrate expertise in data architecture, ETL/ELT processes, data warehousing, and analytical visualization.

## **Technologies Used**
- **Programming Language:** Python 3.12 and greater
- **Database:** PostgreSQL

- **Python Libraries:**
  - `pandas`: For data manipulation and cleaning.
  - `SQLAlchemy`: For database interaction.
  - `numpy`: For numerical computations.
  - `dotenv`: To handle environment variables (API keys, DB credentials).

## Getting Started

1. Clone the repository:
    ```bash
    git clone https://github.com/monicaiyb/RTV-Technical-Assessment.git
    ```

2. Navigate to the project directory:
    ```bash
    cd RTV-Technical-Assessment
    ```
3. Create and activate a virtual environment:
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

4. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

5. Set up the environment variables:
   - Create a `.env` file in the root directory with the following keys:
     ```
    VISUALCROSSING_API_KEY = <your_visualcrossing_api_key>
    AIRVISUAL_API_KEY = <your_airvisual_api_key>
    DATABASE_URL=postgresql://<username>:<password>@<host>:<port>/<dbname>
     ```

6. Set up the PostgreSQL database:
   - Run the SQL script to create the schema:
     ```bash
     psql -U <username> -d <dbname> -f sql/schema.sql
     ```

7. Run the ETL pipeline:
   ```bash
   python src/extract.py
   python src/transform.py
   python src/load.py

   ```
### **3. Running Analysis**
- Explore the data using the provided Jupyter notebook:
  ```bash
  jupyter notebook notebooks/analysis.ipynb
  ``