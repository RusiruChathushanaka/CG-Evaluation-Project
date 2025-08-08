# CG Evaluation Project

This project is designed for end-to-end data processing, transformation, profiling, and uploading to a Supabase database. It includes scripts for loading raw sales data, transforming it into dimension and fact tables, generating profiling reports, and saving/uploading the results.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Configuration](#configuration)

## Installation

1. **Clone the repository:**

   ```bash
   git clone https://github.com/your-username/evaluation-project.git
   cd evaluation-project
   ```

2. **Create a virtual environment (recommended):**

   ```bash
   python -m venv venv
   # On Unix/macOS:
   source venv/bin/activate
   # On Windows:
   venv\Scripts\activate
   ```

3. **Install the required dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

4. **Set up environment variables:**
   - Create a `.env` file in the project root with your Supabase credentials:
     ```
     SUPABASE_URL=your_supabase_url
     SUPABASE_KEY=your_supabase_key
     ```

## Usage

The main workflow is managed by [`app.py`](app.py):

```bash
python app.py
```

This script will:

- Load the latest sales data CSV from the `data/` directory.
- Generate a profiling report in `reports/`.
- Transform the raw data into dimension and fact tables.
- Upload the tables to Supabase (using credentials from `.env`).
- Save the transformed tables as CSVs in `transformed_data/`.
- Log all steps in the `logs/` directory.

## Project Structure

```
.
├── app.py                  # Main application script
├── requirements.txt        # Python dependencies
├── .env                    # Environment variables (Supabase credentials)
├── readme.md               # Project documentation
├── data/                   # Raw data files (CSV)
├── lib/                    # Core library modules
│   ├── __init__.py
│   ├── data_transform.py   # Data transformation functions
│   ├── file_load.py        # Data loading utilities
│   ├── logger.py           # Logging setup
│   └── supabase_connect.py # Supabase connection & upload logic
├── logs/                   # Application logs
├── reports/                # Data profiling reports
├── transformed_data/       # Output: transformed CSVs
├── photos/                 # Database schema images and other assets
└── scripts/                # Additional scripts (if any)
```

## Configuration

- **Supabase:**  
  Set your Supabase URL and API key in the `.env` file as shown above.
- **Data:**  
  Place your raw sales data CSVs in the `data/` directory. The script will automatically pick the latest file.
- **Logging:**
  - Logs are saved in `logs/` with a timestamped filename.
  - The log level can be configured in `logger.py`.
