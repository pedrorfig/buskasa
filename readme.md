# Buskasa

This repository contains an app that helps future home owners to find the best real estate deals by leveraging the power of AI and analytics.

## Usage

1. Open your preferred browser
2. Go to https://buskasa.streamlit.app/ to access the app
3. Select the filters to adjust your results
4. Happy house hunting

## Local Development

### Prerequisites

- Python 3.8 or higher
- pip (Python package installer)
- Git

### Setup

1. Clone the repository:
```bash
git clone https://github.com/pedrorfig/buskasa.git
cd buskasa
```

2. Create and activate a virtual environment:
```bash
python -m venv .venv
# On Windows
.venv\Scripts\activate
# On macOS/Linux
source .venv/bin/activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
   - Copy the `.env.example` file to `.env`
   - Fill in the required environment variables (API keys, database credentials, etc.)

### Running the App Locally

1. Start the Streamlit app:
```bash
streamlit run streamlit_app.py
```

2. Open your browser and navigate to `http://localhost:8501`

### Updating the Database

To update the property listings database:

1. Make sure your environment variables are properly configured in `.env`

2. Run the ETL pipeline:
```bash
python etl.py
```

The script will:
- Scrape property listings from ZAP Imóveis
- Process and clean the data
- Update the database with new listings
- Perform analysis on the properties
- Flag potential good deals

Note: The ETL process may take some time depending on the number of properties and your internet connection.

## Contributing

Contributions are welcome! If you have any suggestions or improvements, feel free to open an issue or submit a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more information.
