# MEI Scraper Application

This project is designed to scrape MEI (Microempreendedor Individual) data from the Receita Federal website using Python. It utilizes web scraping techniques to gather information based on CNPJ numbers provided in a CSV file.

## Project Structure

```
mei-scraper-app
├── src
│   ├── main.py          # Entry point of the application
│   ├── utils.py         # Utility functions for scraping
│   └── __init__.py      # Package initialization
├── data
│   ├── MEI_numbers.csv   # Input CNPJ numbers for scraping
│   └── scraped_data
│       └── pdfs          # Directory for storing downloaded PDFs
├── requirements.txt      # Project dependencies
├── README.md             # Project documentation
└── .gitignore            # Git ignore file
```

## Installation

1. Clone the repository:
   ```
   git clone <repository-url>
   cd mei-scraper-app
   ```

2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

## Usage

1. Prepare your `MEI_numbers.csv` file in the `data` directory with the CNPJ numbers you want to scrape.

2. Run the application:
   ```
   python src/main.py
   ```

3. The scraped data will be stored in the `data/scraped_data/pdfs` directory.

## Dependencies

This project requires the following Python packages:
- pandas
- selenium
- pyautogui
- BeautifulSoup4
- requests

Make sure to install these packages using the `requirements.txt` file.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.

## License

This project is licensed under the MIT License. See the LICENSE file for more details.