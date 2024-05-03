# TWSE Codes Downloader

This Python module provides functionality to download, store, and retrieve codes from the Taiwan Stock Exchange (TWSE). It allows you to fetch codes from TWSE's website, save them to a CSV file, and/or store them in a SQL database.

# Requirements
python modules:
- pandas
- requests
- python-dotenv
- sqlalchemy
- mysql
- PyMySQL

# Installation

You can install the required dependencies using pip:

bash
```
pip install -r requirements.txt
```

# Usage


## Environment Variables
Before running the module, make sure to set up your environment variables in a .env file. Here's an example .env file:
```
SQL_USERNAME=your_sql_username
SQL_PASSWORD=your_sql_password
SQL_ADDRESS=your_sql_address
SQL_PORT=your_sql_port
```


## Download Codes
To download codes from TWSE and store them in a SQL database:
python
```
from twse_codes_downloader import download_codes

download_codes(to_sql=True, to_csv=False)
```
This will download the codes from TWSE's website and store them in a SQL database.


## Retrieve Codes
To retrieve codes from a CSV file:
```
from twse_codes_downloader import get

codes = get(details=True)
```

# References
Source URLs
The data is retrieved from the following URLs:

- TWS: https://isin.twse.com.tw/isin/C_public.jsp?strMode=2
- OTC: https://isin.twse.com.tw/isin/C_public.jsp?strMode=4
- FUTURE: https://isin.twse.com.tw/isin/C_public.jsp?strMode=11

# License
Distributed under the MIT License. See `LICENSE.txt` for more information.

# Contact
Tsui Ho Ming - edwin.tsui919@gmail.com

Project Link: https://github.com/fighterming/twse-codes