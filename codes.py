import requests
import os
from enum import Enum
from warnings import warn

from bs4 import BeautifulSoup
import pandas as pd
from dotenv import load_dotenv
import sqlalchemy
from sqlalchemy import Column, String, CHAR, Table, text


load_dotenv()

SQL_USERNAME = os.environ.get("SQL_USERNAME")
SQL_PASSWORD = os.environ.get("SQL_PASSWORD")
SQL_ADDRESS = os.environ.get("SQL_ADDRESS")
SQL_PORT = os.environ.get("SQL_PORT")
SQL_SCHEMA = "mt_symbols"
TABLE_NAME = "twse"

TWS_URL = "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2"
OTC_URL = "https://isin.twse.com.tw/isin/C_public.jsp?strMode=4"
FUTURE_URL = "https://isin.twse.com.tw/isin/C_public.jsp?strMode=11"
engine = sqlalchemy.create_engine(
    f"mariadb+pymysql://{SQL_USERNAME}:{SQL_PASSWORD}@{SQL_ADDRESS}:{SQL_PORT}/?charset=utf8"
)


class Models:

    class CodesData(Enum):
        SYMBOL = "sc", "代號"
        NAME = "cn", "名稱"
        CATEGORY = "ca", "類別"
        ISIN_CODE = "ic", "國際證券辨識號碼(ISIN Code)"
        DATE_OF_LISTING = "dl", "上市日"
        MARKET_TYPE = "ma", "市場別"
        INDUSTRY = "si", "產業別"
        CFICODE = "cc", "CFICode"
        NOTES = "no", "備註"

        @classmethod
        def get_columns_short(cls):
            return [x.value[0] for x in cls.__members__.values()]

        @classmethod
        def get_columns_long(cls):
            return [x.value[1] for x in cls.__members__.values()]

        @property
        def short_name(self) -> str:
            return self.value[0]

    class CodesCategory(Enum):
        STOCK = "股票"
        WARRANT = "上市認購(售)權證"
        SPECIAL_STOCK = "特別股"
        INNOVATION_BOARD = "創新板"
        ETF = "ETF"
        ETN = "ETN"
        TDR = "臺灣存託憑證(TDR)"
        ASSET_BASED_SECURITIES = "受益證券-資產基礎證券"
        REIT = "受益證券-不動產投資信託"
        OTC_WARRANT = "上櫃認購(售)權證"
        INDEX = "指數"

        @property
        def lower_name(self) -> str:
            return self.name.lower()

    # depreciated
    @property
    def sql_table() -> Table:
        mc = Models.CodesData
        sql_table = Table(
            "twse",
            Column(mc.SYMBOL.short_name, CHAR, primary_key=True),
            Column(mc.NAME.short_name, CHAR),
            Column(mc.CATEGORY.short_name, CHAR),
            Column(mc.ISIN_CODE.short_name, CHAR),
            Column(mc.DATE_OF_LISTING.short_name, CHAR),
            Column(mc.MARKET_TYPE.short_name, CHAR),
            Column(mc.INDUSTRY.short_name, CHAR),
            Column(mc.CFICODE.short_name, CHAR),
            Column(mc.NOTES.short_name, CHAR),
            mysql_engine="InnoDB",
            mysql_charset="utf8mb4",
            mysql_key_block_size="1024",
        )


def download_codes(
    to_csv: bool = True,
    to_sql: bool = False,
    file_path: str = "codes.csv",
    output: bool = False,
    table_name: str = "twse",
) -> None | pd.DataFrame:
    """
    Downloads the latest TWSE stock codes and saves them to a CSV file or to a SQL database.

    Parameters:
        to_csv (bool): Whether to save the codes to a CSV file.
        to_sql (bool): Whether to save the codes to a SQL database.
        file_path (str): The path to the CSV file.
        output (bool): Whether to return a data frame with the codes.
        table_name (str): The name of the SQL table.

    Returns:
        None or pd.DataFrame: If output is True, returns a data frame with the codes. Otherwise, returns None.

    """
    df = pd.DataFrame()
    for url in [TWS_URL, OTC_URL, FUTURE_URL]:
        dfx = _crawl_from_url(url)
        df = pd.concat([df, dfx], ignore_index=True)
    if to_csv is True:
        df.to_csv(file_path, index=False)
    if to_sql is True:
        with engine.connect() as conn:
            verify_database(conn, create=True)
            df.to_sql(
                table_name,
                conn,
                schema=SQL_SCHEMA,
                index=False,
                if_exists="replace",
            )
            pk = Models.CodesData.SYMBOL.short_name
            conn.execute(
                text(
                    f"ALTER TABLE `{SQL_SCHEMA}`.`twse` "
                    f"CHANGE COLUMN `{pk}` `{pk}` {String(20)} NOT NULL, "
                    f"ADD PRIMARY KEY (`{pk}`);"
                )
            )
    if output is True:
        return df
    print(df)


def get(
    category: Models.CodesCategory = Models.CodesCategory.STOCK,
    cache: bool = True,
    from_csv: bool = False,
    file_path: str = "codes.csv",
    details: bool = True,
    table_name: str = "twse",
) -> pd.Series | pd.DataFrame:
    """
    Retrieves stock codes from a SQL database or a CSV file. If the database is not accessible or empty,
    it falls back to reading the codes from a CSV file.

    Parameters:
        file_path (str): The path to the fallback CSV file containing stock codes. Defaults to "codes.csv".
        details (bool): Determines whether to return detailed information about each stock code. If False,
                        only the stock symbols are returned. Defaults to True.
        table_name (str): The name of the table in the SQL database from which to retrieve the stock codes.
                          Defaults to "twse".

    Returns:
        pd.Series | pd.DataFrame: Depending on the 'details' parameter, returns either a pandas DataFrame
                                  containing detailed information about each stock code or a pandas Series
                                  containing only the stock symbols.
    """
    codes = None
    cache_path = os.path.join(".", "cache")
    if cache is True:
        cache_file = os.path.join(cache_path, category.lower_name + ".csv")
        if os.path.exists(cache_file):
            codes = pd.read_csv(cache_file)

    if from_csv is False and codes is None:
        try:
            with engine.connect() as conn:
                if verify_database(conn, table_name=table_name):
                    if category is not None:
                        where = f"WHERE {Models.CodesData.CATEGORY.short_name} = '{category.value}'"
                    columns = ", ".join(Models.CodesData.get_columns_short())
                    table = f"`{SQL_SCHEMA}`.`{table_name}`"
                    query = f"SELECT {columns} FROM {table} {where}"
                    print(query)
                    codes = pd.read_sql(query, conn)
                    if len(codes) == 0:
                        warn("No codes found in SQL database.")
        except sqlalchemy.ExceptionContext as e:
            warn("Error connecting to SQL database: " + str(e))

    if codes is None or codes.empty:
        codes = pd.read_csv(file_path)
        codes = (
            codes.where(codes[Models.CodesData.CATEGORY.short_name] == category.value)
            .dropna(subset=Models.CodesData.SYMBOL.short_name)
            .reset_index(drop=True)
        )
    if codes is None or len(codes) == 0:
        raise LookupError("No codes found in CSV file.")

    cache_file = os.path.join(cache_path, category.lower_name + ".csv")
    codes.to_csv(cache_file, index=False)

    if details is False:
        codes = codes[Models.CodesData.SYMBOL.short_name]
    return codes


def verify_database(conn, table_name: str = None, create: bool = False) -> bool:
    if not conn.dialect.has_schema(conn, SQL_SCHEMA):
        print(UserWarning(f"Schema {SQL_SCHEMA} does not exist."))
        if not create:
            return False
        else:
            conn.execute(sqlalchemy.schema.CreateSchema(SQL_SCHEMA))
    if table_name is not None:
        if not conn.dialect.has_table(conn, table_name, schema=SQL_SCHEMA):
            print(UserWarning(f"Table {table_name} does not exist."))
            return False
    return True


def _crawl_from_url(url: str) -> pd.DataFrame:
    html = requests.get(url)
    if html.status_code != 200:
        raise ConnectionError("Download request failed.")
    soup = BeautifulSoup(html.content, "html.parser")
    table = soup.find("table", attrs={"class": "h4"})
    headings = Models.CodesData.get_columns_short()
    datasets = []
    category = None
    if url == FUTURE_URL:
        category = "指數"
        for row in table.find_all("tr")[1:]:
            all_td = row.find_all("td")
            dataset = [td.get_text() for td in all_td]
            dataset.insert(3, "")
            dataset.insert(3, "")
            symbol_column = dataset[0].split("　")
            dataset.insert(1, category)
            dataset.insert(0, symbol_column[0])
            dataset[1] = symbol_column[1]
            datasets.append(dataset)
    else:
        for row in table.find_all("tr")[1:]:
            all_td = row.find_all("td")
            if len(all_td) <= 1:
                category = all_td[0].get_text().strip()
            else:
                dataset = [td.get_text() for td in all_td]

                symbol_column = dataset[0].split("　")
                dataset.insert(1, category)
                dataset.insert(0, symbol_column[0].replace(" ", ""))
                dataset[1] = symbol_column[1]
                datasets.append(dataset)
    return pd.DataFrame(datasets, columns=headings)


def main():

    print(get())


if __name__ == "__main__":
    main()
