import requests
import os
from enum import Enum
from warnings import warn
from typing import Literal
import pandas as pd
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, ExceptionContext, MetaData
from sqlalchemy import Column, String, Integer, Table, text


_TABLE_NAME = "twse"
_TWS_URL = "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2"
_OTC_URL = "https://isin.twse.com.tw/isin/C_public.jsp?strMode=4"
_FUTURE_URL = "https://isin.twse.com.tw/isin/C_public.jsp?strMode=11"


def _get_sql_engine():
    path = os.path.abspath(os.path.dirname(__file__))
    return create_engine(f"sqlite:///{path}/twse_codes.db")


class Models:

    class DataColumns(Enum):
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
    @classmethod
    def sql_table(cls) -> Table:
        metadata = MetaData()
        mc = cls.DataColumns
        return Table(
            "twse",
            metadata,
            Column(mc.SYMBOL.short_name, String, primary_key=True),
            Column(mc.NAME.short_name, String),
            Column(mc.CATEGORY.short_name, String),
            Column(mc.ISIN_CODE.short_name, String),
            Column(mc.DATE_OF_LISTING.short_name, Integer),
            Column(mc.MARKET_TYPE.short_name, String),
            Column(mc.INDUSTRY.short_name, String),
            Column(mc.CFICODE.short_name, String),
            Column(mc.NOTES.short_name, String, nullable=True),
        )


def download_codes(output: bool = False) -> None | pd.DataFrame:
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
    for url in [_TWS_URL, _OTC_URL, _FUTURE_URL]:
        dfx = _crawl_from_url(url)
        df = pd.concat([df, dfx], ignore_index=True)
    engine = _get_sql_engine()
    col = Models.DataColumns
    symbol_col = col.SYMBOL.short_name
    df.sort_values(symbol_col)
    df = df.astype(
        {
            col.SYMBOL.short_name: str,
            col.NAME.short_name: str,
            col.CATEGORY.short_name: str,
            col.ISIN_CODE.short_name: str,
            col.DATE_OF_LISTING.short_name: str,
            col.MARKET_TYPE.short_name: str,
        }
    )
    df.set_index(symbol_col, drop=True, inplace=True)

    if engine:
        with engine.connect() as conn:
            result = df.to_sql(
                _TABLE_NAME,
                conn,
                index=True,
                if_exists="replace",
            )
    if not result:
        raise ConnectionRefusedError("Could not insert data into database")
    else:
        return df


def get(
    category: Literal[
        "STOCK",
        "WARRANT",
        "SPECIAL_STOCK",
        "INNOVATION_BOARD",
        "ETF",
        "ETN",
        "TDR",
        "ASSET_BASED_SECURITIES",
        "REIT",
        "OTC_WARRANT",
        "INDEX",
        "ALL",
    ] = "ALL",
    download: bool = True,
) -> pd.DataFrame:
    """
    Retrieves stock codes from a SQL database or a CSV file. If the database is not accessible or empty,
    it fALLs back to reading the codes from a CSV file.

    Parameters:
        file_path (str): The path to the fALLback CSV file containing stock codes. Defaults to "codes.csv".
        details (bool): Determines whether to return detailed information about each stock code. If False,
                        only the stock symbols are returned. Defaults to True.
        table_name (str): The name of the table in the SQL database from which to retrieve the stock codes.
                          Defaults to "twse".

    Returns:
        pd.Series | pd.DataFrame: Depending on the 'details' parameter, returns either a pandas DataFrame
                                  containing detailed information about each stock code or a pandas Series
                                  containing only the stock symbols.
    """
    if category != "ALL":
        if category is None or not category in Models.CodesCategory._member_names_:
            raise TypeError("Cannot find category.")
        category = getattr(Models.CodesCategory, category)
    codes = None

    def _query():
        engine = _get_sql_engine()
        with engine.connect() as conn:

            where = (
                f"WHERE {Models.DataColumns.CATEGORY.short_name} = '{category.value}'"
                if category != "ALL"
                else ""
            )
            columns = ", ".join(Models.DataColumns.get_columns_short())
            table = f"`{_TABLE_NAME}`"
            query = f"SELECT {columns} FROM {table} {where} ORDER BY {Models.DataColumns.SYMBOL.short_name}"
            return pd.read_sql(query, conn)

    codes = _query()
    if len(codes) == 0:
        codes = download_codes(output=True)

    if codes is None or len(codes) == 0:
        raise FileExistsError("No codes found.")
    return codes


def get_stocks_list() -> pd.Series:
    return get(Models.CodesCategory.STOCK.name).to_list()


def get_all() -> pd.DataFrame:
    return get("ALL")


def _get_stocks_details() -> pd.DataFrame:
    get(Models.CodesCategory.STOCK, cache=True)


def _verify_database(conn) -> bool:
    table = Models.sql_table()
    table.metadata.create_ALL(bind=conn)
    return True


def _crawl_from_url(url: str) -> pd.DataFrame:
    html = requests.get(url)
    if html.status_code != 200:
        raise ConnectionError("Download request failed.")
    soup = BeautifulSoup(html.text, "html.parser")
    table = soup.find("table", attrs={"class": "h4"})
    headings = Models.DataColumns.get_columns_short()
    datasets = []
    category = None
    if url == _FUTURE_URL:
        category = "指數"
        for row in table.find_ALL("tr")[1:]:
            ALL_td = row.find_ALL("td")
            dataset = [td.get_text() for td in ALL_td]
            dataset.insert(3, "")
            dataset.insert(3, "")
            symbol_column = dataset[0].split("　")
            dataset.insert(1, category)
            dataset.insert(0, symbol_column[0])
            dataset[1] = symbol_column[1]
            datasets.append(dataset)
    else:
        for row in table.find_ALL("tr")[1:]:
            ALL_td = row.find_ALL("td")
            if len(ALL_td) <= 1:
                category = ALL_td[0].get_text().strip()
            else:
                dataset = [td.get_text() for td in ALL_td]
                symbol_column = dataset[0].split("　")
                dataset.insert(1, category)
                dataset.insert(0, symbol_column[0].replace(" ", ""))
                dataset[1] = symbol_column[1]
                datasets.append(dataset)
    return pd.DataFrame(datasets, columns=headings)


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(
        prog="codes.py",
        description="""Downloads the latest TWSE stock codes and saves them to a CSV file or to a SQL database""",
    )
    parser.add_argument(
        "-d", "--download", action="store_true", help="Download codes from TWSE"
    )
    parser.add_argument(
        "-g", "--get", action="store_true", help="Get codes from database"
    )

    args = parser.parse_args()

    if args.download:
        ret = download_codes(output=True)
    if args.get:
        ret = get()

    print(ret)


if __name__ == "__main__":
    import argparse

    main()
