import logging
from pathlib import Path
import re
import json

import json5
import pandas as pd
import requests
from bs4 import BeautifulSoup

URL = "https://www.tiobe.com/tiobe-index/"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"

MONTHS = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "SEPT": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}


def extract_table_row(tr):
    out = []
    for i, td in enumerate(tr.find_all("td")):
        if i == 2 and td.img:
            value = td.img["src"].split("/")[-1].split(".")[0]
        elif i == 3:
            continue
        else:
            value = td.text.strip()
        out.append(value)
    return out


def extract_table_head(table):
    part = table.thead
    if not part:
        part = table.colgroup
    if not part:
        return []
    tr = part.find("tr")
    if tr:
        cols = [tx.text.strip() for tx in tr.find_all("th")]
        return cols
    return []


def extract_table_body(table):
    part = table.tbody
    if not part:
        part = table.colgroup
        if part:
            tr_list = part.find_all("tr")[1:]
        else:
            part = table
            if part:
                tr_list = part.find_all("tr", recursive=False)

    if not part:
        return []

    data = [[tx.text.strip() for tx in tr.find_all("td")] for tr in tr_list]

    return data


def parse_top20(element):
    # #1 to #20
    logging.info("Parse top20")
    table = element.find("table", id="top20")
    data = [extract_table_row(tr) for tr in table.tbody.find_all("tr")]
    cols = extract_table_head(table)
    return data, cols


def parse_top50(element):
    # Other programming languages (#21 to #50)
    logging.info("Parse top50")
    table = element.find("table", id="otherPL")
    data = [
        [td.text.strip() for td in tr.find_all("td")]
        for tr in table.tbody.find_all("tr")
    ]
    cols = extract_table_head(table)

    return data, cols


def parse_top100(element, start=50):
    # The Next 50 Programming Languages (#51 to #100)
    logging.info("Parse top100")
    ul = element.ul
    if ul and ul.li:
        more = ul.li.text
        if more:
            top_100 = more.strip().split(", ")
            data = [[str(i), name] for i, name in enumerate(top_100, start + 1)]
            return data
    return []


def parse_lt(element):
    # Very Long Term History
    table = element.find("table", id="VLTH")
    cols = extract_table_head(table)
    rows = extract_table_body(table)
    return rows, cols


def parse_hof(element):
    # Programming Language Hall of Fame
    table = element.find("table", id="PLHoF")
    cols = extract_table_head(table)
    rows = extract_table_body(table)
    return rows, cols


def parse_series(html_text):
    def _fix_date(match):
        year = int(match.group(1))
        month = int(match.group(2)) + 1
        day = int(match.group(3))
        return f'"{year:04}-{month:02d}-{day:02}"'

    data = re.findall(r"series: \[\n*\s*(\{.*\})", html_text)
    if data:
        js_value = re.sub(r"Date\.UTC\((\d{4}), *(\d+), *(\d+)\)", _fix_date, data[0])
        series_data = json5.loads(f"[{js_value}]")
        return series_data
    return None


def get_version(element):
    h1 = element.h1
    if not h1:
        return None
    h1_text = h1.text.strip()
    result = re.findall(r"TIOBE Index for (\w+) (\d{4})", h1_text)
    if not result:
        return None
    month, year = result[0]
    month_abbr = month[:3].upper()
    if month_abbr not in MONTHS:
        logging.warning("Month error = {month_abbr} / {month}")
        return None

    month_num = MONTHS[month_abbr]
    return f"{year}-{month_num:02d}"


def parse_top_all(article):
    data_top20, cols1 = parse_top20(article)
    data_top50, cols2 = parse_top50(article)

    start = len(data_top20 + data_top50)
    data_top100 = parse_top100(article, start)

    cols2[0] = cols1[0]
    cols1[2] = "Trending"  # Change
    data_list = [data_top20, data_top50, data_top100]
    col_list = [cols1, cols2, cols2[:2]]

    df_list = []
    for data, cols in zip(data_list, col_list):
        if data:
            df_data = pd.DataFrame(data, columns=cols)
            df_list.append(df_data)

    if not df_list:
        logging.warning("No data")
        return None
    if len(df_list) == 1:
        df = df_list[0]
    else:
        df = pd.concat(df_list)

    key_col = cols1[3]  # "Programming Language"
    out_cols = [key_col] + [c for c in df.columns if c != key_col]
    df2 = df[out_cols]
    return df2


def download(save_dir, ignore=True):
    logging.info(f"Request {URL}")
    headers = {"User-Agent": USER_AGENT}

    try:
        res = requests.get(URL, headers=headers, timeout=30)
        res.raise_for_status()
    except Exception as e:
        logging.error(f"Exception = {e}")
        return

    soup = BeautifulSoup(res.text, "html.parser")
    version = get_version(soup)
    if not version:
        return

    year = version.split("-")[0]
    save_file = Path(save_dir, f"{year}/{version}.tsv")
    logging.info(f"Save to {save_file}")
    if ignore and save_file.exists():
        logging.warning("File exists, ignore")
        return

    if not save_file.parent.exists():
        save_file.parent.mkdir(parents=True)
    article = soup.article
    df = parse_top_all(article)
    if df is not None:
        logging.info(f"Save {save_file}, data = {len(df)}")
        df.to_csv(save_file, sep="\t", index=False, na_rep="")

    rows_lt, cols_lt = parse_lt(article)
    if rows_lt:
        df_lt = pd.DataFrame(rows_lt, columns=cols_lt)
        save_file2 = Path(save_dir, f"{year}/{version}-lt.tsv")
        logging.info(f"Save {save_file2}, data = {len(df_lt)}")
        df_lt.to_csv(save_file2, sep="\t", index=False, na_rep="")

    rows_hof, cols_hof = parse_hof(article)
    if rows_hof:
        df_hof = pd.DataFrame(rows_hof, columns=cols_hof)
        save_file3 = "PLHoF.tsv"
        logging.info(f"Save {save_file3}, data = {len(df_hof)}")
        df_hof.to_csv(save_file3, sep="\t", index=False, na_rep="")

    series_data = parse_series(res.text)
    if series_data:
        save_file4 = Path(save_dir, f"{year}/{version}-top.json")
        logging.info(f"Save {save_file4}, data = {len(series_data)}")
        with open(save_file4, "w") as f:
            json5.dump(
                series_data,
                f,
                indent=2,
                ensure_ascii=False,
                quote_keys=True,
                trailing_commas=False,
            )

    logging.info("done")


if __name__ == "__main__":
    fmt = "%(asctime)s %(levelname)s %(message)s"
    logging.basicConfig(level=logging.INFO, format=fmt)

    download(".")
