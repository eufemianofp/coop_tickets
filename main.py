import logging
import sys
import os
import json
import pandas as pd

from tabula import read_pdf, read_pdf_with_template

from globals import (
    DELTA_Y_STEP,
    MAX_VALUE_DELTA_Y,
    NEW_TEMPLATE_FILEPATH,
    FULL_PAGE_TEMPLATE_FILEPATH,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(message)s", "%m-%d-%Y %H:%M:%S"
)

stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.INFO)
stdout_handler.setFormatter(formatter)

logger.addHandler(stdout_handler)
LOGGING_DOCUMENT_SEPARATOR = (
    "==================================================================="
)


def _create_new_template(delta_y: int) -> None:
    logger.debug(f"Creating new template with delta_y={delta_y}")
    with open(FULL_PAGE_TEMPLATE_FILEPATH) as f:
        full_page_template = json.load(f)
    new_template = full_page_template
    new_template[0]["y2"] = new_template[0]["y2"] - delta_y
    new_template[0]["height"] = new_template[0]["height"] - delta_y
    with open(NEW_TEMPLATE_FILEPATH, "w") as fp:
        json.dump(new_template, fp)


def read_table(filename: str) -> pd.DataFrame:
    logger.info(f"Reading table from file {filename}...")
    df_raw = read_pdf(
        input_path=f"pdfs_to_process/{filename}",
        pages=1,
        multiple_tables=False,
        stream=True,
        pandas_options=dict(
            header=None,
            names=[
                "Artikel",
                "Menge",
                "Preis",
                "Aktion",
                "Total",
                "Zusatz",
                "Letter",
            ],
        ),
        silent=True,
    )
    if not df_raw:
        for delta_y in range(
            DELTA_Y_STEP, MAX_VALUE_DELTA_Y + DELTA_Y_STEP, DELTA_Y_STEP
        ):
            _create_new_template(delta_y)
            df_raw = read_pdf_with_template(
                input_path=f"pdfs_to_process/{filename}",
                template_path=NEW_TEMPLATE_FILEPATH,
                pages=1,
                stream=True,
                silent=True,
            )
            if not df_raw:  # try a new template
                continue
            else:
                df_tmp = df_raw[0]
                if "Total CHF" in df_tmp.values:  # try a new template
                    continue
                else:  # template worked
                    break
    if not df_raw:
        logger.info(f"Could not read any tables from file {filename}")
        return pd.DataFrame()
    else:
        logger.info(f"Successfully read table from file {filename}!")
        return df_raw[0]


def process_df(df_raw: pd.DataFrame) -> pd.DataFrame:
    logger.debug("Processing articles that have been removed partially or entirely...")
    df_grouped = df_raw.groupby(by="Artikel", as_index=False, sort=False).sum(
        numeric_only=True
    )
    df_grouped = df_grouped[df_grouped["Menge"] > 0.0]
    df_grouped = df_grouped.drop(columns=["Preis", "Aktion"])
    df = df_grouped.merge(
        df_raw[["Artikel", "Preis", "Aktion"]].drop_duplicates(),
        on="Artikel",
        how="left",
    )

    logger.debug("Renaming and reordering columns...")
    df = df.rename(
        columns={
            "Preis": "EinzelPreis",
            "Aktion": "AktionPreis",
            "Total": "TotalPreis",
        }
    )
    df["Rabatt"] = (df["EinzelPreis"] - df["AktionPreis"]) * df["Menge"]
    df["Assigned_to"] = None
    df = df[
        [
            "Artikel",
            "Menge",
            "EinzelPreis",
            "AktionPreis",
            "TotalPreis",
            "Rabatt",
            "Assigned_to",
        ]
    ]

    return df


def save_df_as_excel(df: pd.DataFrame, filename_no_extension: str) -> None:
    logger.info(f"Saving table as Excel file...")
    df.to_excel(excel_writer=f"processed/{filename_no_extension}.xlsx", index=False)
    logger.info(f"Saved table as Excel file!")


def main():
    for filename in os.listdir("pdfs_to_process"):
        if filename == ".gitkeep":
            continue

        logger.info(LOGGING_DOCUMENT_SEPARATOR)
        try:
            df = read_table(filename)
            if df.empty:
                continue
        except Exception as e:
            logger.error(e)
            logger.info(LOGGING_DOCUMENT_SEPARATOR)
            continue

        df_processed = process_df(df)
        save_df_as_excel(df_processed, filename_no_extension=filename.split(".")[0])
        logger.info(LOGGING_DOCUMENT_SEPARATOR)


if __name__ == "__main__":
    main()
