from pathlib import Path

DELTA_Y_STEP = 10
MAX_VALUE_DELTA_Y = 600

TABULA_TEMPLATES_DIR = Path("tabula_templates")
NEW_TEMPLATE_FILEPATH = TABULA_TEMPLATES_DIR / "template.json"
FULL_PAGE_TEMPLATE_FILEPATH = TABULA_TEMPLATES_DIR / "full_page_table_template.json"

TABLE_COLUMNS = {
    "Artikel": "string",
    "Menge": "float64",
    "Preis": "float64",
    "Aktion": "float64",
    "Total": "float64",
}
