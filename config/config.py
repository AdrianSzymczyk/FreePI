from pathlib import Path

# Dictionaries
DEFAULT_DICT = Path(__file__).parent.parent.absolute()
DATA_DICT = Path(DEFAULT_DICT, 'data')
EXTENSIONS_DICT = Path(DEFAULT_DICT, 'extensions')

# Create dictionaries
DATA_DICT.mkdir(parents=True, exist_ok=True)
