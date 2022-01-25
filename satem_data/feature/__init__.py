DB_SATEM = "satem"
DB_SATEM_TEST = "satem_test"
FEATURES_UNIQUE_COLS = ["facility_id", "date"]
FEATURES_COLLECTION = "features"
DATE_FORMAT = "%Y-%m-%d"

from .feature import get_features
from .feature import insert_features