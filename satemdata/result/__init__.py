DB_SATEM = "satem"
DB_SATEM_TEST = "satem_test"
RESULTS_UNIQUE_COLS = ["location_id", "date", "frequency", "method.id"]
RESULTS_COLLECTION = "results"
DATE_FORMAT = "%Y-%m-%d"

from result.crud import *