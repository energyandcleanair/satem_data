DB_SATEM = "satem"
DB_SATEM_TEST = "satem_test"
RESULTS_COLLECTION = "results"
DATE_FORMAT = "%Y-%m-%d"

# SHOULD NOT BE USED ANYMORE
RESULTS_UNIQUE_COLS = ["location_id", "date", "window", "method.id", "crosswind_km", "wind_m_s_threshold", "wind_bin_width_m_s"]

from .crud import *
from .db import *