import satemdata
from decouple import config
import os
from satemdata.feature.db import create_feature_index
from satemdata.result.db import create_result_index

import result
import feature

# os.environ["CREA_MONGODB_URL"] = config("CREA_MONGODB_URL")
# from feature import delete_features, get_features
# f = {'tropomi_no2.density_line.density_line_step_km': 10}
# a=get_features(location_id=None, additional_filter=f)
# delete_features(location_id=None, additional_filter=f)

# db = result.get_result_db()
# db.results.update_many({}, {"$rename": {'frequency': 'window'}})

result.enforce_schema()

create_feature_index()
create_result_index()

