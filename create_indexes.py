import satemdata
from decouple import config
import os
from feature.db import create_feature_index
from result.db import create_result_index

create_feature_index()
create_result_index()

