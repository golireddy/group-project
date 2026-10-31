from delta.tables import DeltaTable
from pyspark.sql import functions as f
from functools import reduce
from operator import and_, or_

from common_utils.logging import get_logger

logger = get_logger("scd_functions")