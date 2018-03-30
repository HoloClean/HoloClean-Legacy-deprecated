from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re


@udf(returnType=StringType())
def lowercase(s):
    if type(s) != str and type(s) != unicode:
        return s
    return str(s.encode('utf-8')).lower()


@udf(returnType=StringType())
def trim(s):
    if type(s) != str and type(s) != unicode:
        return s
    s = str(s)
    s = s.lstrip()
    s = s.rstrip()
    s = re.sub(r"\n", '', s)
    s = re.sub(r'"', '', s)
    s = re.sub(r"'", '', s)
    return s
