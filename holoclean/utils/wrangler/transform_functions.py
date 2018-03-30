from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re


@udf(returnType=StringType())
def lowercase(s):
    if s is None:
        return ''
    if type(s) != str and type(s) != unicode:
        return s
    return s.lower()


@udf(returnType=StringType())
def trim(s):
    if s is None:
        return ''
    if type(s) != str and type(s) != unicode:
        return s
    #print s
    s = s.encode('utf-8', 'replace')
    s = s.lstrip()
    s = s.rstrip()
    s = re.sub(r"\n", '', s)
    s = re.sub(r'"', '', s)
    s = re.sub(r"'", '', s)
    return s
