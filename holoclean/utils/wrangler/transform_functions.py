from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re
import unicodedata


@udf(returnType=StringType())
def lowercase(s):
    """
    Changes string to lower case

    :param s: input string

    :return: string
    """
    if s is None:
        return ''
    if type(s) != str and type(s) != unicode:
        return s
    return \
        str(unicodedata.normalize('NFKD', s).encode('ASCII', 'ignore')).lower()


@udf(returnType=StringType())
def trim(s):
    """
    Removing spaces

    :param s: input string

    :return: string
    """
    if s is None:
        return ''
    if type(s) != str and type(s) != unicode:
        return s

    if not isinstance(s, str):
        s = unicodedata.normalize('NFKD', s).encode('ASCII', 'ignore')

    if s.isspace():
        return ''
    s = s.lstrip()
    s = s.rstrip()
    s = re.sub("(\n)+", '', s)
    s = re.sub("(\s)+", ' ', s)
    s = re.sub("(\t)+", '', s)
    s = re.sub(r'"', '', s)
    s = re.sub(r"'", '', s)
    return s
