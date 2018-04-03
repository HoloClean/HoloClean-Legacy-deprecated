import click
from pyspark.sql import SparkSession
from transformer import Transformer
from transform_functions import lowercase, trim
from normalizer import Normalizer
from col_norm_info import ColNormInfo
import distance


@click.command()
@click.argument('path')
@click.argument('out_path')
def wrangle(path, out_path):

    spark = SparkSession.builder.getOrCreate()

    data = spark.read.csv(path, header=True, encoding='utf-8')

    functions = [lowercase, trim]

    # food cols
    #columns = ["city", "facilitytype"]
    columns = data.columns

    # hospital cols
    columns = data.columns
    #columns = ["val"]
    #columns = ["correct_value"]

    transformer = Transformer(functions, columns)

    data = transformer.transform(data)

    # food cols
    cols_info = list()
    cols_info.append(ColNormInfo("city"))
    cols_info.append(ColNormInfo("facilitytype", distance.jaccard, 0.3))

    # hospital cols
    #for col in data.columns:
    #    cols_info.append(ColNormInfo(col))

    normalizer = Normalizer(cols_info)

    data = normalizer.normalize(data)

    data.toPandas().to_csv(out_path, index=False, header=True)


if __name__ == '__main__':
    wrangle()
