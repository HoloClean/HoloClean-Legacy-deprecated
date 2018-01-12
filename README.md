# HoloClean: Weakly Supervised Data Cleaning
HoloClean over Spark and Tensorflow

**_v0.0.1_**

## Data Cleaning with HoloClean
<p>
Noisy and erroneous data is a major bottleneck in analytics. Data cleaning and repairing account for about <a href="https://www.forbes.com/sites/gilpress/2016/03/23/data-preparation-most-time-consuming-least-enjoyable-data-science-task-survey-says/#259a5d256f63">60% of the work of data scientists</a>. To address this bottleneck, we recently introduced <a href="https://arxiv.org/abs/1702.00820">HoloClean</a>, a semi-automated data repairing framework that relies on statistical learning and inference to repair errors in structured data. In HoloClean, we build upon the paradigm of <a href="http://hazyresearch.github.io/snorkel/blog/weak_supervision.html">weak supervision</a> and demonstrate how to leverage diverse signals, including user-defined heuristic rules (such as <a href="http://dl.acm.org/citation.cfm?id=2511233">generalized data integrity constraints</a>) and external dictionaries, to repair erroneous data.
</p>


<p>HoloClean has three key properties:
</p>
<ul>
<li><p>It is the first holistic data cleaning framework that combines a variety of heterogeneous signals, such as integrity constraints, external knowledge, and quantitative statistics, in a unified framework.</p></li>
<li><p>It is the first data cleaning framework driven by probabilistic inference. Users only need to provide a dataset to be cleaned and describe high-level domain specific signals.</p></li>
<li><p>It can scale to large real-world dirty datasets and perform automatic repairs that are two times more accurate than state-of-the-art methods.</p></li>
</ul>

<p>
More more information read our recent <a href="http://dawn.cs.stanford.edu/2017/05/12/holoclean/">blog post</a>.
</p>

### References
* _HoloClean:Holistic Data Repairs with Probabilistic Inference_, ([VLDB 2017](https://arxiv.org/pdf/1702.00820.pdf))

### Installation

Holoclean uses Python 2.7 and requires [a few python packages](python-package-requirement.txt) which can be installed using conda and `pip`.

### Setting Up Conda

Before the installation we have to download and install [`conda`](https://www.continuum.io/downloads).
If you are running multiple version of Python, you might need to run:
```
conda create -n py27Env python=2.7 anaconda
```
And then run the correct environment:
```
source activate py27Env
```
### Download and install Spark
Visiting the web page

In order to download spark you should first go to the web page:
https://spark.apache.org/downloads.html
And choose the spark release you want to download. For our project, you should choose the options:
1. Choose a Spark release: 2.2.0( JUl 11 2017)
2. Choose a package type: Pre-built for Apache Hadoop 2.7 and later
3. Choose a dowload type: Direct Download

And then you press to download spark-2.2.0-bin-hadoop2.7.tgz

Go the folder where you downloaded the file, extract the file 
```
tar -xzf spark-2.2.0-bin-hadoop2.7.tgz
```
### Setup MysqlServer
In order to install Mysql use the command:
```
sudo apt-get install mysql-server
```

In order to create the database and the user that will be used by Holoclean,from the Holoclean folder run the script:
```
./mysql_script.sh 
```
### Installing dependencies
Install the package requirements:
```
pip install --requirement python-package-requirement.txt

```
### Running
After installing, just run:
```
./run.sh

```
