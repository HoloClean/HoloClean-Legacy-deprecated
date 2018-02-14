
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

In order to create the database and the user that will be used by Holoclean, from the Holoclean folder run the script:
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

## Installation

This file will go through the steps needed to install the required packages and software to run HoloClean. For a more detailed installation guide check out the [Holoclean_Installation_v3.pdf](https://github.com/HoloClean/HoloClean-v0.01/blob/pytorch/Holoclean_Installation_v3.pdf) file in the git repo.

### 1. Setting Up and Using Conda 
 <b>1.1 Ubuntu: </b>
 <b>For 32 bit machines, run:</b>
 
 ```
 wget https://3230d63b5fc54e62148ec95ac804525aac4b6dba79b00b39d1d3.ssl.cf1.rackcdn.com/Anaconda-2.3.0-Linux-x86.sh
bash Anaconda-2.3.0-Linux-x86.sh
 ```

<b>For 64 bit machines, run:<b>
```
wget https://3230d63b5fc54e62148e-c95ac804525aac4b6dba79b00b39d1d3.ssl.cf1.rackcdn.com/Anaconda-2.3.0-Linux-x86_64.sh
bash Anaconda-2.3.0-Linux-x86_64.sh
```
<h4>1.2 MacOS: <h4>

Follow instructions [here](https://conda.io/docs/user-guide/install/macos.html) to install conda for MacOS

<h4> 1.3 Using Conda </h4>
Open the terminal and create a Python 2.7 environment by running the command:

	conda create -n py27Env python=2.7 anaconda

Then the environment can be activated by running:

	source activate py27Env
Make sure the keep the environment activated for the rest of the installation process

### 2. Download and Install Spark
Download the ``spark-2.2.0-bin-hadoop2.7.tgz`` file from the [spark website](https://spark.apache.org/downloads.html)
Go to the directory where you downloaded the file and run:
```
tar -xzf spark-2.2.0-bin-hadoop2.7.tgz
pip install pyspark
```

### 3. Install MySQL Server
<b> 3.1 For Ubuntu: </b>
update and upgrade your apt-get:
```
sudo apt-get update	
sudo apt-get upgrade
```
Install MySQL by running:
```
sudo apt-get install mysql-server
```
<br>
<b> 3.2 For MacOS </b>

Install and run the MySQL .dmg file for MacOS from https://dev.mysql.com/downloads/mysql/
After the installation is finished, open system preferences and click on the MySQL icon and make sure the MySQL Server Instance is running.

Next run :
```
sudo usr/local/mysql/bin/mysql_secure_installation
```

<b> 3.3 Create MySQL User and Database </b>

Go to the [Holoclean-v0.01/](https://github.com/HoloClean/HoloClean-v0.01/tree/pytorch) directory and run the script:
```
./mysql_script.sh
```

### 4. Installing Required Packages
Again go to the [Holoclean-v0.01/](https://github.com/HoloClean/HoloClean-v0.01/tree/pytorch)  directory and run:
```
pip install python-package-requirement.txt
```
