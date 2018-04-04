# HoloClean: Weakly Supervised Data Cleaning
HoloClean over Spark and PyTorch

### Status

[![Build Status](https://travis-ci.org/HoloClean/HoloClean.svg?branch=test)](https://travis-ci.org/HoloClean/HoloClean)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Documentation Status](https://readthedocs.org/projects/holoclean/badge/?version=latest)](http://holoclean.readthedocs.io/en/latest/?badge=latest)
[![Maintainability](https://api.codeclimate.com/v1/badges/d355e754057907628593/maintainability)](https://codeclimate.com/github/HoloClean/HoloClean/maintainability)


**_v0.1.0_**

## Data Cleaning with HoloClean
<p>
Noisy and erroneous data is a major bottleneck in analytics. Data cleaning and repairing account for about <a href="https://www.forbes.com/sites/gilpress/2016/03/23/data-preparation-most-time-consuming-least-enjoyable-data-science-task-survey-says/#259a5d256f63">60% of the work of data scientists</a>. To address this bottleneck, we recently introduced <a href="https://arxiv.org/abs/1702.00820">HoloClean</a>, a semi-automated data repairing framework that relies on statistical learning and inference to repair errors in structured data. In HoloClean, we build upon the paradigm of <a href="http://hazyresearch.github.io/snorkel/blog/weak_supervision.html">weak supervision</a> and demonstrate how to leverage diverse signals, including user-defined heuristic rules (such as <a href="http://dl.acm.org/citation.cfm?id=2511233">generalized data integrity constraints</a>) and external dictionaries, to repair erroneous data.
<br>
<br>
HoloClean is a statistical inference engine to impute, clean, and enrich data. As a weakly supervised machine learning system, HoloClean leverages available quality rules, value correlations, reference data, and multiple other signals to build a probabilistic model that accurately captures the data generation process, and uses the model in a variety of data curation tasks. HoloClean allows data practitioners and scientists to save the enormous time they spend in building piecemeal cleaning solutions, and instead, effectively communicate their domain knowledge in a declarative way to enable accurate analytics, predictions, and insights form noisy, incomplete, and erroneous data.
</p>


<p>HoloClean has three key properties:
</p>
<ul>
<li><p>It is the first holistic data cleaning framework that combines a variety of heterogeneous signals, such as integrity constraints, external knowledge, and quantitative statistics, in a unified framework.</p></li>
<li><p>It is the first data cleaning framework driven by probabilistic inference. Users only need to provide a dataset to be cleaned and describe high-level domain specific signals.</p></li>
<li><p>It can scale to large real-world dirty datasets and perform automatic repairs that are two times more accurate than state-of-the-art methods.</p></li>
</ul>

<p>
For more information read our <a href="https://holoclean.github.io/gh-pages/blog/holoclean.html">blog post</a>.
</p>

### References
* _HoloClean:Holistic Data Repairs with Probabilistic Inference_, ([VLDB 2017](https://arxiv.org/pdf/1702.00820.pdf))


## Installation

This file will go through the steps needed to install the required packages and software to run HoloClean. For a more detailed installation guide check out the [Holoclean_Installation_v3.pdf](docs/Holoclean_Installation_v3.pdf) file in the git repo.

### 1. Setting Up and Using Conda 
 <b>1.1 Ubuntu: </b>
 <b>For 32 bit machines, run:</b>
 
 ```
 wget  wget https://repo.continuum.io/archive/Anaconda-2.3.0-Linux-x86.sh
 bash Anaconda-2.3.0-Linux-x86.sh
 ```

<b>For 64 bit machines, run: </b>
```
wget https://3230d63b5fc54e62148e-c95ac804525aac4b6dba79b00b39d1d3.ssl.cf1.rackcdn.com/Anaconda-2.3.0-Linux-x86_64.sh
bash Anaconda-2.3.0-Linux-x86_64.sh
```
<h4>1.2 MacOS: <h4>

Follow instructions [here](https://conda.io/docs/user-guide/install/macos.html) to install Anaconda (Not miniconda) for MacOS

<h4> 1.3 Using Conda </h4>
Open/Restart the terminal and create a Python 2.7 environment by running the command:

	conda create -n py27Env python=2.7 anaconda

Then the environment can be activated by running:

	source activate py27Env
<b> Make sure to keep the environment activated for the rest of the installation process </b>

### 2. Install Postgresql
<b> 2.1 Ubuntu Installation: </b>

Install Postgres by running:
```
sudo apt-get install postgresql postgresql-contrib
```
<br>
<b> 2.2 Using Postgres on Ubuntu </b>

To start postgres run:
```
sudo -u postgres psql
```
<b> 2.3 Mac Installation </b>
<br>
Check out the following page to install Postgres for MacOS
<br>
https://www.postgresql.org/download/macosx/
<br>
<b> 2.4 Setup Postgres for Holoclean </b>

Create the database and user by running the following on the Postgres console:
```
CREATE database holo;
CREATE user holocleanuser;
ALTER USER holocleanuser WITH PASSWORD 'abcd1234';
GRANT ALL PRIVILEGES on database holo to holocleanUser ;
```
To Connect to the holo database run:
```
\c holo
```
HoloClean currently appends new tables to the database holo with each instance that is ran.
To clear the database, open MySql with holocleanUser and run:
```
drop database holo;
create database holo;
```

Or alternatively use the function <b>reset_database()</b> function in the Session class in holoclean/holoclean.py




### 3. Installing Pytorch

Follow instructions for your OS at:
http://pytorch.org/
To install pytorch
<br>
make sure to use Python 2.7 for installation (the other settings can be left as default)
<br>
Make sure to install <b>version 0.3.0</b> or later
<br>

### 4. Installing Required Packages
Again go to the repo's root directory directory and run:
```
pip install -r python-package-requirement.txt
```



### 5. Install JDK 8
<b> 5.1 For Ubuntu: </b>
<br>
Check if you have JDK 8 installed by running
```
java -version
```
If you do not have JDK 8, run the following command: 
```
sudo apt-get install openjdk-8-jre
```
<br>
<b> 5.2 For MacOS </b>
<br>
Check if you have JDK 8 by running

	/usr/libexec/java_home -V

<br>
If you do not have JDK 8, download and install JDK 8 for MacOS from the oracle website: http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html

### 6. Getting Started
To get started, the following tutorials in the tutorial directory will get you familiar with the HoloClean framework
<br>
To run the tutorials in Jupyter Notebook go to the root directory in the terminal and run
```
./start_notebook.sh
```
[Data Loading & Denial Constraints Tutorial](tutorial/Tutorial_1.ipynb)
<br>
[Complete Pipeline](tutorial/Tutorial_2.ipynb)
<br>
[Error Detection](tutorial/Tutorial_3.ipynb)
<br>

