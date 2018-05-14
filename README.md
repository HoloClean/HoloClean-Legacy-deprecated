# HoloClean: A Machine Learning System for Data Enrichment
<a href="http://www.holoclean.io"> HoloClean </a>  is built over Spark and PyTorch.

### Status

[![Build Status](https://travis-ci.org/HoloClean/HoloClean.svg?branch=test)](https://travis-ci.org/HoloClean/HoloClean)
[![Documentation Status](https://readthedocs.org/projects/holoclean/badge/?version=latest)](http://holoclean.readthedocs.io/en/latest/?badge=latest)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

**_v0.1.1_**

<p>
<br>
HoloClean is a statistical inference engine to impute, clean, and enrich data. As a weakly supervised machine learning system, HoloClean leverages available quality rules, value correlations, reference data, and multiple other signals to build a probabilistic model that accurately captures the data generation process, and uses the model in a variety of data curation tasks. HoloClean allows data practitioners and scientists to save the enormous time they spend in building piecemeal cleaning solutions, and instead, effectively communicate their domain knowledge in a declarative way to enable accurate analytics, predictions, and insights form noisy, incomplete, and erroneous data.
</p>



## Installation

This file will go through the steps needed to install the required packages and software to run HoloClean.

### 1. Install Postgresql
<b> 1.1 Ubuntu Installation: </b>

Install Postgres by running:
```
sudo apt-get install postgresql postgresql-contrib
```
<br>
<b> 1.2 Using Postgres on Ubuntu </b>

To start postgres run:
```
sudo -u postgres psql
```
<b> 1.3 Mac Installation </b>
<br>
Check out the following page to install Postgres for MacOS
<br>
https://www.postgresql.org/download/macosx/
<br>
<b> 1.4 Setup Postgres for Holoclean </b>

Create the database and user by running the following on the Postgres console:
```
CREATE database holo;
CREATE user holocleanuser;
ALTER USER holocleanuser WITH PASSWORD 'abcd1234';
GRANT ALL PRIVILEGES on database holo to holocleanUser ;
\c holo
ALTER SCHEMA public OWNER TO holocleanUser;
```
In general, to connect to the holo database run:
```
\c holo
```
HoloClean currently appends new tables to the database holo with each instance that is ran.
To clear the database, open PSQL with holocleanUser and run:
```
drop database holo;
create database holo;
```

Or alternatively use the function <b>reset_database()</b> function in the Holoclean class in holoclean/holoclean.py


### 2. Install HoloClean Using Conda 

#### 2.1. Install Conda

##### 2.1.1 Ubuntu:
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
##### 2.1.2 MacOS:

Follow instructions [here](https://conda.io/docs/user-guide/install/macos.html) to install Anaconda (Not miniconda) for MacOS

#### 2.2 Create new Conda environment
Open/Restart the terminal and create a Python 2.7 environment by running the command:

	conda create -n py27Env python=2.7 anaconda

Then the environment can be activated by running:

	source activate py27Env
<b> Make sure to keep the environment activated for the rest of the installation process </b>


### 3. Install HoloClean Using Virtualenv

If you are already familiar with Virtualenv please create a new environment with **Python 2.7** with your preferred virtualenv wrapper, e.g.:

- [virtualenvwrapper](https://virtualenvwrapper.readthedocs.io/en/latest/) (Bourne-shells)
- [virtualfish](https://virtualfish.readthedocs.io/en/latest/) (fish-shell)

Otherwise, continue with the instructions on installing Virtualenv below.

#### 3.1 Install Virtualenv

Install `Virtualenv` following the instructions from [their homepage](https://virtualenv.pypa.io/en/stable/installation/).
For example install globally via pip:

    $ [sudo] pip install virtualenv

#### 3.2 Create a new Virtualenv environment

Create a new directory for your virtual environment with Python 2.7:

    $ virtualenv --python=python2.7 py27Env

Where `py27Env` is a folder, where all virtual environments will be stored and `python2.7` is a valid python executable.
Activate the new `py27Env` envrionment with:

    $ source bin/activate

<b> Make sure to keep the environment activated for the rest of the installation process </b>


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


### 6. Install  Spark (MacOS only)

To install Spark on MacOS run

```
brew install apache-spark
```

After installation of spark, add a `SPARK_HOME` environment variable to your shell, and add `/usr/local/Cellar/apache-spark/<version>/libexec/python` to 
your python path.

### 7. Getting Started

To get started, the following tutorials in the tutorial directory will get you familiar with the HoloClean framework
<br>
To run the tutorials in Jupyter Notebook go to the root directory in the terminal and run
```
./start_notebook.sh
```
[Data Loading & Denial Constraints Tutorial](tutorials/Tutorial_1.ipynb)
<br>
[Complete Pipeline](tutorials/Tutorial_2.ipynb)
<br>
[Error Detection](tutorials/Tutorial_3.ipynb)
<br>


## Developing

### Installation
Follow the steps from Installation to configure your development environment.

### Running Unit Tests
To run unit tests
```
$ cd tests/unit_tests
$ python unittest_dcfeaturizer.py 
2018-04-05 15:15:22 WARN  Utils:66 - Your hostname, apollo resolves to a loopback address: 127.0.1.1; using 192.168.0.66 instead (on interface wlan0)
2018-04-05 15:15:22 WARN  Utils:66 - Set SPARK_LOCAL_IP if you need to bind to another address
2018-04-05 15:15:23 WARN  NativeCodeLoader:62 - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Time to Load Data: 11.9292609692

Time for Error Detection: 14.7168970108

.
----------------------------------------------------------------------
Ran 1 test in 28.680s

OK
$
$ python unittest_sql_dcerrordetector.py 
2018-04-05 15:16:28 WARN  Utils:66 - Your hostname, apollo resolves to a loopback address: 127.0.1.1; using 192.168.0.66 instead (on interface wlan0)
2018-04-05 15:16:28 WARN  Utils:66 - Set SPARK_LOCAL_IP if you need to bind to another address
2018-04-05 15:16:29 WARN  NativeCodeLoader:62 - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Time to Load Data: 12.6399619579

Time for Error Detection: 14.1602239609

.Time to Load Data: 1.38744020462

Time for Error Detection: 8.26235389709

.Time to Load Data: 0.998204946518

Time for Error Detection: 8.1832909584

.Time to Load Data: 1.46859908104

Time for Error Detection: 6.7251560688

.
----------------------------------------------------------------------
Ran 4 tests in 62.365s

OK

```

### Running Integration Tests
To run integration tests
```
cd tests
python test.py
```
Successful tests run looks like:
```
<output>
Time for Test Featurization: 3.3679060936

Time for Inference: 0.249126911163

The multiple-values precision that we have is :0.998899284535
The multiple-values recall that we have is :0.972972972973 out of 185
The single-value precision that we have is :1.0
The single-value recall that we have is :1.0 out of 0
The precision that we have is :0.999022801303
The recall that we have is :0.972972972973 out of 185
Execution finished
```
