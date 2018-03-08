# HoloClean: Weakly Supervised Data Cleaning
HoloClean over Spark and PyTorch

### Status

[![Build Status](https://travis-ci.org/HoloClean/HoloClean.svg?branch=test)](https://travis-ci.org/HoloClean/HoloClean)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Documentation Status](https://readthedocs.org/projects/holoclean/badge/?version=latest)](http://holoclean.readthedocs.io/en/latest/?badge=latest)


**_v0.1.0_**

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
For more information read our <a href="http://pages.cs.wisc.edu/%7Ethodrek/blog/holoclean.html">blog post</a>.
</p>

### References
* _HoloClean:Holistic Data Repairs with Probabilistic Inference_, ([VLDB 2017](https://arxiv.org/pdf/1702.00820.pdf))


## Building the Docker Image

To build the docker image, run the following command:
```
docker build -t holoclean/holoclean:0.1.0 .
```
## Running the Docker Image
To run the Holoclean container. use the command:
```
docker run --name holoclean -p 8888:8888 -p 3306  holoclean/holoclean:0.1.0 
```
Then Copy/paste the URL provided into your browser to open Jupyter notebook.

When you are finished with Holoclean, close the container and remove it with the following command:
```
docker rm holoclean
```

## Additional commands

In order to enter the Holoclean container, use the following command:
```
docker exec -it holoclean  /bin/bash
```
In order to mount a host directory(e.g. a dataset folder) in the Holoclean container, use the following command to run the Holoclean Docker:
```
docker run --name holoclean -v /host/directory:/datasets -p 8888:8888 -p 3306  holoclean/holoclean:0.1.0 
```

If you wish to enter the mysql of the container from your local machine you should first check the ip of the Holoclean container by typing:
```
docker inspect holoclean
```
And then you can connect by using the following command:
```
mysql -h IPAddress -u holocleanUser -p
```








