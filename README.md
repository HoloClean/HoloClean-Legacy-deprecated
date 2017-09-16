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
* _HoloClean: Holistic Data Repairs with Probabilistic Inference_, ([VLDB 2017](https://arxiv.org/pdf/1702.00820.pdf))

## Jupyter Notebook Best Practices

HoloClean is built specifically with usage in **Jupyter/IPython notebooks** in mind; an incomplete set of best practices for the notebooks:

It's usually most convenient to write most code in an external `.py` file, and load as a module that's automatically reloaded; use:
```python
%load_ext autoreload
%autoreload 2
```
A more convenient option is to add these lines to your IPython config file, in `~/.ipython/profile_default/ipython_config.py`:
```
c.InteractiveShellApp.extensions = ['autoreload']     
c.InteractiveShellApp.exec_lines = ['%autoreload 2']
```

