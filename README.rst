===========================
|makinage-logo| Maki Nage
===========================

.. |makinage-logo| image:: https://github.com/maki-nage/makinage/raw/master/asset/makinage_logo.png

Stream Processing Made Easy

.. image:: https://badge.fury.io/py/makinage.svg
    :target: https://badge.fury.io/py/makinage

.. image:: https://github.com/maki-nage/makinage/workflows/Python%20package/badge.svg
    :target: https://github.com/maki-nage/makinage/actions?query=workflow%3A%22Python+package%22
    :alt: Github WorkFlows

.. image:: https://github.com/maki-nage/makinage/raw/master/asset/docs_download.svg
    :target: https://www.makinage.org/doc/makinage-book/latest/index.html
    :alt: Documentation


Maki Nage is a Python stream processing framework for data scientists. It
provides **expressive** and **extensible** APIs, allowing to speed up the
development of stream applications. It can be used to process **stream** and
**batch** data. More than that, it allows to develop an application with batch
data, and deploy it as a **Kafka micro-service**.

`Read the book <https://www.makinage.org/doc/makinage-book/latest/index.html>`_
to learn more.

.. image:: https://github.com/maki-nage/makinage/raw/master/asset/graph.png
    :width: 50%

Main Features
==============

* **Expressive** and **Extensible** APIs: Maki-Nage is based on `ReactiveX <https://github.com/ReactiveX/RxPY>`_.
* Deployment Ready: Maki-Nage uses **Kafka** to scale the workload, and be resilient to errors.
* **Unifies** Batch and Stream processing: The same APIs work on both sources of data.
* Flexible: Start working on your laptop, continue on a server, deploy on a cluster.
* **ML Streaming Serving**: Serve your machine learning model as a Kafka micro-service.

Installation
==============

Maki Nage is available on PyPI:

.. code:: console

    pip install makinage


Getting started
===============

Write your data transforms
---------------------------

.. code:: Python

    import rx
    import rxsci as rs

    def rolling_mean():
        return rx.pipe(            
            rs.data.roll(window=3, stride=3, pipeline=rx.pipe(
                rs.math.mean(reduce=True),
            )),
        )

Test your code on batch data
-------------------------------

You can test your code from any python data or CSV file.

.. code:: Python

    data = [1, 2, 3, 4, 5, 6, 7]

    rx.from_(data).pipe(
        rs.state.with_memory_store(rx.pipe(
            rolling_mean(),
        )),
    ).subscribe(
        on_next=print
    )

.. code:: console

    2.0
    5.0


Deploy your code as a Kafka micro-service
-------------------------------------------

To deploy the code, package it as a function:

.. code:: Python

    def my_app(config, data):
        roll_mean = rx.from_(data).pipe(
            rs.state.with_memory_store(rx.pipe(
                rolling_mean(),
            )),
        )

        return roll_mean,

Create a configuration file:

.. code:: yaml

    application:
        name: my_app
    kafka:
        endpoint: "localhost"
    topics:
        - name: data
        - name: features
    operators:
        compute_features:
            factory: my_app:my_app
            sources:
                - data
            sinks:
                - features

And start it!

.. code:: console

    makinage --config myconfig.yaml


Serve Machine Learning Models
===============================

Maki Nage contains a model serving tool. With it, serving a machine
learning model in streaming mode just requires a configuration file:

.. code:: yaml

    application:
        name: my_model_serving
    Kafka:
        endpoint: "localhost"
    topics:
    - name: data
      encoder: makinage.encoding.json
    - name: model
      encoder: makinage.encoding.none
      start_from: last
    - name: predict
      encoder: makinage.encoding.json
    operators:
      serve:
        factory: makinage.serve:serve
        sources:
          - model
          - data
        sinks:
          - predict
    config:
      serve: {}

And then serving the model it done the same way than any makinage application:

.. code:: console

    makinage --config config.serve.yaml


Some pre and post processing steps are possible if input features or predictions
must be modified before/after the inference:

.. image:: https://github.com/maki-nage/makinage/raw/master/asset/serve.png

`Read the book <https://www.makinage.org/doc/makinage-book/latest/serving.html#>`_
to learn more.


Publications
===============

* Toward Data Science: `Stream Processing Made Easy <https://towardsdatascience.com/stream-processing-made-easy-5f4892736623>`_


License
=========

Maki Nage is publised under the MIT License.