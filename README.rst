===========================
|makinage-logo| Maki Nage
===========================

.. |makinage-logo| image:: https://github.com/maki-nage/makinage/raw/master/asset/makinage_logo.png

The Reactive Machine Learning Framework

.. image:: https://badge.fury.io/py/makinage.svg
    :target: https://badge.fury.io/py/makinage

.. image:: https://github.com/maki-nage/makinage/workflows/Python%20package/badge.svg
    :target: https://github.com/maki-nage/makinage/actions?query=workflow%3A%22Python+package%22
    :alt: Github WorkFlows

.. image:: https://github.com/maki-nage/makinage/raw/master/asset/docs_download.svg
    :target: https://www.makinage.org/doc/makinage-book/latest/index.html
    :alt: Documentation


Maki Nage is a Reactive Data Science framework designed to work on streaming data.

This repository contains the Maki Nage CLI tools, used to deploy `RxSci
<https://github.com/maki-nage/rxsci>`_ based applications on a Kafka cluster.

Installation
==============

The Maki Nage CLI tools are available on pypi:

.. code:: console

    pip install makinage


Getting started
===============

An application can be started by providing its manifest file. See the
`documentation <https://www.makinage.org/doc/makinage-book/latest/index.html>`_
for more information.

.. code:: console

    makinage --config myconfig.yaml
