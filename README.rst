Robot Framework Tarantool Library
=======================================

|Build Status|

Short Description
-----------------

`Robot Framework`_ library for working with Tarantool DB.

Installation
------------

::

    pip install robotframework-tarantoollibrary


Example
-------

.. code:: robotframework

    *** Settings ***
    Library    tarantoollibrary

    *** Test Cases ***
        ${data_from_trnt}=    Select   space_name=some_space_name   key=0   key_type=NUM 
        Set Test Variable     ${key}   ${data_from_trnt[0][0]} 
        Set Test Variable     ${data_from_field}   ${data_from_trnt[0][1]} 
    *** Settings ***
    
        
License
-------

Apache License 2.0

.. _Robot Framework: http://www.robotframework.org

.. |Build Status| image:: https://travis-ci.org/peterservice-rnd/robotframework-tarantoollibrary.svg?branch=master
   :target: https://travis-ci.org/peterservice-rnd/robotframework-tarantoollibrary