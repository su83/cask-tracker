===========================
Cask Tracker Application
===========================

Introduction
============

The Cask™ Data Application Platform (CDAP) is an integrated, open source application
development platform for the Hadoop ecosystem that provides developers with data and
application abstractions to simplify and accelerate application development.

Tracker
=======

Cask Tracker is one such application built by the team at Cask that provides the ability to track data ingested
either through Cask Hydrator or a Custom CDAP Application and provide input to data governance processes on a cluster.
It includes this data about "the data":
- Metadata
-- Tags, Properties, Schema for CDAP Datasets and Programs
-- System and User
- Data Quality
-- Metadata that include Feed-level and Field-level quality metrics of datasets
- Data Usage Statistics
-- Usage statistics of dataset and programs.

Getting Started
===============

Prerequisites
-------------
To use the 1.0.x version of Tracker, you must have CDAP version 3.4.x.

Audit Publishing to Kafka
----------------------------
The Tracker App contains a Flow that subscribes to the Kafka topic to which CDAP publishes
the audit updates. Hence, before using this application, you should enable publishing of audit updates to
Kafka, as described in the CDAP documentation `Enable Metadata Update Notifications
<http://docs.cask.co/cdap/current/en/developers-manual/building-blocks/metadata-lineage.html#metadata-update-notifications>`__.

Building Cask Tracker
----------------
You get started with Hydrator plugins by building directly from the latest source code::

  git clone https://github.com/caskdata/cask-tracker.git
  cd cask-tracker
  mvn clean package

After the build completes, you will have a JAR in:
``<plugin-name>/target/`` directory.

Deploying Cask Tracker
-----------------
Step 1: Deploy a plugin using the CDAP CLI::

  > load artifact <target/jar>


Step 2: Create an application configuration file that contains:

- Kafka Metadata Config (``metadataKafkaConfig``): Kafka Consumer Flowlet configuration information
  (info about where we can fetch metadata updates)

Sample Application Configuration file::

  {
    "config": {
      "metadataKafkaConfig": {
        "zookeeperString": "hostname:2181/cdap/kafka"
      }
    }
  }

**Metadata Kafka Config:**

This key contains a property map with these properties:

Required Properties:

- ``zookeeperString``: Kafka Zookeeper string that can be used to subscribe to the CDAP metadata updates
- ``brokerString``: Kafka Broker string to which CDAP metadata is published

*Note:* Specify either the ``zookeeperString`` or the ``brokerString``.

Optional Properties:

- ``topic``: Kafka Topic to which CDAP Metadata updates are published; default is ``cdap-metadata-updates`` which
  corresponds to the default topic used in CDAP for Metadata updates
- ``numPartitions``: Number of Kafka partitions; default is set to ``10``
- ``offsetDataset``: Name of the dataset where Kafka offsets are stored; default is ``kafkaOffset``

Step 3: Create a CDAP Application by providing the configuration file::

  > create app TrackerApp tracker 1.0.0-SNAPSHOT USER appconfig.txt

You can build without running tests: ``mvn clean install -DskipTests``

Mailing Lists
-------------
CDAP User Group and Development Discussions:

- `cdap-user@googlegroups.com <https://groups.google.com/d/forum/cdap-user>`__

The *cdap-user* mailing list is primarily for users using the product to develop
applications or building plugins for appplications. You can expect questions from
users, release announcements, and any other discussions that we think will be helpful
to the users.

IRC Channel
-----------
CDAP IRC Channel: #cdap on irc.freenode.net


License and Trademarks
======================

Copyright © 2015-2016 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied. See the License for the specific language governing permissions
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks.
