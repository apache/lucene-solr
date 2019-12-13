/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * The classes under this package were copied from Apache Hadoop and modified
 * to avoid certain issues during tests. The copied classes override the
 * Apache Hadoop dependency versions during tests.
 *
 * HttpServer2 class was copied to avoid Jetty 9.4 dependency issues. Since
 * Solr uses Jetty 9.4, Hadoop integration tests needs to use Jetty 9.4 as
 * well. The HttpServer2 class should be removed when Hadoop is upgraded to
 * 3.3.0 due to HADOOP-16152 upgrading Hadoop to Jetty 9.4.
 *
 * The classes BlockPoolSlice (HDFS-14251), DiskChecker, FileUtil, HardLink,
 * NameNodeResourceChecker, and RawLocalFileSystem were copied to avoid
 * issues with running Hadoop integration tests under the Java security
 * manager. Many of these classes use org.apache.hadoop.util.Shell
 * which shells out to try to do common filesystem checks.
 *
 * Overtime these classes should be removed as upstream fixes to Apache
 * Hadoop are made. When the Apache Hadoop dependency is upgraded in
 * Solr, the classes should be compared against that version.
 */
package org.apache.hadoop;

