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
package org.apache.solr.util;

import com.carrotsearch.randomizedtesting.ThreadFilter;

public class BadHdfsThreadsFilter implements ThreadFilter {

  @Override
  public boolean reject(Thread t) {
    String name = t.getName();
    if (name.startsWith("IPC Parameter Sending Thread ")) { // SOLR-5007
      return true;
    } if (name.startsWith("IPC Client")) { // SOLR-5007
      return true;
    } else if (name.startsWith("org.apache.hadoop.hdfs.PeerCache")) { // SOLR-7288
      return true;
    } else if (name.endsWith("StatisticsDataReferenceCleaner")) {
      return true;
    } else if (name.startsWith("LeaseRenewer")) { // SOLR-7287
      return true;
    } else if (name.startsWith("org.apache.hadoop.fs.FileSystem$Statistics")) { // SOLR-11261
      return true;
    } else if (name.startsWith("ForkJoinPool.")) { // JVM built in pool
      return true;
    } else if (name.startsWith("solr-hdfs-threadpool-")) { // SOLR-9515 and HDFS-14251
      return true;
    }
    
    return false;
  }
}
