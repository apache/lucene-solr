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

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;

public class HdfsUtil {
  
  // Allows tests to easily add additional conf
  public static Configuration TEST_CONF = null;
  
  private static final String[] HADOOP_CONF_FILES = {"core-site.xml",
    "hdfs-site.xml", "mapred-site.xml", "yarn-site.xml", "hadoop-site.xml"};
  
  public static void addHdfsResources(Configuration conf, String confDir) {

    if (confDir != null && confDir.length() > 0) {
      File confDirFile = new File(confDir);
      if (!confDirFile.exists()) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Resource directory does not exist: " + confDirFile.getAbsolutePath());
      }
      if (!confDirFile.isDirectory()) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Specified resource directory is not a directory" + confDirFile.getAbsolutePath());
      }
      if (!confDirFile.canRead()) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Resource directory must be readable by the Solr process: " + confDirFile.getAbsolutePath());
      }
      for (String file : HADOOP_CONF_FILES) {
        if (new File(confDirFile, file).exists()) {
          conf.addResource(new Path(confDir, file));
        }
      }
    }
    
    if (TEST_CONF != null) {
      conf.addResource(TEST_CONF);
    }
  }
}
