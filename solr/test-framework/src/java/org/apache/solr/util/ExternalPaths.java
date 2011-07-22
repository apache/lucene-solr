package org.apache.solr.util;

/**
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

import java.io.File;

import org.apache.solr.SolrTestCaseJ4;

/**
 * Some tests need to reach outside the classpath to get certain resources (e.g. the example configuration).
 * This class provides some paths to allow them to do this.
 * @lucene.internal
 */
public class ExternalPaths {
  private static final String SOURCE_HOME = determineSourceHome();
  public static String WEBAPP_HOME = new File(SOURCE_HOME, "webapp/web").getAbsolutePath();
  public static String EXAMPLE_HOME = new File(SOURCE_HOME, "example/solr").getAbsolutePath();
  public static String EXAMPLE_MULTICORE_HOME = new File(SOURCE_HOME, "example/multicore").getAbsolutePath();
  public static String EXAMPLE_SCHEMA=EXAMPLE_HOME+"/conf/schema.xml";
  public static String EXAMPLE_CONFIG=EXAMPLE_HOME+"/conf/solrconfig.xml";
  
  static String determineSourceHome() {
    // ugly, ugly hack to determine the example home without depending on the CWD
    // this is needed for example/multicore tests which reside outside the classpath
    File file;
    try {
      file = new File("solr/conf");
      if (!file.exists()) {
        file = new File(Thread.currentThread().getContextClassLoader().getResource("solr/conf").toURI());
      }
    } catch (Exception e) {
      // If there is no "solr/conf" in the classpath, fall back to searching from the current directory.
      file = new File(".");
    }
    File base = file.getAbsoluteFile();
    while (!new File(base, "solr/CHANGES.txt").exists()) {
      base = base.getParentFile();
    }
    return new File(base, "solr/").getAbsolutePath();
  }
}
