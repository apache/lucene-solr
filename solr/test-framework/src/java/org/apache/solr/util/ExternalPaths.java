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


/**
 * Some tests need to reach outside the classpath to get certain resources (e.g. the example configuration).
 * This class provides some paths to allow them to do this.
 * @lucene.internal
 */
public class ExternalPaths {

  /**
   * <p>
   * The main directory path for the solr source being built if it can be determined.  If it 
   * can not be determined -- possily because the current context is a client code base 
   * using hte test frameowrk -- then this variable will be null.
   * </p>
   * <p>
   * Note that all other static paths available in this class are derived from the source 
   * home, and if it is null, those paths will just be relative to 'null' and may not be 
   * meaningful.
   */
  public static final String SOURCE_HOME = determineSourceHome();
  /** @see #SOURCE_HOME */
  public static String WEBAPP_HOME = new File(SOURCE_HOME, "webapp/web").getAbsolutePath();
  /** @see #SOURCE_HOME */
  public static String DEFAULT_CONFIGSET =
      new File(SOURCE_HOME, "server/solr/configsets/_default/conf").getAbsolutePath();
  /** @see #SOURCE_HOME */
  public static String TECHPRODUCTS_CONFIGSET =
      new File(SOURCE_HOME, "server/solr/configsets/sample_techproducts_configs/conf").getAbsolutePath();

  /** @see #SOURCE_HOME */
  public static String SERVER_HOME = new File(SOURCE_HOME, "server/solr").getAbsolutePath();

  /**
   * Ugly, ugly hack to determine the example home without depending on the CWD
   * this is needed for example/multicore tests which reside outside the classpath.
   * if the source home can't be determined, this method returns null.
   */
  static String determineSourceHome() {
    try {
      File file;
      try {
        file = new File("solr/conf");
        if (!file.exists()) {
          file = new File(ExternalPaths.class.getClassLoader().getResource("solr/conf").toURI());
        }
      } catch (Exception e) {
        // If there is no "solr/conf" in the classpath, fall back to searching from the current directory.
        file = new File(System.getProperty("tests.src.home", "."));
      }
      File base = file.getAbsoluteFile();
      while (!(new File(base, "solr/CHANGES.txt").exists()) && null != base) {
        base = base.getParentFile();
      }
      return (null == base) ? null : new File(base, "solr/").getAbsolutePath();
    } catch (RuntimeException e) {
      // all bets are off
      return null;
    }
  }
}
