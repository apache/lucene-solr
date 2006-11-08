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

package org.apache.solr.core;

import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;

import java.io.IOException;
import java.io.InputStream;


/**
 * Provides a static refrence to a Config object modeling the main
 * configuration data for a a Solr instance -- typically found in
 * "solrconfig.xml".
 *
 * @author yonik
 * @version $Id$
 */
public class SolrConfig {

  public static final String DEFAULT_CONF_FILE = "solrconfig.xml";

  /**
   * Singleton containing all configuration.
   */
  public static Config config;

  /**
   * (Re)loads the static configation information from the specified file.
   *
   * <p>
   * This method is called implicitly on ClassLoad, but it may be
   * called explicitly to change the Configuration used for the purpose
   * of testing - in which case it should be called prior to initializing
   * a SolrCore.
   * </p>
   *
   * <p>
   * This method should <b>only</b> be called for testing purposes.
   * Because it modifies a singleton, it is not suitable for running
   * multi-threaded tests.
   * </p>
   *
   * @param file file name to load
   * @see Config#openResource
   */
  public static synchronized void initConfig(String file)
    throws ParserConfigurationException, IOException, SAXException {

    InputStream is = Config.openResource(file);
    config=new Config(file, is, "/config/");
    is.close();
    Config.log.info("Loaded SolrConfig: " + file);
  }
  
  static {
    try {
      initConfig(DEFAULT_CONF_FILE);
    } catch (Exception ee) {
      throw new RuntimeException("Error in " + DEFAULT_CONF_FILE, ee);
    }
  }
}
