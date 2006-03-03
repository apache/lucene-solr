/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.io.InputStream;


/**
 * @author yonik
 * @version $Id$
 */
public class SolrConfig {
  public static Config config;
  static {
    Exception e=null;
    String file="solrconfig.xml";
    InputStream is=null;
    try {
      is = Config.openResource(file);
    } catch (Exception ee) {
      e=ee;
      file = "solarconfig.xml"; // backward compat
      try {
        is = Config.openResource(file);
      } catch (Exception eee) {}
    }
    if (is!=null) {
      try {
        config=new Config(file, is, "/config/");
        is.close();
      } catch (Exception ee) {
        throw new RuntimeException(ee);
      }
      Config.log.info("Loaded Config solrconfig.xml");
    } else {
      throw new RuntimeException("Can't find Solr config file ./conf/solrconfig.xml",e);
    }
  }
}
