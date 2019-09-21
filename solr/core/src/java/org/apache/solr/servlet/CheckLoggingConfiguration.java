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
package org.apache.solr.servlet;

import java.lang.invoke.MethodHandles;

import org.slf4j.LoggerFactory;

final class CheckLoggingConfiguration {
  
  static void check() {
    try {
      LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    } catch (NoClassDefFoundError e) {
      throw new NoClassDefFoundError("Failed to initialize Apache Solr: "
          +"Could not find necessary SLF4j logging jars. If using Jetty, the SLF4j logging jars need to go in "
          +"the jetty lib/ext directory. For other containers, the corresponding directory should be used. "
          +"For more information, see: http://wiki.apache.org/solr/SolrLogging");
    }  
  }
  
  private CheckLoggingConfiguration() {}
    
}
