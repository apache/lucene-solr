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

package org.apache.solr;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.util.LogLevel;
import org.junit.Test;

@SuppressForbidden(reason="We need to use log4J2 classes to access the log levels")
@LogLevel("org.apache.solr.ClassLogLevel=error;org.apache.solr.MethodLogLevel=warn")
public class TestLogLevelAnnotations extends SolrTestCaseJ4 {

  @Test
  public void testClassLogLevels() {
    assertEquals(Level.ERROR, LogManager.getLogger("org.apache.solr.ClassLogLevel").getLevel());
    assertEquals(Level.WARN, LogManager.getLogger("org.apache.solr.MethodLogLevel").getLevel());
  }

  @Test
  @LogLevel("org.apache.solr.MethodLogLevel=debug")
  public void testMethodLogLevels() {
    assertEquals(Level.ERROR, LogManager.getLogger("org.apache.solr.ClassLogLevel").getLevel());
    assertEquals(Level.DEBUG, LogManager.getLogger("org.apache.solr.MethodLogLevel").getLevel());
  }

}
