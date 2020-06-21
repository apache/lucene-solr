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
package org.apache.solr.cloud;

import org.junit.AfterClass;
import org.junit.BeforeClass;

public class OverseerSolrResponseUnsafeSerializationTest extends OverseerSolrResponseTest {
  
  @BeforeClass
  public static void setUpClass() {
    System.setProperty("solr.useUnsafeOverseerResponse", "true");
  }
  
  @AfterClass
  public static void tearDownClass() {
    System.clearProperty("solr.useUnsafeOverseerResponse");
  }
  
  
  public void testUnsafeSerializartionToggles() {
    assertToggles("true", true, true);
    assertToggles("deserialization", false, true);
    assertToggles(null, false, false); // By default, don't use unsafe
    assertToggles("foo", false, false);
    assertToggles("false", false, false);
    assertToggles("serialization", false, false); // This is not an option
  }

  private void assertToggles(String propertyValue, boolean serializationEnabled, boolean deserializationEnabled) {
    String previousValue = System.getProperty("solr.useUnsafeOverseerResponse");
    try  {
      if (propertyValue == null) {
        System.clearProperty("solr.useUnsafeOverseerResponse");
      } else {
        System.setProperty("solr.useUnsafeOverseerResponse", propertyValue);
      }
      assertEquals("Unexpected serialization toggle for value: " + propertyValue, serializationEnabled, OverseerSolrResponseSerializer.useUnsafeSerialization());
      assertEquals("Unexpected serialization toggle for value: " + propertyValue, deserializationEnabled, OverseerSolrResponseSerializer.useUnsafeDeserialization());
    } finally {
      if (previousValue != null) {
        System.setProperty("solr.useUnsafeOverseerResponse", previousValue);
      }
    }
  }

}
