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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;

public class OverseerSolrResponseTest extends SolrTestCaseJ4 {
  
  public void testEmpty() {
    assertSerializeDeserialize(new NamedList<Object>());
  }
  
  public void testWithSingleObject() {
    NamedList<Object> responseNl = new NamedList<>();
    responseNl.add("foo", "bar");
    assertSerializeDeserialize(responseNl);
  }
  
  public void testWithMultipleObject() {
    NamedList<Object> responseNl = new NamedList<>();
    responseNl.add("foo", "bar");
    responseNl.add("foobar", "foo");
    assertSerializeDeserialize(responseNl);
  }
  
  public void testRepeatedKeys() {
    NamedList<Object> responseNl = new NamedList<>();
    responseNl.add("foo", "bar");
    responseNl.add("foo", "zoo");
    assertSerializeDeserialize(responseNl);
  }
  
  public void testNested() {
    NamedList<Object> responseNl = new NamedList<>();
    NamedList<Object> response2 = new NamedList<>();
    response2.add("foo", "bar");
    responseNl.add("foo", response2);
    assertSerializeDeserialize(responseNl);
  }
  
  public void testException() {
    NamedList<Object> responseNl = new NamedList<>();
    SolrException e = new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Foo");
    SimpleOrderedMap<Object> exceptionNl = new SimpleOrderedMap<>();
    exceptionNl.add("msg", e.getMessage());
    exceptionNl.add("rspCode", e.code());
    responseNl.add("exception", exceptionNl);
    OverseerSolrResponse deserialized = OverseerSolrResponseSerializer.deserialize(OverseerSolrResponseSerializer.serialize(new OverseerSolrResponse(responseNl)));
    assertNotNull("Expecting an exception", deserialized.getException());
    assertEquals("Unexpected exception type in deserialized response", SolrException.class, deserialized.getException().getClass());
    assertEquals("Unexpected exception code in deserialized response", e.code(), ((SolrException)deserialized.getException()).code());
    assertEquals("Unexpected exception message in deserialized response", e.getMessage(), deserialized.getException().getMessage());
  }
  
  private void assertSerializeDeserialize(NamedList<Object> content) {
    OverseerSolrResponse response = new OverseerSolrResponse(content);
    byte[] serialized = OverseerSolrResponseSerializer.serialize(response);
    OverseerSolrResponse deserialized = OverseerSolrResponseSerializer.deserialize(serialized);
    assertEquals("Deserialized response is different than original", response.getResponse(), deserialized.getResponse());
  }

}
