package org.apache.solr.cloud;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import junit.framework.TestCase;


public class ZkNodePropsTest extends TestCase {

  public void testBasic() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    ZkNodeProps props = new ZkNodeProps();
    props.put("prop1", "value1");
    props.put("prop2", "value2");
    props.put("prop3", "value3");
    props.store(new DataOutputStream(baos));
    
    ZkNodeProps props2 = new ZkNodeProps();
    props2.load(new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));
    assertEquals("value1", props2.get("prop1"));
    assertEquals("value2", props2.get("prop2"));
    assertEquals("value3", props2.get("prop3"));
  }
}
