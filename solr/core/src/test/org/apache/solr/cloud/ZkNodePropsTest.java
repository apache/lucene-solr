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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.Test;

public class ZkNodePropsTest extends SolrTestCaseJ4 {
  @Test
  public void testBasic() throws IOException {
    
    Map<String,String> props = new HashMap<String,String>();
    props.put("prop1", "value1");
    props.put("prop2", "value2");
    props.put("prop3", "value3");
    props.put("prop4", "value4");
    props.put("prop5", "value5");
    props.put("prop6", "value6");
    
    ZkNodeProps zkProps = new ZkNodeProps(props);
    byte[] bytes = ZkStateReader.toJSON(zkProps);
    
    ZkNodeProps props2 = ZkNodeProps.load(bytes);
    assertEquals("value1", props2.get("prop1"));
    assertEquals("value2", props2.get("prop2"));
    assertEquals("value3", props2.get("prop3"));
    assertEquals("value4", props2.get("prop4"));
    assertEquals("value5", props2.get("prop5"));
    assertEquals("value6", props2.get("prop6"));
  }
}
