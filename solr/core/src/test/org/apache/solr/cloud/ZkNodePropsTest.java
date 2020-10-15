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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.SimplePostTool;
import org.junit.Test;

public class ZkNodePropsTest extends SolrTestCaseJ4 {
  @Test
  public void testBasic() throws IOException {
    
    Map<String,Object> props = new HashMap<>();
    props.put("prop1", "value1");
    props.put("prop2", "value2");
    props.put("prop3", "value3");
    props.put("prop4", "value4");
    props.put("prop5", "value5");
    props.put("prop6", "value6");
    
    ZkNodeProps zkProps = new ZkNodeProps(props);
    byte[] bytes = Utils.toJSON(zkProps);
    ZkNodeProps props2 = ZkNodeProps.load(bytes);

    props.forEach((s, o) -> assertEquals(o, props2.get(s)));
    SimplePostTool.BAOS baos = new SimplePostTool.BAOS();
    try (JavaBinCodec jbc = new JavaBinCodec()) {
      jbc.marshal(zkProps.getProperties(), baos);
    }
    bytes = baos.toByteArray();
    System.out.println("BIN size : " + bytes.length);
    ZkNodeProps props3 = ZkNodeProps.load(bytes);
    props.forEach((s, o) -> assertEquals(o, props3.get(s)));
  }
}
