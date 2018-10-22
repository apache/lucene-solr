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
package runtimecode;

import java.io.IOException;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.component.RealTimeGetComponent;
import org.apache.solr.handler.component.ResponseBuilder;

public class RuntimeLibSearchComponent extends RealTimeGetComponent {
  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    SolrParams params = rb.req.getParams();

    if (params.getBool(COMPONENT_NAME, true)) {
      rb.rsp.add(COMPONENT_NAME, RuntimeLibSearchComponent.class.getName());
      rb.rsp.add("loader",  getClass().getClassLoader().getClass().getName() );
      rb.rsp.add("Version", "2" );
    }
    super.process(rb);
  }
}
