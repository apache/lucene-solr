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
package org.apache.solr.handler.dataimport;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;

public class TestURLDataSource extends AbstractDataImportHandlerTestCase {
  private List<Map<String, String>> fields = new ArrayList<>();
  private URLDataSource dataSource = new URLDataSource();
  private VariableResolver variableResolver = new VariableResolver();
  private Context context = AbstractDataImportHandlerTestCase.getContext(null, variableResolver,
      dataSource, Context.FULL_DUMP, fields, null);
  private Properties initProps = new Properties();
  
  @Test
  public void substitutionsOnBaseUrl() throws Exception {
    String url = "http://example.com/";
    
    variableResolver.addNamespace("dataimporter.request", Collections.<String,Object>singletonMap("baseurl", url));
    
    initProps.setProperty(URLDataSource.BASE_URL, "${dataimporter.request.baseurl}");
    dataSource.init(context, initProps);
    assertEquals(url, dataSource.getBaseUrl());
  }
}
