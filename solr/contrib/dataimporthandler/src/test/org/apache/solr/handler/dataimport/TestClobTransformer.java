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

import org.junit.Test;

import java.io.StringReader;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Clob;
import java.util.*;

/**
 * Test for ClobTransformer
 *
 *
 * @see org.apache.solr.handler.dataimport.ClobTransformer
 * @since solr 1.4
 */
public class TestClobTransformer extends AbstractDataImportHandlerTestCase {
  @Test
  public void simple() throws Exception {
    List<Map<String, String>> flds = new ArrayList<>();
    Map<String, String> f = new HashMap<>();
    // <field column="dsc" clob="true" name="description" />
    f.put(DataImporter.COLUMN, "dsc");
    f.put(ClobTransformer.CLOB, "true");
    f.put(DataImporter.NAME, "description");
    flds.add(f);
    Context ctx = getContext(null, new VariableResolver(), null, Context.FULL_DUMP, flds, Collections.emptyMap());
    Transformer t = new ClobTransformer();
    Map<String, Object> row = new HashMap<>();
    Clob clob = (Clob) Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[]{Clob.class}, new InvocationHandler() {
      @Override
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (method.getName().equals("getCharacterStream")) {
          return new StringReader("hello!");
        }
        return null;
      }
    });

    row.put("dsc", clob);
    t.transformRow(row, ctx);
    assertEquals("hello!", row.get("dsc"));
  }
}
