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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * Test for EntityProcessorBase
 * </p>
 *
 *
 * @since solr 1.3
 */
public class TestEntityProcessorBase extends AbstractDataImportHandlerTestCase {

  @Test
  public void multiTransformer() {
    List<Map<String, String>> fields = new ArrayList<>();
    Map<String, String> entity = new HashMap<>();
    entity.put("transformer", T1.class.getName() + "," + T2.class.getName()
            + "," + T3.class.getName());
    fields.add(getField("A", null, null, null, null));
    fields.add(getField("B", null, null, null, null));

    Context context = getContext(null, null, new MockDataSource(), Context.FULL_DUMP,
            fields, entity);
    Map<String, Object> src = new HashMap<>();
    src.put("A", "NA");
    src.put("B", "NA");
    EntityProcessorWrapper sep = new EntityProcessorWrapper(new SqlEntityProcessor(), null, null);
    sep.init(context);
    Map<String, Object> res = sep.applyTransformer(src);
    assertNotNull(res.get("T1"));
    assertNotNull(res.get("T2"));
    assertNotNull(res.get("T3"));
  }

  static class T1 extends Transformer {

    @Override
    public Object transformRow(Map<String, Object> aRow, Context context) {
      aRow.put("T1", "T1 called");
      return aRow;

    }
  }

  static class T2 extends Transformer {

    @Override
    public Object transformRow(Map<String, Object> aRow, Context context) {
      aRow.put("T2", "T2 called");
      return aRow;
    }
  }

  static class T3 {

    public Object transformRow(Map<String, Object> aRow) {
      aRow.put("T3", "T3 called");
      return aRow;
    }
  }
}
