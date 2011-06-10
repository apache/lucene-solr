/**
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

import java.util.*;

/**
 * <p>
 * Test for SqlEntityProcessor
 * </p>
 *
 *
 * @since solr 1.3
 */
public class TestSqlEntityProcessor extends AbstractDataImportHandlerTestCase {
  private static ThreadLocal<Integer> local = new ThreadLocal<Integer>();

  @Test
  public void testSingleBatch() {
    SqlEntityProcessor sep = new SqlEntityProcessor();
    List<Map<String, Object>> rows = getRows(3);
    VariableResolverImpl vr = new VariableResolverImpl();
    HashMap<String, String> ea = new HashMap<String, String>();
    ea.put("query", "SELECT * FROM A");
    Context c = getContext(null, vr, getDs(rows), Context.FULL_DUMP, null, ea);
    sep.init(c);
    int count = 0;
    while (true) {
      Map<String, Object> r = sep.nextRow();
      if (r == null)
        break;
      count++;
    }

    assertEquals(3, count);
  }

  @Test
  public void testTranformer() {
    EntityProcessor sep = new EntityProcessorWrapper( new SqlEntityProcessor(), null);
    List<Map<String, Object>> rows = getRows(2);
    VariableResolverImpl vr = new VariableResolverImpl();
    HashMap<String, String> ea = new HashMap<String, String>();
    ea.put("query", "SELECT * FROM A");
    ea.put("transformer", T.class.getName());

    sep.init(getContext(null, vr, getDs(rows), Context.FULL_DUMP, null, ea));
    List<Map<String, Object>> rs = new ArrayList<Map<String, Object>>();
    Map<String, Object> r = null;
    while (true) {
      r = sep.nextRow();
      if (r == null)
        break;
      rs.add(r);

    }
    assertEquals(2, rs.size());
    assertNotNull(rs.get(0).get("T"));
  }

  @Test
  public void testTranformerWithReflection() {
    EntityProcessor sep = new EntityProcessorWrapper(new SqlEntityProcessor(), null);
    List<Map<String, Object>> rows = getRows(2);
    VariableResolverImpl vr = new VariableResolverImpl();
    HashMap<String, String> ea = new HashMap<String, String>();
    ea.put("query", "SELECT * FROM A");
    ea.put("transformer", T3.class.getName());

    sep.init(getContext(null, vr, getDs(rows), Context.FULL_DUMP, null, ea));
    List<Map<String, Object>> rs = new ArrayList<Map<String, Object>>();
    Map<String, Object> r = null;
    while (true) {
      r = sep.nextRow();
      if (r == null)
        break;
      rs.add(r);

    }
    assertEquals(2, rs.size());
    assertNotNull(rs.get(0).get("T3"));
  }

  @Test
  public void testTranformerList() {
    EntityProcessor sep = new EntityProcessorWrapper(new SqlEntityProcessor(),null);
    List<Map<String, Object>> rows = getRows(2);
    VariableResolverImpl vr = new VariableResolverImpl();

    HashMap<String, String> ea = new HashMap<String, String>();
    ea.put("query", "SELECT * FROM A");
    ea.put("transformer", T2.class.getName());
    sep.init(getContext(null, vr, getDs(rows), Context.FULL_DUMP, null, ea));

    local.set(0);
    Map<String, Object> r = null;
    int count = 0;
    while (true) {
      r = sep.nextRow();
      if (r == null)
        break;
      count++;
    }
    assertEquals(2, (int) local.get());
    assertEquals(4, count);
  }

  private List<Map<String, Object>> getRows(int count) {
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    for (int i = 0; i < count; i++) {
      Map<String, Object> row = new HashMap<String, Object>();
      row.put("id", i);
      row.put("value", "The value is " + i);
      rows.add(row);
    }
    return rows;
  }

  private static DataSource<Iterator<Map<String, Object>>> getDs(
          final List<Map<String, Object>> rows) {
    return new DataSource<Iterator<Map<String, Object>>>() {
      @Override
      public Iterator<Map<String, Object>> getData(String query) {
        return rows.iterator();
      }

      @Override
      public void init(Context context, Properties initProps) {
      }

      @Override
      public void close() {
      }
    };
  }

  public static class T extends Transformer {
    @Override
    public Object transformRow(Map<String, Object> aRow, Context context) {
      aRow.put("T", "Class T");
      return aRow;
    }
  }

  public static class T3 {
    public Object transformRow(Map<String, Object> aRow) {
      aRow.put("T3", "T3 class");
      return aRow;
    }
  }

  public static class T2 extends Transformer {
    @Override
    public Object transformRow(Map<String, Object> aRow, Context context) {
      Integer count = local.get();
      local.set(count + 1);
      List<Map<String, Object>> l = new ArrayList<Map<String, Object>>();
      l.add(aRow);
      l.add(aRow);
      return l;
    }
  }
}
