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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * <p>
 * Test for CachedSqlEntityProcessor
 * </p>
 *
 *
 * @since solr 1.3
 */
public class TestCachedSqlEntityProcessor extends AbstractDataImportHandlerTestCase {

  @Test
  public void withoutWhereClause() {
    List fields = new ArrayList();
    fields.add(createMap("column", "id"));
    fields.add(createMap("column", "desc"));
    String q = "select * from x where id=${x.id}";
    Map<String, String> entityAttrs = createMap("query", q);
    MockDataSource ds = new MockDataSource();
    VariableResolverImpl vr = new VariableResolverImpl();

    vr.addNamespace("x", createMap("id", 1));
    Context context = getContext(null, vr, ds, Context.FULL_DUMP, fields, entityAttrs);
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    rows.add(createMap("id", 1, "desc", "one"));
    rows.add(createMap("id", 1, "desc", "another one"));
    MockDataSource.setIterator(vr.replaceTokens(q), rows.iterator());
    EntityProcessor csep = new EntityProcessorWrapper( new CachedSqlEntityProcessor(), null);
    csep.init(context);
    rows = new ArrayList<Map<String, Object>>();
    while (true) {
      Map<String, Object> r = csep.nextRow();
      if (r == null)
        break;
      rows.add(r);
    }
    assertEquals(2, rows.size());
    ds.close();
    csep.init(context);
    rows = new ArrayList<Map<String, Object>>();
    while (true) {
      Map<String, Object> r = csep.nextRow();
      if (r == null)
        break;
      rows.add(r);
    }
    assertEquals(2, rows.size());
    assertEquals(2, rows.get(0).size());
    assertEquals(2, rows.get(1).size());
  }

  @Test
  public void withoutWhereClauseWithTransformers() {
    List fields = new ArrayList();
    fields.add(createMap("column", "id"));
    fields.add(createMap("column", "desc"));
    String q = "select * from x where id=${x.id}";
    Map<String, String> entityAttrs = createMap(
            "query", q, "transformer", UppercaseTransformer.class.getName());
    MockDataSource ds = new MockDataSource();
    VariableResolverImpl vr = new VariableResolverImpl();

    vr.addNamespace("x", createMap("id", 1));
    Context context = getContext(null, vr, ds, Context.FULL_DUMP, fields, entityAttrs);
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    rows.add(createMap("id", 1, "desc", "one"));
    rows.add(createMap("id", 1, "desc", "another one"));
    MockDataSource.setIterator(vr.replaceTokens(q), rows.iterator());
    EntityProcessor csep = new EntityProcessorWrapper( new CachedSqlEntityProcessor(), null);
    csep.init(context);
    rows = new ArrayList<Map<String, Object>>();
    while (true) {
      Map<String, Object> r = csep.nextRow();
      if (r == null)
        break;
      rows.add(r);
    }
    assertEquals(2, rows.size());
    ds.close();
    csep.init(context);
    rows = new ArrayList<Map<String, Object>>();
    while (true) {
      Map<String, Object> r = csep.nextRow();
      if (r == null)
        break;
      rows.add(r);
      assertEquals(r.get("desc").toString().toUpperCase(Locale.ENGLISH), r.get("desc"));
    }
    assertEquals(2, rows.size());
    assertEquals(2, rows.get(0).size());
    assertEquals(2, rows.get(1).size());
  }

  @Test
  public void withoutWhereClauseWithMultiRowTransformer() {
    List fields = new ArrayList();
    fields.add(createMap("column", "id"));
    fields.add(createMap("column", "desc"));
    String q = "select * from x where id=${x.id}";
    Map<String, String> entityAttrs = createMap(
            "query", q, "transformer", DoubleTransformer.class.getName());
    MockDataSource ds = new MockDataSource();
    VariableResolverImpl vr = new VariableResolverImpl();

    vr.addNamespace("x", createMap("id", 1));
    Context context = getContext(null, vr, ds, Context.FULL_DUMP, fields, entityAttrs);
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    rows.add(createMap("id", 1, "desc", "one"));
    rows.add(createMap("id", 1, "desc", "another one"));
    MockDataSource.setIterator(vr.replaceTokens(q), rows.iterator());
    EntityProcessor csep = new EntityProcessorWrapper( new CachedSqlEntityProcessor(), null);
    csep.init(context);
    rows = new ArrayList<Map<String, Object>>();
    while (true) {
      Map<String, Object> r = csep.nextRow();
      if (r == null)
        break;
      rows.add(r);
    }
    assertEquals(4, rows.size());
    ds.close();
    csep.init(context);
    rows = new ArrayList<Map<String, Object>>();
    while (true) {
      Map<String, Object> r = csep.nextRow();
      if (r == null)
        break;
      rows.add(r);
    }
    assertEquals(4, rows.size());
    assertEquals(2, rows.get(0).size());
    assertEquals(2, rows.get(1).size());
  }

  public static class DoubleTransformer extends Transformer {

    @Override
    public Object transformRow(Map<String, Object> row, Context context) {
      List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
      rows.add(row);
      rows.add(row);

      return rows;
    }
  }

  public static class UppercaseTransformer extends Transformer {

    @Override
    public Object transformRow(Map<String, Object> row, Context context) {
      for (Map.Entry<String, Object> entry : row.entrySet()) {
        Object val = entry.getValue();
        if (val instanceof String) {
          String s = (String) val;
          entry.setValue(s.toUpperCase(Locale.ENGLISH));
        }
      }
      return row;
    }
  }

  @Test
  public void withWhereClause() {
    List fields = new ArrayList();
    fields.add(createMap("column", "id"));
    fields.add(createMap("column", "desc"));
    String q = "select * from x";
    Map<String, String> entityAttrs = createMap(
        "query", q, DIHCacheSupport.CACHE_PRIMARY_KEY,"id", DIHCacheSupport.CACHE_FOREIGN_KEY ,"x.id");
    MockDataSource ds = new MockDataSource();
    VariableResolverImpl vr = new VariableResolverImpl();
    Map xNamespace = createMap("id", 0);
    vr.addNamespace("x", xNamespace);
    Context context = getContext(null, vr, ds, Context.FULL_DUMP, fields, entityAttrs);
    doWhereTest(q, context, ds, xNamespace);
  }

  @Test
  public void withKeyAndLookup() {
    List fields = new ArrayList();
    fields.add(createMap("column", "id"));
    fields.add(createMap("column", "desc"));
    String q = "select * from x";
    Map<String, String> entityAttrs = createMap("query", q, "where", "id=x.id");
    MockDataSource ds = new MockDataSource();
    VariableResolverImpl vr = new VariableResolverImpl();
    Map xNamespace = createMap("id", 0);
    vr.addNamespace("x", xNamespace);
    Context context = getContext(null, vr, ds, Context.FULL_DUMP, fields, entityAttrs);
    doWhereTest(q, context, ds, xNamespace);
  }

  private void doWhereTest(String q, Context context, MockDataSource ds, Map xNamespace) {
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    rows.add(createMap("id", 1, "desc", "one"));
    rows.add(createMap("id", 2, "desc", "two"));
    rows.add(createMap("id", 2, "desc", "another two"));
    rows.add(createMap("id", 3, "desc", "three"));
    rows.add(createMap("id", 3, "desc", "another three"));
    rows.add(createMap("id", 3, "desc", "another another three"));
    MockDataSource.setIterator(q, rows.iterator());
    EntityProcessor csep = new EntityProcessorWrapper(new CachedSqlEntityProcessor(), null);
    csep.init(context);
    rows = new ArrayList<Map<String, Object>>();
    while (true) {
      Map<String, Object> r = csep.nextRow();
      if (r == null)
        break;
      rows.add(r);
    }
    assertEquals(0, rows.size());
    ds.close();

    csep.init(context);
    rows = new ArrayList<Map<String, Object>>();
    xNamespace.put("id", 2);
    while (true) {
      Map<String, Object> r = csep.nextRow();
      if (r == null)
        break;
      rows.add(r);
    }
    assertEquals(2, rows.size());

    csep.init(context);
    rows = new ArrayList<Map<String, Object>>();
    xNamespace.put("id", 3);
    while (true) {
      Map<String, Object> r = csep.nextRow();
      if (r == null)
        break;
      rows.add(r);
    }
    assertEquals(3, rows.size());
  }
}
