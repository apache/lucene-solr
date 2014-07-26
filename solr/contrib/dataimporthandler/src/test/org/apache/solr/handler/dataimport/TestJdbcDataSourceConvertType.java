package org.apache.solr.handler.dataimport;

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

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakAction;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakZombies;
import org.apache.solr.SolrTestCaseJ4;

import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

@ThreadLeakAction({ThreadLeakAction.Action.WARN})
@ThreadLeakLingering(linger = 0)
@ThreadLeakZombies(ThreadLeakZombies.Consequence.CONTINUE)
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class TestJdbcDataSourceConvertType extends AbstractDataImportHandlerTestCase {
  public void testConvertType() throws Throwable {

    assumeTrue("Derby is not happy with locale sr__#Latn", !"sr__#Latn".equals(Locale.getDefault().toString()));

    // ironically convertType=false causes BigDecimal to String conversion
    convertTypeTest("false", String.class);

    // convertType=true uses the "long" conversion (see mapping of some_i to "long")
    convertTypeTest("true", Long.class);
  }

  private void convertTypeTest(String convertType, Class resultClass) throws Throwable {
    JdbcDataSource dataSource = new JdbcDataSource();
    Properties p = new Properties();
    p.put("driver", "org.apache.derby.jdbc.EmbeddedDriver");
    p.put("url", "jdbc:derby:memory:tempDB;create=true;territory=en_US");
    p.put("convertType", convertType);

    List<Map<String, String>> flds = new ArrayList<>();
    Map<String, String> f = new HashMap<>();
    f.put("column", "some_i");
    f.put("type", "long");
    flds.add(f);

    Context c = getContext(null, null,
        dataSource, Context.FULL_DUMP, flds, null);
    dataSource.init(c, p);
    Iterator<Map<String, Object>> i = dataSource
        .getData("select 1 as id, CAST(9999 AS DECIMAL) as \"some_i\" from sysibm.sysdummy1");
    assertTrue(i.hasNext());
    Map<String, Object> map = i.next();
    Object val = map.get("some_i");
    assertEquals(resultClass, val.getClass());

    dataSource.close();
  }
}
