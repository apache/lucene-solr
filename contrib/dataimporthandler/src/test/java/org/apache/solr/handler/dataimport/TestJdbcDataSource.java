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

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;

/**
 * <p>
 * Test for JdbcDataSource
 * </p>
 * <p/>
 * <p>
 * Note: The tests are ignored for the lack of DB support for testing
 * </p>
 *
 * @version $Id$
 * @since solr 1.3
 */
public class TestJdbcDataSource {

  @Test
  @Ignore
  public void basic() throws Exception {
    JdbcDataSource dataSource = new JdbcDataSource();
    Properties p = new Properties();
    p.put("driver", "com.mysql.jdbc.Driver");
    p.put("url", "jdbc:mysql://localhost/autos");
    p.put("user", "root");
    p.put("password", "");

    List<Map<String, String>> flds = new ArrayList<Map<String, String>>();
    Map<String, String> f = new HashMap<String, String>();
    f.put("column", "trim_id");
    f.put("type", "long");
    flds.add(f);
    f = new HashMap<String, String>();
    f.put("column", "msrp");
    f.put("type", "float");
    flds.add(f);

    Context c = AbstractDataImportHandlerTest.getContext(null, null,
            dataSource, 0, flds, null);
    dataSource.init(c, p);
    Iterator<Map<String, Object>> i = dataSource
            .getData("select make,model,year,msrp,trim_id from atrimlisting where make='Acura'");
    int count = 0;
    Object msrp = null;
    Object trim_id = null;
    while (i.hasNext()) {
      Map<String, Object> map = i.next();
      msrp = map.get("msrp");
      trim_id = map.get("trim_id");
      count++;
    }
    Assert.assertEquals(5, count);
    Assert.assertEquals(Float.class, msrp.getClass());
    Assert.assertEquals(Long.class, trim_id.getClass());
  }

}
