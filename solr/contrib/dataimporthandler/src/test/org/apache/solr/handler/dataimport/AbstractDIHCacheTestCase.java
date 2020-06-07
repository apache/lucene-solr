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

import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.sql.rowset.serial.SerialClob;

import org.apache.solr.handler.dataimport.AbstractDataImportHandlerTestCase.TestContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

public class AbstractDIHCacheTestCase {
  protected static final Date Feb21_2011 = new Date(1298268000000l);
  protected final String[] fieldTypes = { "INTEGER", "BIGDECIMAL", "STRING", "STRING",   "FLOAT",   "DATE",   "CLOB" };
  protected final String[] fieldNames = { "a_id",    "PI",         "letter", "examples", "a_float", "a_date", "DESCRIPTION" };
  protected List<ControlData> data = new ArrayList<>();
  protected Clob APPLE = null;

  @Before
  public void setup() {
    try {
      APPLE = new SerialClob("Apples grow on trees and they are good to eat.".toCharArray());
    } catch (SQLException sqe) {
      Assert.fail("Could not Set up Test");
    }

    // The first row needs to have all non-null fields,
    // otherwise we would have to always send the fieldTypes & fieldNames as CacheProperties when building.
    data = new ArrayList<>();
    data.add(new ControlData(new Object[] {1, new BigDecimal(Math.PI), "A", "Apple", 1.11f, Feb21_2011, APPLE }));
    data.add(new ControlData(new Object[] {2, new BigDecimal(Math.PI), "B", "Ball", 2.22f, Feb21_2011, null }));
    data.add(new ControlData(new Object[] {4, new BigDecimal(Math.PI), "D", "Dog", 4.44f, Feb21_2011, null }));
    data.add(new ControlData(new Object[] {3, new BigDecimal(Math.PI), "C", "Cookie", 3.33f, Feb21_2011, null }));
    data.add(new ControlData(new Object[] {4, new BigDecimal(Math.PI), "D", "Daisy", 4.44f, Feb21_2011, null }));
    data.add(new ControlData(new Object[] {4, new BigDecimal(Math.PI), "D", "Drawing", 4.44f, Feb21_2011, null }));
    data.add(new ControlData(new Object[] {5, new BigDecimal(Math.PI), "E",
        Arrays.asList("Eggplant", "Ear", "Elephant", "Engine"), 5.55f, Feb21_2011, null }));
  }

  @After
  public void teardown() {
    APPLE = null;
    data = null;
  }

  //A limitation of this test class is that the primary key needs to be the first one in the list.
  //DIHCaches, however, can handle any field being the primary key.
  static class ControlData implements Comparable<ControlData>, Iterable<Object> {
    Object[] data;

    ControlData(Object[] data) {
      this.data = data;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public int compareTo(ControlData cd) {
      Comparable c1 = (Comparable) data[0];
      Comparable c2 = (Comparable) cd.data[0];
      return c1.compareTo(c2);
    }

    @Override
    public Iterator<Object> iterator() {
      return Arrays.asList(data).iterator();
    }
  }

  protected void loadData(DIHCache cache, List<ControlData> theData, String[] theFieldNames, boolean keepOrdered) {
    for (ControlData cd : theData) {
      cache.add(controlDataToMap(cd, theFieldNames, keepOrdered));
    }
  }

  protected List<ControlData> extractDataInKeyOrder(DIHCache cache, String[] theFieldNames) {
    List<Object[]> data = new ArrayList<>();
    Iterator<Map<String, Object>> cacheIter = cache.iterator();
    while (cacheIter.hasNext()) {
      data.add(mapToObjectArray(cacheIter.next(), theFieldNames));
    }
    return listToControlData(data);
  }

  //This method assumes that the Primary Keys are integers and that the first id=1.
  //It will look for id's sequentially until one is skipped, then will stop.
  protected List<ControlData> extractDataByKeyLookup(DIHCache cache, String[] theFieldNames) {
    int recId = 1;
    List<Object[]> data = new ArrayList<>();
    while (true) {
      Iterator<Map<String, Object>> listORecs = cache.iterator(recId);
      if (listORecs == null) {
        break;
      }

      while(listORecs.hasNext()) {
        data.add(mapToObjectArray(listORecs.next(), theFieldNames));
      }
      recId++;
    }
    return listToControlData(data);
  }

  protected List<ControlData> listToControlData(List<Object[]> data) {
    List<ControlData> returnData = new ArrayList<>(data.size());
    for (int i = 0; i < data.size(); i++) {
      returnData.add(new ControlData(data.get(i)));
    }
    return returnData;
  }

  protected Object[] mapToObjectArray(Map<String, Object> rec, String[] theFieldNames) {
    Object[] oos = new Object[theFieldNames.length];
    for (int i = 0; i < theFieldNames.length; i++) {
      oos[i] = rec.get(theFieldNames[i]);
    }
    return oos;
  }

  protected void compareData(List<ControlData> theControl, List<ControlData> test) {
    // The test data should come back primarily in Key order and secondarily in insertion order.
    List<ControlData> control = new ArrayList<>(theControl);
    Collections.sort(control);

    StringBuilder errors = new StringBuilder();
    if (test.size() != control.size()) {
      errors.append("-Returned data has " + test.size() + " records.  expected: " + control.size() + "\n");
    }
    for (int i = 0; i < control.size() && i < test.size(); i++) {
      Object[] controlRec = control.get(i).data;
      Object[] testRec = test.get(i).data;
      if (testRec.length != controlRec.length) {
        errors.append("-Record indexAt=" + i + " has " + testRec.length + " data elements.  extpected: " + controlRec.length + "\n");
      }
      for (int j = 0; j < controlRec.length && j < testRec.length; j++) {
        Object controlObj = controlRec[j];
        Object testObj = testRec[j];
        if (controlObj == null && testObj != null) {
          errors.append("-Record indexAt=" + i + ", Data Element indexAt=" + j + " is not NULL as expected.\n");
        } else if (controlObj != null && testObj == null) {
          errors.append("-Record indexAt=" + i + ", Data Element indexAt=" + j + " is NULL.  Expected: " + controlObj + " (class="
              + controlObj.getClass().getName() + ")\n");
        } else if (controlObj != null && testObj != null && controlObj instanceof Clob) {
          String controlString = clobToString((Clob) controlObj);
          String testString = clobToString((Clob) testObj);
          if (!controlString.equals(testString)) {
            errors.append("-Record indexAt=" + i + ", Data Element indexAt=" + j + " has: " + testString + " (class=Clob) ... Expected: " + controlString
                + " (class=Clob)\n");
          }
        } else if (controlObj != null && !controlObj.equals(testObj)) {
          errors.append("-Record indexAt=" + i + ", Data Element indexAt=" + j + " has: " + testObj + " (class=" + testObj.getClass().getName()
              + ") ... Expected: " + controlObj + " (class=" + controlObj.getClass().getName() + ")\n");
        }
      }
    }
    if (errors.length() > 0) {
      Assert.fail(errors.toString());
    }
  }

  protected Map<String, Object> controlDataToMap(ControlData cd, String[] theFieldNames, boolean keepOrdered) {
    Map<String, Object> rec = null;
    if (keepOrdered) {
      rec = new LinkedHashMap<>();
    } else {
      rec = new HashMap<>();
    }
    for (int i = 0; i < cd.data.length; i++) {
      String fieldName = theFieldNames[i];
      Object data = cd.data[i];
      rec.put(fieldName, data);
    }
    return rec;
  }

  protected String stringArrayToCommaDelimitedList(String[] strs) {
    StringBuilder sb = new StringBuilder();
    for (String a : strs) {
      if (sb.length() > 0) {
        sb.append(",");
      }
      sb.append(a);
    }
    return sb.toString();
  }

  protected String clobToString(Clob cl) {
    StringBuilder sb = new StringBuilder();
    try {
      Reader in = cl.getCharacterStream();
      char[] cbuf = new char[1024];
      int numGot = -1;
      while ((numGot = in.read(cbuf)) != -1) {
        sb.append(String.valueOf(cbuf, 0, numGot));
      }
    } catch (Exception e) {
      Assert.fail(e.toString());
    }
    return sb.toString();
  }

  public static Context getContext(final Map<String, String> entityAttrs) {
    VariableResolver resolver = new VariableResolver();
    final Context delegate = new ContextImpl(null, resolver, null, null, new HashMap<String, Object>(), null, null);
    return new TestContext(entityAttrs, delegate, null, true);
  }

}
