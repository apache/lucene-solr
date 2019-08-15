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

import java.lang.invoke.MethodHandles;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSortedMapBackedCache extends AbstractDIHCacheTestCase {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  @Test
  public void testCacheWithKeyLookup() {
    DIHCache cache = null;
    try {
      cache = new SortedMapBackedCache();
      cache.open(getContext(new HashMap<String,String>()));
      loadData(cache, data, fieldNames, true);
      List<ControlData> testData = extractDataByKeyLookup(cache, fieldNames);
      compareData(data, testData);
    } catch (Exception e) {
      log.warn("Exception thrown: " + e.toString());
      Assert.fail();
    } finally {
      try {
        cache.destroy();
      } catch (Exception ex) {
      }
    }
  }

  @Test
  public void testCacheWithOrderedLookup() {
    DIHCache cache = null;
    try {
      cache = new SortedMapBackedCache();
      cache.open(getContext(new HashMap<String,String>()));
      loadData(cache, data, fieldNames, true);
      List<ControlData> testData = extractDataInKeyOrder(cache, fieldNames);
      compareData(data, testData);
    } catch (Exception e) {
      log.warn("Exception thrown: " + e.toString());
      Assert.fail();
    } finally {
      try {
        cache.destroy();
      } catch (Exception ex) {
      }
    }
  }
  
  @Test
  public void testNullKeys() throws Exception {
    //A null key should just be ignored, but not throw an exception
    DIHCache cache = null;
    try {
      cache = new SortedMapBackedCache();
      Map<String, String> cacheProps = new HashMap<>();
      cacheProps.put(DIHCacheSupport.CACHE_PRIMARY_KEY, "a_id");
      cache.open(getContext(cacheProps));
      
      Map<String,Object> data = new HashMap<>();
      data.put("a_id", null);
      data.put("bogus", "data");
      cache.add(data);
      
      Iterator<Map<String, Object>> cacheIter = cache.iterator();
      while (cacheIter.hasNext()) {
        Assert.fail("cache should be empty.");
      }
      Assert.assertNull(cache.iterator(null));
      cache.delete(null);      
    } catch (Exception e) {
      throw e;
    } finally {
      try {
        cache.destroy();
      } catch (Exception ex) {
      }
    }    
  }

  @SuppressWarnings("unused")
  @Test
  public void testCacheReopensWithUpdate() {
    DIHCache cache = null;
    try {      
      Map<String, String> cacheProps = new HashMap<>();
      cacheProps.put(DIHCacheSupport.CACHE_PRIMARY_KEY, "a_id");
      
      cache = new SortedMapBackedCache();
      cache.open(getContext(cacheProps));
      // We can let the data hit the cache with the fields out of order because
      // we've identified the pk up-front.
      loadData(cache, data, fieldNames, false);

      // Close the cache.
      cache.close();

      List<ControlData> newControlData = new ArrayList<>();
      Object[] newIdEqualsThree = null;
      int j = 0;
      for (int i = 0; i < data.size(); i++) {
        // We'll be deleting a_id=1 so remove it from the control data.
        if (data.get(i).data[0].equals(1)) {
          continue;
        }

        // We'll be changing "Cookie" to "Carrot" in a_id=3 so change it in the control data.
        if (data.get(i).data[0].equals(3)) {
          newIdEqualsThree = new Object[data.get(i).data.length];
          System.arraycopy(data.get(i).data, 0, newIdEqualsThree, 0, newIdEqualsThree.length);
          newIdEqualsThree[3] = "Carrot";
          newControlData.add(new ControlData(newIdEqualsThree));
        }
        // Everything else can just be copied over.
        else {
          newControlData.add(data.get(i));
        }

        j++;
      }

      // These new rows of data will get added to the cache, so add them to the control data too.
      Object[] newDataRow1 = new Object[] {99, new BigDecimal(Math.PI), "Z", "Zebra", 99.99f, Feb21_2011, null };
      Object[] newDataRow2 = new Object[] {2, new BigDecimal(Math.PI), "B", "Ballerina", 2.22f, Feb21_2011, null };

      newControlData.add(new ControlData(newDataRow1));
      newControlData.add(new ControlData(newDataRow2));

      // Re-open the cache
      cache.open(getContext(new HashMap<String,String>()));

      // Delete a_id=1 from the cache.
      cache.delete(1);

      // Because the cache allows duplicates, the only way to update is to
      // delete first then add.
      cache.delete(3);
      cache.add(controlDataToMap(new ControlData(newIdEqualsThree), fieldNames, false));

      // Add this row with a new Primary key.
      cache.add(controlDataToMap(new ControlData(newDataRow1), fieldNames, false));

      // Add this row, creating two records in the cache with a_id=2.
      cache.add(controlDataToMap(new ControlData(newDataRow2), fieldNames, false));

      // Read the cache back and compare to the newControlData
      List<ControlData> testData = extractDataInKeyOrder(cache, fieldNames);
      compareData(newControlData, testData);

      // Now try reading the cache read-only.
      cache.close();
      cache.open(getContext(new HashMap<String,String>()));
      testData = extractDataInKeyOrder(cache, fieldNames);
      compareData(newControlData, testData);

    } catch (Exception e) {
      log.warn("Exception thrown: " + e.toString());
      Assert.fail();
    } finally {
      try {
        cache.destroy();
      } catch (Exception ex) {
      }
    }
  }
}
