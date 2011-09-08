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

package org.apache.solr.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.junit.Assert;

/**
 *
 */
public class TestUtils extends LuceneTestCase {
  public void testSplitEscaping() {
    List<String> arr = StrUtils.splitSmart("\\r\\n:\\t\\f\\b", ":", true);
    assertEquals(2,arr.size());
    assertEquals("\r\n",arr.get(0));
    assertEquals("\t\f\b",arr.get(1));

    arr = StrUtils.splitSmart("\\r\\n:\\t\\f\\b", ":", false);
    assertEquals(2,arr.size());
    assertEquals("\\r\\n",arr.get(0));
    assertEquals("\\t\\f\\b",arr.get(1));

    arr = StrUtils.splitWS("\\r\\n \\t\\f\\b", true);
    assertEquals(2,arr.size());
    assertEquals("\r\n",arr.get(0));
    assertEquals("\t\f\b",arr.get(1));

    arr = StrUtils.splitWS("\\r\\n \\t\\f\\b", false);
    assertEquals(2,arr.size());
    assertEquals("\\r\\n",arr.get(0));
    assertEquals("\\t\\f\\b",arr.get(1));

    arr = StrUtils.splitSmart("\\:foo\\::\\:bar\\:", ":", true);
    assertEquals(2,arr.size());
    assertEquals(":foo:",arr.get(0));
    assertEquals(":bar:",arr.get(1));

    arr = StrUtils.splitWS("\\ foo\\  \\ bar\\ ", true);
    assertEquals(2,arr.size());
    assertEquals(" foo ",arr.get(0));
    assertEquals(" bar ",arr.get(1));
    
    arr = StrUtils.splitFileNames("/h/s,/h/\\,s,");
    assertEquals(2,arr.size());
    assertEquals("/h/s",arr.get(0));
    assertEquals("/h/,s",arr.get(1));

    arr = StrUtils.splitFileNames("/h/s");
    assertEquals(1,arr.size());
    assertEquals("/h/s",arr.get(0));
  }

  public void testNamedLists()
  {
    SimpleOrderedMap<Integer> map = new SimpleOrderedMap<Integer>();
    map.add( "test", 10 );
    SimpleOrderedMap<Integer> clone = map.clone();
    assertEquals( map.toString(), clone.toString() );
    assertEquals( new Integer(10), clone.get( "test" ) );
  
    Map<String,Integer> realMap = new HashMap<String, Integer>();
    realMap.put( "one", 1 );
    realMap.put( "two", 2 );
    realMap.put( "three", 3 );
    map = new SimpleOrderedMap<Integer>();
    map.addAll( realMap );
    assertEquals( 3, map.size() );
    map = new SimpleOrderedMap<Integer>();
    map.add( "one", 1 );
    map.add( "two", 2 );
    map.add( "three", 3 );
    map.add( "one", 100 );
    map.add( null, null );
    
    assertEquals( "one", map.getName(0) );
    map.setName( 0, "ONE" );
    assertEquals( "ONE", map.getName(0) );
    assertEquals( new Integer(100), map.get( "one", 1 ) );
    assertEquals( 4, map.indexOf( null, 1 ) );
    assertEquals( null, map.get( null, 1 ) );

    map = new SimpleOrderedMap<Integer>();
    map.add( "one", 1 );
    map.add( "two", 2 );
    Iterator<Map.Entry<String, Integer>> iter = map.iterator();
    while( iter.hasNext() ) {
      Map.Entry<String, Integer> v = iter.next();
      v.toString(); // coverage
      v.setValue( v.getValue()*10 );
      try {
        iter.remove();
        Assert.fail( "should be unsupported..." );
      } catch( UnsupportedOperationException ex ) {}
    }
    // the values should be bigger
    assertEquals( new Integer(10), map.get( "one" ) );
    assertEquals( new Integer(20), map.get( "two" ) );
  }
  
  public void testNumberUtils()
  {
    double number = 1.234;
    String sortable = NumberUtils.double2sortableStr( number );
    assertEquals( number, NumberUtils.SortableStr2double(sortable), 0.001);
    
    long num = System.currentTimeMillis();
    sortable = NumberUtils.long2sortableStr( num );
    assertEquals( num, NumberUtils.SortableStr2long(sortable, 0, sortable.length() ) );
    assertEquals( Long.toString(num), NumberUtils.SortableStr2long(sortable) );
  }
}
