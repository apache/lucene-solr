package org.apache.lucene.search.trie;

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

import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.lucene.util.LuceneTestCase;

public class TestTrieUtils extends LuceneTestCase {

  public void testSpecialValues() throws Exception {
    // Variant 8bit values
    assertEquals( TrieUtils.VARIANT_8BIT.TRIE_CODED_NUMERIC_MIN, "\u0100\u0100\u0100\u0100\u0100\u0100\u0100\u0100");
    assertEquals( TrieUtils.VARIANT_8BIT.TRIE_CODED_NUMERIC_MAX, "\u01ff\u01ff\u01ff\u01ff\u01ff\u01ff\u01ff\u01ff");
    assertEquals( TrieUtils.VARIANT_8BIT.longToTrieCoded(-1),    "\u017f\u01ff\u01ff\u01ff\u01ff\u01ff\u01ff\u01ff");
    assertEquals( TrieUtils.VARIANT_8BIT.longToTrieCoded(0),     "\u0180\u0100\u0100\u0100\u0100\u0100\u0100\u0100");
    assertEquals( TrieUtils.VARIANT_8BIT.longToTrieCoded(1),     "\u0180\u0100\u0100\u0100\u0100\u0100\u0100\u0101");
    // Variant 4bit values
    assertEquals( TrieUtils.VARIANT_4BIT.TRIE_CODED_NUMERIC_MIN, "\u0100\u0100\u0100\u0100\u0100\u0100\u0100\u0100\u0100\u0100\u0100\u0100\u0100\u0100\u0100\u0100");
    assertEquals( TrieUtils.VARIANT_4BIT.TRIE_CODED_NUMERIC_MAX, "\u010f\u010f\u010f\u010f\u010f\u010f\u010f\u010f\u010f\u010f\u010f\u010f\u010f\u010f\u010f\u010f");
    assertEquals( TrieUtils.VARIANT_4BIT.longToTrieCoded(-1),    "\u0107\u010f\u010f\u010f\u010f\u010f\u010f\u010f\u010f\u010f\u010f\u010f\u010f\u010f\u010f\u010f");
    assertEquals( TrieUtils.VARIANT_4BIT.longToTrieCoded(0),     "\u0108\u0100\u0100\u0100\u0100\u0100\u0100\u0100\u0100\u0100\u0100\u0100\u0100\u0100\u0100\u0100");
    assertEquals( TrieUtils.VARIANT_4BIT.longToTrieCoded(1),     "\u0108\u0100\u0100\u0100\u0100\u0100\u0100\u0100\u0100\u0100\u0100\u0100\u0100\u0100\u0100\u0101");
    // TODO: 2bit tests
  }

  private void testBinaryOrderingAndIncrement(TrieUtils variant) throws Exception {
    // generate a series of encoded longs, each numerical one bigger than the one before
    String last=null;
    for (long l=-100000L; l<100000L; l++) {
      String act=variant.longToTrieCoded(l);
      if (last!=null) {
        // test if smaller
        assertTrue( last.compareTo(act) < 0 );
        // test the increment method (the last incremented by one should be the actual)
        assertEquals( variant.incrementTrieCoded(last), act );
        // test the decrement method (the actual decremented by one should be the last)
        assertEquals( last, variant.decrementTrieCoded(act) );
      }
      // next step
      last=act;
    }
  }

  public void testBinaryOrderingAndIncrement_8bit() throws Exception {
    testBinaryOrderingAndIncrement(TrieUtils.VARIANT_8BIT);
  }

  public void testBinaryOrderingAndIncrement_4bit() throws Exception {
    testBinaryOrderingAndIncrement(TrieUtils.VARIANT_8BIT);
  }

  public void testBinaryOrderingAndIncrement_2bit() throws Exception {
    testBinaryOrderingAndIncrement(TrieUtils.VARIANT_8BIT);
  }

  private void testLongs(TrieUtils variant) throws Exception {
    long[] vals=new long[]{
      Long.MIN_VALUE, -5000L, -4000L, -3000L, -2000L, -1000L, 0L,
      1L, 10L, 300L, 5000L, Long.MAX_VALUE-2, Long.MAX_VALUE-1, Long.MAX_VALUE
    };
    String[] trieVals=new String[vals.length];
    
    // check back and forth conversion
    for (int i=0; i<vals.length; i++) {
      trieVals[i]=variant.longToTrieCoded(vals[i]);
      assertEquals( "Back and forth conversion should return same value", vals[i], variant.trieCodedToLong(trieVals[i]) );
      assertEquals( "Automatic back conversion with encoding detection should return same value", vals[i], TrieUtils.trieCodedToLongAuto(trieVals[i]) );
    }
    
    // check sort order (trieVals should be ascending)
    for (int i=1; i<vals.length; i++) {
      assertTrue( trieVals[i-1].compareTo( trieVals[i] ) < 0 );
    }
  }

  public void testLongs_8bit() throws Exception {
    testLongs(TrieUtils.VARIANT_8BIT);
  }

  public void testLongs_4bit() throws Exception {
    testLongs(TrieUtils.VARIANT_4BIT);
  }

  public void testLongs_2bit() throws Exception {
    testLongs(TrieUtils.VARIANT_2BIT);
  }

  private void testDoubles(TrieUtils variant) throws Exception {
    double[] vals=new double[]{
      Double.NEGATIVE_INFINITY, -2.3E25, -1.0E15, -1.0, -1.0E-1, -1.0E-2, -0.0, 
      +0.0, 1.0E-2, 1.0E-1, 1.0, 1.0E15, 2.3E25, Double.POSITIVE_INFINITY
    };
    String[] trieVals=new String[vals.length];
    
    // check back and forth conversion
    for (int i=0; i<vals.length; i++) {
      trieVals[i]=variant.doubleToTrieCoded(vals[i]);
      assertTrue( "Back and forth conversion should return same value", vals[i]==variant.trieCodedToDouble(trieVals[i]) );
      assertTrue( "Automatic back conversion with encoding detection should return same value", vals[i]==TrieUtils.trieCodedToDoubleAuto(trieVals[i]) );
    }
    
    // check sort order (trieVals should be ascending)
    for (int i=1; i<vals.length; i++) {
      assertTrue( trieVals[i-1].compareTo( trieVals[i] ) < 0 );
    }
  }

  public void testDoubles_8bit() throws Exception {
    testDoubles(TrieUtils.VARIANT_8BIT);
  }

  public void testDoubles_4bit() throws Exception {
    testDoubles(TrieUtils.VARIANT_4BIT);
  }

  public void testDoubles_2bit() throws Exception {
    testDoubles(TrieUtils.VARIANT_2BIT);
  }

  private void testDates(TrieUtils variant) throws Exception {
    Date[] vals=new Date[]{
      new GregorianCalendar(1000,1,1).getTime(),
      new GregorianCalendar(1999,1,1).getTime(),
      new GregorianCalendar(2000,1,1).getTime(),
      new GregorianCalendar(2001,1,1).getTime()
    };
    String[] trieVals=new String[vals.length];
    
    // check back and forth conversion
    for (int i=0; i<vals.length; i++) {
      trieVals[i]=variant.dateToTrieCoded(vals[i]);
      assertEquals( "Back and forth conversion should return same value", vals[i], variant.trieCodedToDate(trieVals[i]) );
      assertEquals( "Automatic back conversion with encoding detection should return same value", vals[i], TrieUtils.trieCodedToDateAuto(trieVals[i]) );
    }
    
    // check sort order (trieVals should be ascending)
    for (int i=1; i<vals.length; i++) {
      assertTrue( trieVals[i-1].compareTo( trieVals[i] ) < 0 );
    }
  }

  public void testDates_8bit() throws Exception {
    testDates(TrieUtils.VARIANT_8BIT);
  }

  public void testDates_4bit() throws Exception {
    testDates(TrieUtils.VARIANT_4BIT);
  }

  public void testDates_2bit() throws Exception {
    testDates(TrieUtils.VARIANT_2BIT);
  }

}
