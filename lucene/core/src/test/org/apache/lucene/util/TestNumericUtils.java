package org.apache.lucene.util;

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

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Random;

import org.apache.lucene.document.Document;

public class TestNumericUtils extends LuceneTestCase {

  public void testDoubles() throws Exception {
    double[] vals=new double[]{
      Double.NEGATIVE_INFINITY, -2.3E25, -1.0E15, -1.0, -1.0E-1, -1.0E-2, -0.0, 
      +0.0, 1.0E-2, 1.0E-1, 1.0, 1.0E15, 2.3E25, Double.POSITIVE_INFINITY, Double.NaN
    };
    long[] longVals=new long[vals.length];
    
    // check forward and back conversion
    for (int i=0; i<vals.length; i++) {
      longVals[i]=NumericUtils.doubleToLong(vals[i]);
      assertTrue( "forward and back conversion should generate same double", Double.compare(vals[i], NumericUtils.longToDouble(longVals[i]))==0 );
    }
    
    // check sort order (prefixVals should be ascending)
    for (int i=1; i<longVals.length; i++) {
      assertTrue( "check sort order", longVals[i-1] < longVals[i] );
    }
  }

  public static final double[] DOUBLE_NANs = {
    Double.NaN,
    Double.longBitsToDouble(0x7ff0000000000001L),
    Double.longBitsToDouble(0x7fffffffffffffffL),
    Double.longBitsToDouble(0xfff0000000000001L),
    Double.longBitsToDouble(0xffffffffffffffffL)
  };

  public void testSortableDoubleNaN() {
    final long plusInf = NumericUtils.doubleToLong(Double.POSITIVE_INFINITY);
    for (double nan : DOUBLE_NANs) {
      assertTrue(Double.isNaN(nan));
      final long sortable = NumericUtils.doubleToLong(nan);
      assertTrue("Double not sorted correctly: " + nan + ", long repr: " 
          + sortable + ", positive inf.: " + plusInf, sortable > plusInf);
    }
  }
  
  public void testFloats() throws Exception {
    float[] vals=new float[]{
      Float.NEGATIVE_INFINITY, -2.3E25f, -1.0E15f, -1.0f, -1.0E-1f, -1.0E-2f, -0.0f, 
      +0.0f, 1.0E-2f, 1.0E-1f, 1.0f, 1.0E15f, 2.3E25f, Float.POSITIVE_INFINITY, Float.NaN
    };
    int[] intVals=new int[vals.length];
    
    // check forward and back conversion
    for (int i=0; i<vals.length; i++) {
      intVals[i]=NumericUtils.floatToInt(vals[i]);
      assertTrue( "forward and back conversion should generate same double", Float.compare(vals[i], NumericUtils.intToFloat(intVals[i]))==0 );
    }
    
    // check sort order (prefixVals should be ascending)
    for (int i=1; i<intVals.length; i++) {
      assertTrue( "check sort order", intVals[i-1] < intVals[i] );
    }
  }

  public static final float[] FLOAT_NANs = {
    Float.NaN,
    Float.intBitsToFloat(0x7f800001),
    Float.intBitsToFloat(0x7fffffff),
    Float.intBitsToFloat(0xff800001),
    Float.intBitsToFloat(0xffffffff)
  };

  public void testSortableFloatNaN() {
    final int plusInf = NumericUtils.floatToInt(Float.POSITIVE_INFINITY);
    for (float nan : FLOAT_NANs) {
      assertTrue(Float.isNaN(nan));
      final int sortable = NumericUtils.floatToInt(nan);
      assertTrue("Float not sorted correctly: " + nan + ", int repr: " 
          + sortable + ", positive inf.: " + plusInf, sortable > plusInf);
    }
  }

  public void testHalfFloat() throws Exception {
    for(int x=Short.MIN_VALUE;x<=Short.MAX_VALUE;x++) {
      BytesRef bytes = NumericUtils.shortToBytes((short) x);
      assertEquals(x, NumericUtils.bytesToShort(bytes));

      short y = NumericUtils.sortableHalfFloatBits((short) x);
      assertEquals(x, NumericUtils.sortableHalfFloatBits(y));
    }
  }
}
