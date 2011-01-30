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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.util.NumberUtils;
import org.apache.solr.util.BCDUtils;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TestNumberUtils extends LuceneTestCase {

  private static String arrstr(char[] arr, int start, int end) {
    String str="[";
    for (int i=start; i<end; i++) str += arr[i]+"("+(int)arr[i]+"),";
    return str+"]";
  }

  static Random rng = random;

  static int[] special = {0,10,100,1000,10000,Integer.MAX_VALUE, Integer.MIN_VALUE};
  static int getSpecial() {
    int i = rng.nextInt();
    int j = rng.nextInt();
    if ((i & 0x10) != 0) return j;
    return special[(j&0x7fffffff) % special.length]* ((i & 0x20)==0?1:-1) + ((i&0x03)-1);
  }

  static long[] lspecial = {0,10,100,1000,10000,2,4,8,256,16384,32768,65536,
                            Integer.MAX_VALUE, Integer.MIN_VALUE,
                            Long.MAX_VALUE, Long.MIN_VALUE};
  static long getLongSpecial() {
    int i = rng.nextInt();
    long j = rng.nextLong();
    if ((i & 0x10) != 0) return j;
    return lspecial[((int)j&0x7fffffff) % special.length]* ((i & 0x20)==0?1:-1) + ((i&0x03)-1);
  }

  static float[] fspecial = {0,1,2,4,8,256,16384,32768,65536,.1f,.25f
     ,Float.NEGATIVE_INFINITY,Float.POSITIVE_INFINITY,Float.MIN_VALUE, Float.MAX_VALUE};
  static float getFloatSpecial() {
    int i = rng.nextInt();
    int j = rng.nextInt();
    float f = Float.intBitsToFloat(j);
    if (f!=f) f=0; // get rid of NaN for comparison purposes
    if ((i & 0x10) != 0) return f;
    return fspecial[(j&0x7fffffff) % fspecial.length]* ((i & 0x20)==0?1:-1) + ((i&0x03)-1);
  }

  static double[] dspecial = {0,1,2,4,8,256,16384,32768,65536,.1,.25
     ,Float.NEGATIVE_INFINITY,Float.POSITIVE_INFINITY,Float.MIN_VALUE, Float.MAX_VALUE
     ,Double.NEGATIVE_INFINITY,Double.POSITIVE_INFINITY,Double.MIN_VALUE, Double.MAX_VALUE
      };
  static double getDoubleSpecial() {
    int i = rng.nextInt();
    long j = rng.nextLong();
    double f = Double.longBitsToDouble(j);
    if (f!=f) f=0; // get rid of NaN for comparison purposes
    if ((i & 0x10) != 0) return f;
    return dspecial[((int)j&0x7fffffff) % dspecial.length]* ((i & 0x20)==0?1:-1) + ((i&0x03)-1);
  }


  public static void test(Comparable n1, Comparable n2, Converter conv) {
    String s1=n1.toString();
    String s2=n2.toString();
    String v1 = conv.toInternal(s1);
    String v2 = conv.toInternal(s2);
    String out1=conv.toExternal(v1);
    String out2=conv.toExternal(v2);

    Assert.assertEquals(conv + " :: n1 :: input!=output", s1, out1);
    Assert.assertEquals(conv + " :: n2 :: input!=output", s2, out2);
    
    int c1 = n1.compareTo(n2);
    int c2 = v1.compareTo(v2);

    Assert.assertFalse( (c1==0 && !(c2==0)) );
//    Assert.assertFalse( c1 < 0 && !(c2<0) );
//    Assert.assertFalse( c1 > 0 && !(c2>0) );
//    
    //    if (c1==0 && !(c2==0) 
//    || c1 < 0 && !(c2<0) 
//    || c1 > 0 && !(c2>0)
//    || !out1.equals(s1) || !out2.equals(s2))
//    {
//      Assert.fail("Comparison error:"+s1+","+s2 + " :: " + conv);
//      System.out.print("v1=");
//      for (int ii=0; ii<v1.length(); ii++) {
//        System.out.print(" " + (int)v1.charAt(ii));
//      }
//      System.out.print("\nv2=");
//      for (int ii=0; ii<v2.length(); ii++) {
//        System.out.print(" " + (int)v2.charAt(ii));
//      }
//      System.out.println("\nout1='"+out1+"', out2='" + out2 + "'");
//    }
  }
  
  
  public void testConverters()
  {
    int iter=1000;
    int arrsz=100000;
    int num=12345;

    // INTEGERS
    List<Converter> converters = new ArrayList<Converter>();
    converters.add( new Int2Int() );
    converters.add( new SortInt() );
    converters.add( new Base10kS() );
    converters.add( new Base100S() );
    
    for( Converter c : converters ) {
      for (int i=0; i<iter; i++) {
        Comparable n1 = getSpecial();
        Comparable n2 = getSpecial();
        test( n1, n2, c );
      }
    }

    // LONG
    converters.clear();
    converters.add( new SortLong() );
    converters.add( new Base10kS() );
    converters.add( new Base100S() );
    for( Converter c : converters ) {
      for (int i=0; i<iter; i++) {
        Comparable n1 = getLongSpecial();
        Comparable n2 = getLongSpecial();
        test( n1, n2, c );
      }
    }

    // FLOAT
    converters.clear();
    converters.add( new Float2Float() );
    converters.add( new SortFloat() );
    for( Converter c : converters ) {
      for (int i=0; i<iter; i++) {
        Comparable n1 = getFloatSpecial();
        Comparable n2 = getFloatSpecial();
        test( n1, n2, c );
      }
    }

    // DOUBLE
    converters.clear();
    converters.add( new SortDouble() );
    for( Converter c : converters ) {
      for (int i=0; i<iter; i++) {
        Comparable n1 = getDoubleSpecial();
        Comparable n2 = getDoubleSpecial();
        test( n1, n2, c );
      }
    }
  }
}


abstract class Converter {
  abstract String toInternal(String val);
  abstract String toExternal(String val);
}

class Int2Int extends Converter {
  @Override
  public String toInternal(String val) {
    return Integer.toString(Integer.parseInt(val));
  }
  @Override
  public String toExternal(String val) {
    return Integer.toString(Integer.parseInt(val));
  }
}

class SortInt extends Converter {
  @Override
  public String toInternal(String val) {
    return NumberUtils.int2sortableStr(val);
  }
  @Override
  public String toExternal(String val) {
    return NumberUtils.SortableStr2int(val);
  }
}

class SortLong extends Converter {
  @Override
  public String toInternal(String val) {
    return NumberUtils.long2sortableStr(val);
  }
  @Override
  public String toExternal(String val) {
    return NumberUtils.SortableStr2long(val);
  }
}

class Float2Float extends Converter {
  @Override
  public String toInternal(String val) {
    return Float.toString(Float.parseFloat(val));
  }
  @Override
  public String toExternal(String val) {
    return Float.toString(Float.parseFloat(val));
  }
}

class SortFloat extends Converter {
  @Override
  public String toInternal(String val) {
    return NumberUtils.float2sortableStr(val);
  }
  @Override
  public String toExternal(String val) {
    return NumberUtils.SortableStr2floatStr(val);
  }
}

class SortDouble extends Converter {
  @Override
  public String toInternal(String val) {
    return NumberUtils.double2sortableStr(val);
  }
  @Override
  public String toExternal(String val) {
    return NumberUtils.SortableStr2doubleStr(val);
  }
}

class Base100S extends Converter {
  @Override
  public String toInternal(String val) {
    return BCDUtils.base10toBase100SortableInt(val);
  }
  @Override
  public String toExternal(String val) {
    return BCDUtils.base100SortableIntToBase10(val);
  }
}

class Base10kS extends Converter {
  @Override
  public String toInternal(String val) {
    return BCDUtils.base10toBase10kSortableInt(val);
  }
  @Override
  public String toExternal(String val) {
    return BCDUtils.base10kSortableIntToBase10(val);
  }
}

