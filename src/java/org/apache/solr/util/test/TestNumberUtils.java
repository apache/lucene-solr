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

package org.apache.solr.util.test;

import org.apache.solr.util.NumberUtils;
import org.apache.solr.util.BCDUtils;

import java.util.Random;

/**
 * @author yonik
 */

public class TestNumberUtils {

  private static String arrstr(char[] arr, int start, int end) {
    String str="[";
    for (int i=start; i<end; i++) str += arr[i]+"("+(int)arr[i]+"),";
    return str+"]";
  }

  static Random rng = new Random();

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

    int c1 = n1.compareTo(n2);
    int c2 = v1.compareTo(v2);
    if (c1==0 && !(c2==0) || c1 < 0 && !(c2<0) || c1>0 && !(c2>0)
        || !out1.equals(s1) || !out2.equals(s2))
    {
      System.out.println("Comparison error:"+s1+","+s2);
      System.out.print("v1=");
      for (int ii=0; ii<v1.length(); ii++) {
        System.out.print(" " + (int)v1.charAt(ii));
      }
      System.out.print("\nv2=");
      for (int ii=0; ii<v2.length(); ii++) {
        System.out.print(" " + (int)v2.charAt(ii));
      }
      System.out.println("\nout1='"+out1+"', out2='" + out2 + "'");
    }
  }


  public static void main(String[] args) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    int iter=1000000;
    int arrsz=100000;
    int ret=0;
    int num=0;

    String test="b100";
    String clazz="NoClass";

    for (int argnum=0; argnum<args.length; argnum++) {
      String arg = args[argnum];
      if ("-t".equals(arg)) { test=args[++argnum]; }
      if ("-i".equals(arg)) { iter=Integer.parseInt(args[++argnum]); }
      if ("-a".equals(arg)) { arrsz=Integer.parseInt(args[++argnum]); }
      if ("-c".equals(arg)) { clazz=args[++argnum]; }
      if ("-r".equals(arg)) { rng.setSeed(Long.parseLong(args[++argnum])); };
      if ("-n".equals(arg)) { num = Integer.parseInt(args[++argnum]); };

    }

    // Converter conv = (Converter)(Class.forName(clazz).newInstance());
    Class cls=null;
    try {
      cls = Class.forName(clazz);
    } catch (Exception e) {
      cls = Class.forName("solr.util.test." + clazz);
    }
    Converter conv = (Converter)cls.newInstance();

    long startTime = System.currentTimeMillis();

    if ("ispecial".equals(test)) {
     for (int i=0; i<iter; i++) {
        Integer i1 = getSpecial();
        Integer i2 = getSpecial();
        test(i1,i2,conv);
      }
    }
    else if ("lspecial".equals(test)) {
     for (int i=0; i<iter; i++) {
        Long f1 = getLongSpecial();
        Long f2 = getLongSpecial();
        test(f1,f2,conv);
      }
    }
    else if ("fspecial".equals(test)) {
     for (int i=0; i<iter; i++) {
        Float f1 = getFloatSpecial();
        Float f2 = getFloatSpecial();
        test(f1,f2,conv);
      }
    }
    else if ("dspecial".equals(test)) {
     for (int i=0; i<iter; i++) {
        Double f1 = getDoubleSpecial();
        Double f2 = getDoubleSpecial();
        test(f1,f2,conv);
      }
    }
    else if ("10kout".equals(test)) {
      String n = Integer.toString(num);
      char[] arr = new char[n.length()];
      char[] arr2 = new char[n.length()+1];
      n.getChars(0,n.length(),arr,0);
      for (int i=0; i<iter; i++) {
        ret += BCDUtils.base10toBase100SortableInt(arr,0,arr.length,arr2,arr2.length);
      }

    } else if ("internal".equals(test) || "external".equals(test)) {
      int min=-1000000; int max=1000000;
      String[] arr = new String[arrsz];
      String[] internal = new String[arrsz];

      if ("external".equals(test)) {
        for (int i=0; i<arrsz; i++) {
          int val = rng.nextInt();
          // todo - move to between min and max...
          arr[i] = Integer.toString(rng.nextInt());
          internal[i] = conv.toInternal(arr[i]);
        }
        for (int i=0; i<iter; i++) {
          int slot=i%arrsz;
          arr[slot] = conv.toExternal(internal[slot]);
          ret += arr[slot].length();
        }
      } else {
        for (int i=0; i<arrsz; i++) {
          int val = rng.nextInt();
          // todo - move to between min and max...
          arr[i] = Integer.toString(rng.nextInt());
        }
        for (int i=0; i<iter; i++) {
          int slot=i%arrsz;
          internal[slot] = conv.toInternal(arr[slot]);
          ret += internal[slot].length();
        }
      }
    } else if ("itest".equals(test) || "ltest".equals(test) || "ftest".equals(test)) {
      long internalLen=0;
      long externalLen=0;
      for (int i=0; i<iter; i++) {
        Comparable n1=null,n2=null;

        if ("itest".equals(test)) {
          Integer i1
                  = rng.nextInt();
          Integer i2
                  = rng.nextInt();

          // concentrate on small numbers for a while
          // to try and hit boundary cases 0,1,-1,100,-100,etc
          if (i < 10000) {
            i1 = (i1 % 250)-125;
            i2 = (i2 % 250)-125;
          } else if (i < 500000) {
            i1 = (i1 % 25000)-12500;
            i2 = (i2 % 25000)-12500;
          }

          n1=i1;
          n2=i2;
        } else if ("ltest".equals(test)) {
          Long i1 = rng.nextLong();
          Long i2 = rng.nextLong();

          // concentrate on small numbers for a while
          // to try and hit boundary cases 0,1,-1,100,-100,etc
          if (i < 10000) {
            i1 = (long)(i1 % 250)-125;
            i2 = (long)(i2 % 250)-125;
          } else if (i < 500000) {
            i1 = (long)(i1 % 25000)-12500;
            i2 = (long)(i2 % 25000)-12500;
          }

          n1=i1;
          n2=i2;
        } else if ("ftest".equals(test)) {
          Float i1;
          Float i2;
          if (i < 10000) {
            i1 = (float)(rng.nextInt() % 250)-125;
            i2 = (float)(rng.nextInt() % 250)-125;
          } else if (i < 300000) {
            i1 = (float)(rng.nextInt() % 2500)-1250;
            i2 = (float)(rng.nextInt() % 2500)-1250;
          } else if (i < 500000) {
            i1 = rng.nextFloat() / rng.nextFloat();
            i2 = rng.nextFloat() / rng.nextFloat();
          } else {
            i1 = Float.intBitsToFloat(rng.nextInt());
            i2 = Float.intBitsToFloat(rng.nextInt());
          }
          n1=i1;
          n2=i2;
        }
        String s1=n1.toString();
        String s2=n2.toString();
        String v1 = conv.toInternal(s1);
        String v2 = conv.toInternal(s2);
        String out1=conv.toExternal(v1);
        String out2=conv.toExternal(v2);

        externalLen += s1.length();
        internalLen += v1.length();

        int c1 = n1.compareTo(n2);
        int c2 = v1.compareTo(v2);
        if (c1==0 && !(c2==0) || c1 < 0 && !(c2<0) || c1>0 && !(c2>0)
            || !out1.equals(s1) || !out2.equals(s2))
        {
          System.out.println("Comparison error:"+s1+","+s2);
          System.out.print("v1=");
          for (int ii=0; ii<v1.length(); ii++) {
            System.out.print(" " + (int)v1.charAt(ii));
          }
          System.out.print("\nv2=");
          for (int ii=0; ii<v2.length(); ii++) {
            System.out.print(" " + (int)v2.charAt(ii));
          }
          System.out.println("\nout1='"+out1+"', out2='" + out2 + "'");

        }
      }
    }


    /******************
    int sz=20;
    char[] arr1 = new char[sz];
    char[] arr2 = new char[sz];
    char[] arr3 = new char[sz];
    if ("noconv".equals(test)) {
      for (int i=0; i<iter; i++) {
        int val = rng.nextInt();
        String istr = Integer.toString(val);
        int n = istr.length();
        Integer.toString(val).getChars(0, n, arr1, 0);
        String nStr = new String(arr1,0,n);
        if (!nStr.equals(istr)) {
          System.out.println("ERROR! input="+istr+" output="+nStr);
          System.out.println(arrstr(arr1,0,n));
        }
      }
    } else if ("b100".equals(test)) {
      for (int i=0; i<iter; i++) {
        int val = rng.nextInt();
        String istr = Integer.toString(val);
        int n = istr.length();
        Integer.toString(val).getChars(0, n, arr1, 0);

        int b100_start = NumberUtils.base10toBase100(arr1,0,n,arr2,sz);
        int b10_len = NumberUtils.base100toBase10(arr2,b100_start,sz,arr3,0);

        String nStr = new String(arr3,0,b10_len);
        if (!nStr.equals(istr)) {
          System.out.println("ERROR! input="+istr+" output="+nStr);
          System.out.println(arrstr(arr1,0,n));
          System.out.println(arrstr(arr2,b100_start,sz));
          System.out.println(arrstr(arr3,0,b10_len));
        }

      }
    } else if ("b100sParse".equals(test)) {
      int min=-1000000; int max=1000000;
      String[] arr = new String[arrsz];
      String[] internal = new String[arrsz];
      for (int i=0; i<arrsz; i++) {
        int val = rng.nextInt();
        // todo - move to between min and max...
        arr[i] = Integer.toString(rng.nextInt());
      }
      for (int i=0; i<iter; i++) {
        int slot=i%arrsz;
        internal[slot] = NumberUtils.base10toBase100SortableInt(arr[i%arrsz]);
        ret += internal[slot].length();
      }
    } else if ("intParse".equals(test)) {
      int min=-1000000; int max=1000000;
      String[] arr = new String[arrsz];
      String[] internal = new String[arrsz];
      for (int i=0; i<arrsz; i++) {
        int val = rng.nextInt();
        // todo - move to between min and max...
        arr[i] = Integer.toString(rng.nextInt());
      }
      for (int i=0; i<iter; i++) {
        int slot=i%arrsz;
        int val = Integer.parseInt(arr[i%arrsz]);
        String sval = Integer.toString(val);
        internal[slot] = sval;
        ret += internal[slot].length();
      }
    } else if ("b100s".equals(test)) {
      for (int i=0; i<iter; i++) {
        Integer i1 = rng.nextInt();
        Integer i2 = rng.nextInt();

        // concentrate on small numbers for a while
        // to try and hit boundary cases 0,1,-1,100,-100,etc
        if (iter < 10000) {
          i1 = (i1 % 250)-125;
          i2 = (i2 % 250)-125;
        } else if (iter < 500000) {
          i1 = (i1 % 25000)-12500;
          i2 = (i2 % 25000)-12500;
        }

        String s1=Integer.toString(i1);
        String s2=Integer.toString(i2);
        String v1 = NumberUtils.base10toBase10kSortableInt(s1);
        String v2 = NumberUtils.base10toBase10kSortableInt(s2);
        String out1=NumberUtils.base10kSortableIntToBase10(v1);
        String out2=NumberUtils.base10kSortableIntToBase10(v2);

        int c1 = i1.compareTo(i2);
        int c2 = v1.compareTo(v2);
        if (c1==0 && c2 !=0 || c1 < 0 && c2 >= 0 || c1 > 0 && c2 <=0
            || !out1.equals(s1) || !out2.equals(s2))
        {
          System.out.println("Comparison error:"+s1+","+s2);
          System.out.print("v1=");
          for (int ii=0; ii<v1.length(); ii++) {
            System.out.print(" " + (int)v1.charAt(ii));
          }
          System.out.print("\nv2=");
          for (int ii=0; ii<v2.length(); ii++) {
            System.out.print(" " + (int)v2.charAt(ii));
          }
          System.out.println("\nout1='"+out1+"', out2='" + out2 + "'");

        }





      }
    }
    ****/

    long endTime = System.currentTimeMillis();
    System.out.println("time="+(endTime-startTime));
    System.out.println("ret="+ret);
  }
}


interface Converter {
  String toInternal(String val);
  String toExternal(String val);
}

class Int2Int implements Converter {
  public String toInternal(String val) {
    return Integer.toString(Integer.parseInt(val));
  }
  public String toExternal(String val) {
    return Integer.toString(Integer.parseInt(val));
  }
}

class SortInt implements Converter {
  public String toInternal(String val) {
    return NumberUtils.int2sortableStr(val);
  }
  public String toExternal(String val) {
    return NumberUtils.SortableStr2int(val);
  }
}

class SortLong implements Converter {
  public String toInternal(String val) {
    return NumberUtils.long2sortableStr(val);
  }
  public String toExternal(String val) {
    return NumberUtils.SortableStr2long(val);
  }
}

class Float2Float implements Converter {
  public String toInternal(String val) {
    return Float.toString(Float.parseFloat(val));
  }
  public String toExternal(String val) {
    return Float.toString(Float.parseFloat(val));
  }
}

class SortFloat implements Converter {
  public String toInternal(String val) {
    return NumberUtils.float2sortableStr(val);
  }
  public String toExternal(String val) {
    return NumberUtils.SortableStr2floatStr(val);
  }
}

class SortDouble implements Converter {
  public String toInternal(String val) {
    return NumberUtils.double2sortableStr(val);
  }
  public String toExternal(String val) {
    return NumberUtils.SortableStr2doubleStr(val);
  }
}

class Base100S implements Converter {
  public String toInternal(String val) {
    return BCDUtils.base10toBase100SortableInt(val);
  }
  public String toExternal(String val) {
    return BCDUtils.base100SortableIntToBase10(val);
  }
}

class Base10kS implements Converter {
  public String toInternal(String val) {
    return BCDUtils.base10toBase10kSortableInt(val);
  }
  public String toExternal(String val) {
    return BCDUtils.base10kSortableIntToBase10(val);
  }
}
