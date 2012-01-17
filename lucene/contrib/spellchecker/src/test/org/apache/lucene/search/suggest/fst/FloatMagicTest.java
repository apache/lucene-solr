package org.apache.lucene.search.suggest.fst;

import java.util.*;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;
import org.junit.Ignore;
import org.junit.Test;

public class FloatMagicTest extends LuceneTestCase {
  public void testFloatMagic() {
    ArrayList<Float> floats = new ArrayList<Float>(Arrays.asList(
        Float.intBitsToFloat(0x7f800001), // NaN (invalid combination).
        Float.intBitsToFloat(0x7fffffff), // NaN (invalid combination).
        Float.intBitsToFloat(0xff800001), // NaN (invalid combination).
        Float.intBitsToFloat(0xffffffff), // NaN (invalid combination).
        Float.POSITIVE_INFINITY,
        Float.MAX_VALUE,
        100f,
        0f,
        0.1f,
        Float.MIN_VALUE,
        Float.NaN,
        -0.0f,
        -Float.MIN_VALUE,
        -0.1f,
        -1f,
        -10f,
        Float.NEGATIVE_INFINITY));

    // Sort them using juc.
    Collections.sort(floats);
    
    // Convert to sortable int4 representation (as long to have an unsigned sort).
    long [] int4 = new long [floats.size()];
    for (int i = 0; i < floats.size(); i++) {
      int4[i] = FloatMagic.toSortable(floats.get(i)) & 0xffffffffL;

      /*
      System.out.println(
          String.format("raw %8s sortable %8s %8s numutils %8s %s",
              Integer.toHexString(Float.floatToRawIntBits(floats.get(i))),
              Integer.toHexString(FloatMagic.toSortable(floats.get(i))),
              Integer.toHexString(FloatMagic.unsignedOrderedToFloatBits(FloatMagic.toSortable(floats.get(i)))),
              Integer.toHexString(NumericUtils.floatToSortableInt(floats.get(i))),
              floats.get(i)));
      */
    }

    // Sort and compare. Should be identical order.
    Arrays.sort(int4);
    ArrayList<Float> backFromFixed = new ArrayList<Float>();
    for (int i = 0; i < int4.length; i++) {
      backFromFixed.add(FloatMagic.fromSortable((int) int4[i]));
    }

    /*
    for (int i = 0; i < int4.length; i++) {
      System.out.println(
          floats.get(i) + " " + FloatMagic.fromSortable((int) int4[i]));
    }
    */
    
    assertEquals(floats, backFromFixed);
  }

  @Ignore("Once checked, valid forever?") @Test
  public void testRoundTripFullRange() {
    int i = 0;
    do {
      float f = Float.intBitsToFloat(i);
      float f2 = FloatMagic.fromSortable(FloatMagic.toSortable(f));
      
      if (!((Float.isNaN(f) && Float.isNaN(f2)) || f == f2)) {
        throw new RuntimeException("! " + Integer.toHexString(i) + "> " + f + " " + f2); 
      }

      if ((i & 0xffffff) == 0) {
        System.out.println(Integer.toHexString(i));
      }
      
      i++;
    } while (i != 0);
  }
  
  @Ignore("Once checked, valid forever?") @Test
  public void testIncreasingFullRange() {
    // -infinity ... -0.0
    for (int i = 0xff800000; i != 0x80000000; i--) {
      checkSmaller(i, i - 1); 
    }
    
    // -0.0 +0.0
    checkSmaller(0x80000000, 0);

    // +0.0 ... +infinity
    for (int i = 0; i != 0x7f800000; i++) {
      checkSmaller(i, i + 1); 
    }

    // All other are NaNs and should be after positive infinity.
    final long infinity = toSortableL(Float.POSITIVE_INFINITY);
    for (int i = 0x7f800001; i != 0x7fffffff; i++) {
      assertTrue(infinity < toSortableL(Float.intBitsToFloat(i)));
    }
    for (int i = 0xff800001; i != 0xffffffff; i++) {
      assertTrue(infinity < toSortableL(Float.intBitsToFloat(i)));
    }
  }

  private long toSortableL(float f) {
    return FloatMagic.toSortable(f) & 0xffffffffL;
  }

  private void checkSmaller(int i1, int i2) {
    float f1 = Float.intBitsToFloat(i1);
    float f2 = Float.intBitsToFloat(i2);
    if (f1 > f2) {
      throw new AssertionError(f1 + " " + f2 + " " + i1 + " " + i2);
    }
    assertTrue(toSortableL(f1) < toSortableL(f2));
  }
}
