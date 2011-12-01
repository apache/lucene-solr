package org.apache.lucene.search.suggest.fst;

import org.apache.lucene.util.NumericUtils;

/**
 * Converts normalized float representations ({@link Float#floatToIntBits(float)})
 * into integers that are directly sortable in int4 representation (or unsigned values or
 * after promoting to a long with higher 32-bits zeroed).
 */
class FloatMagic {
  /**
   * Convert a float to a directly sortable unsigned integer. For sortable signed
   * integers, see {@link NumericUtils#floatToSortableInt(float)}.
   */
  public static int toSortable(float f) {
    return floatBitsToUnsignedOrdered(Float.floatToRawIntBits(f));
  }

  /**
   * Back from {@link #toSortable(float)} to float.
   */
  public static float fromSortable(int v) {
    return Float.intBitsToFloat(unsignedOrderedToFloatBits(v));
  }

  /**
   * Convert float bits to directly sortable bits. 
   * Normalizes all NaNs to canonical form.
   */
  static int floatBitsToUnsignedOrdered(int v) {
    // Canonicalize NaN ranges. I assume this check will be faster here than 
    // (v == v) == false on the FPU? We don't distinguish between different
    // flavors of NaNs here (see http://en.wikipedia.org/wiki/NaN). I guess
    // in Java this doesn't matter much anyway.
    if ((v & 0x7fffffff) > 0x7f800000) {
      // Apply the logic below to a canonical "quiet NaN"
      return 0x7fc00000 ^ 0x80000000;
    }

    if (v < 0) {
      // Reverse the order of negative values and push them before positive values.
      return ~v;
    } else {
      // Shift positive values after negative, but before NaNs, they're sorted already.
      return v ^ 0x80000000;
    }
  }

  /**
   * Back from {@link #floatBitsToUnsignedOrdered(int)}.
   */
  static int unsignedOrderedToFloatBits(int v) {
    if (v < 0)
      return v & ~0x80000000;
    else
      return ~v; 
  }
}
