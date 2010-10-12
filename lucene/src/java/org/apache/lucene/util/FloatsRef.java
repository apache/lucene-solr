/**
 * 
 */
package org.apache.lucene.util;


public final class FloatsRef implements Cloneable{
  public double[] floats;
  public int offset;
  public int length;

  public FloatsRef() {
  }

  public FloatsRef(int capacity) {
    floats = new double[capacity];
  }
  
  public void set(double value) {
    floats[offset] = value;
  }
  
  public double get() {
    return floats[offset];
  }

  public FloatsRef(double[] floats, int offset, int length) {
    this.floats = floats;
    this.offset = offset;
    this.length = length;
  }

  public FloatsRef(FloatsRef other) {
    copy(other);
  }

  @Override
  public Object clone() {
    return new FloatsRef(this);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 0;
    final int end = offset + length;
    for(int i = offset; i < end; i++) {
      long value = Double.doubleToLongBits(floats[i]);
      result = prime * result + (int) (value ^ (value >>> 32));
    }
    return result;
  }
  
  @Override
  public boolean equals(Object other) {
    return other instanceof FloatsRef && this.floatsEquals((FloatsRef) other);
  }

  public boolean floatsEquals(FloatsRef other) {
    if (length == other.length) {
      int otherUpto = other.offset;
      final double[] otherFloats = other.floats;
      final int end = offset + length;
      for(int upto=offset;upto<end;upto++,otherUpto++) {
        if (floats[upto] != otherFloats[otherUpto]) {
          return false;
        }
      }
      return true;
    } else {
      return false;
    }
  }

  public void copy(FloatsRef other) {
    if (floats == null) {
      floats = new double[other.length];
    } else {
      floats = ArrayUtil.grow(floats, other.length);
    }
    System.arraycopy(other.floats, other.offset, floats, 0, other.length);
    length = other.length;
    offset = 0;
  }

  public void grow(int newLength) {
    if (floats.length < newLength) {
      floats = ArrayUtil.grow(floats, newLength);
    }
  }
}