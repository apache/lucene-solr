/**
 * 
 */
package org.apache.lucene.util;


public final class LongsRef implements Cloneable {
  public long[] ints;
  public int offset;
  public int length;

  public LongsRef() {
  }

  public LongsRef(int capacity) {
    ints = new long[capacity];
  }

  public LongsRef(long[] ints, int offset, int length) {
    this.ints = ints;
    this.offset = offset;
    this.length = length;
  }

  public LongsRef(LongsRef other) {
    copy(other);
  }

  @Override
  public Object clone() {
    return new LongsRef(this);
  }
  
  public void set(long value) {
    ints[offset] = value;
  }
  
  public long get() {
    return ints[offset];
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 0;
    final int end = offset + length;
    for(int i = offset; i < end; i++) {
      long value = ints[i];
      result = prime * result + (int) (value ^ (value >>> 32));
    }
    return result;
  }
  
  @Override
  public boolean equals(Object other) {
    return this.intsEquals((LongsRef) other);
  }

  public boolean intsEquals(LongsRef other) {
    if (length == other.length) {
      int otherUpto = other.offset;
      final long[] otherInts = other.ints;
      final int end = offset + length;
      for(int upto=offset;upto<end;upto++,otherUpto++) {
        if (ints[upto] != otherInts[otherUpto]) {
          return false;
        }
      }
      return true;
    } else {
      return false;
    }
  }

  public void copy(LongsRef other) {
    if (ints == null) {
      ints = new long[other.length];
    } else {
      ints = ArrayUtil.grow(ints, other.length);
    }
    System.arraycopy(other.ints, other.offset, ints, 0, other.length);
    length = other.length;
    offset = 0;
  }

  public void grow(int newLength) {
    if (ints.length < newLength) {
      ints = ArrayUtil.grow(ints, newLength);
    }
  }
}