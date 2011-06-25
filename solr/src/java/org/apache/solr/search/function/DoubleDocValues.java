package org.apache.solr.search.function;

import org.apache.lucene.common.mutable.MutableValue;
import org.apache.lucene.common.mutable.MutableValueDouble;

public abstract class DoubleDocValues extends DocValues {
  protected final ValueSource vs;

  public DoubleDocValues(ValueSource vs) {
    this.vs = vs;
  }

  @Override
  public byte byteVal(int doc) {
    return (byte)doubleVal(doc);
  }

  @Override
  public short shortVal(int doc) {
    return (short)doubleVal(doc);
  }

  @Override
  public float floatVal(int doc) {
    return (float)doubleVal(doc);
  }

  @Override
  public int intVal(int doc) {
    return (int)doubleVal(doc);
  }

  @Override
  public long longVal(int doc) {
    return (long)doubleVal(doc);
  }

  @Override
  public boolean boolVal(int doc) {
    return doubleVal(doc) != 0;
  }

  @Override
  public abstract double doubleVal(int doc);

  @Override
  public String strVal(int doc) {
    return Double.toString(doubleVal(doc));
  }

  @Override
  public Object objectVal(int doc) {
    return exists(doc) ? doubleVal(doc) : null;
  }

  @Override
  public String toString(int doc) {
    return vs.description() + '=' + strVal(doc);
  }

  @Override
  public ValueFiller getValueFiller() {
    return new ValueFiller() {
      private final MutableValueDouble mval = new MutableValueDouble();

      @Override
      public MutableValue getValue() {
        return mval;
      }

      @Override
      public void fillValue(int doc) {
        mval.value = doubleVal(doc);
        mval.exists = exists(doc);
      }
    };
  }

}
