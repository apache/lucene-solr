package org.apache.solr.search.function;

import org.apache.solr.search.MutableValue;
import org.apache.solr.search.MutableValueLong;

public abstract class LongDocValues extends DocValues {
  protected final ValueSource vs;

  public LongDocValues(ValueSource vs) {
    this.vs = vs;
  }

  @Override
  public byte byteVal(int doc) {
    return (byte)longVal(doc);
  }

  @Override
  public short shortVal(int doc) {
    return (short)longVal(doc);
  }

  @Override
  public float floatVal(int doc) {
    return (float)longVal(doc);
  }

  @Override
  public int intVal(int doc) {
    return (int)longVal(doc);
  }

  @Override
  public abstract long longVal(int doc);

  @Override
  public double doubleVal(int doc) {
    return (double)longVal(doc);
  }

  @Override
  public boolean boolVal(int doc) {
    return longVal(doc) != 0;
  }

  @Override
  public String strVal(int doc) {
    return Long.toString(longVal(doc));
  }

  @Override
  public Object objectVal(int doc) {
    return exists(doc) ? longVal(doc) : null;
  }

  @Override
  public String toString(int doc) {
    return vs.description() + '=' + strVal(doc);
  }

  @Override
  public ValueFiller getValueFiller() {
    return new ValueFiller() {
      private final MutableValueLong mval = new MutableValueLong();

      @Override
      public MutableValue getValue() {
        return mval;
      }

      @Override
      public void fillValue(int doc) {
        mval.value = longVal(doc);
        mval.exists = exists(doc);
      }
    };
  }
}
