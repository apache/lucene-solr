package org.apache.solr.search.function;

import org.apache.lucene.common.mutable.MutableValue;
import org.apache.lucene.common.mutable.MutableValueInt;


public abstract class IntDocValues extends DocValues {
  protected final ValueSource vs;

  public IntDocValues(ValueSource vs) {
    this.vs = vs;
  }

  @Override
  public byte byteVal(int doc) {
    return (byte)intVal(doc);
  }

  @Override
  public short shortVal(int doc) {
    return (short)intVal(doc);
  }

  @Override
  public float floatVal(int doc) {
    return (float)intVal(doc);
  }

  @Override
  public abstract int intVal(int doc);

  @Override
  public long longVal(int doc) {
    return (long)intVal(doc);
  }

  @Override
  public double doubleVal(int doc) {
    return (double)intVal(doc);
  }

  @Override
  public String strVal(int doc) {
    return Integer.toString(intVal(doc));
  }

  @Override
  public Object objectVal(int doc) {
    return exists(doc) ? intVal(doc) : null;
  }

  @Override
  public String toString(int doc) {
    return vs.description() + '=' + strVal(doc);
  }

  @Override
  public ValueFiller getValueFiller() {
    return new ValueFiller() {
      private final MutableValueInt mval = new MutableValueInt();

      @Override
      public MutableValue getValue() {
        return mval;
      }

      @Override
      public void fillValue(int doc) {
        mval.value = intVal(doc);
        mval.exists = exists(doc);
      }
    };
  }
}
