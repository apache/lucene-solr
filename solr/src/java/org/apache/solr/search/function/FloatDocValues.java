package org.apache.solr.search.function;

import org.apache.solr.search.MutableValue;
import org.apache.solr.search.MutableValueFloat;

public abstract class FloatDocValues extends DocValues {
  protected final ValueSource vs;

  public FloatDocValues(ValueSource vs) {
    this.vs = vs;
  }

  @Override
  public byte byteVal(int doc) {
    return (byte)floatVal(doc);
  }

  @Override
  public short shortVal(int doc) {
    return (short)floatVal(doc);
  }

  @Override
  public abstract float floatVal(int doc);

  @Override
  public int intVal(int doc) {
    return (int)floatVal(doc);
  }

  @Override
  public long longVal(int doc) {
    return (long)floatVal(doc);
  }

  @Override
  public double doubleVal(int doc) {
    return (double)floatVal(doc);
  }

  @Override
  public String strVal(int doc) {
    return Float.toString(floatVal(doc));
  }

  @Override
  public Object objectVal(int doc) {
    return exists(doc) ? floatVal(doc) : null;
  }

  @Override
  public String toString(int doc) {
    return vs.description() + '=' + strVal(doc);
  }

  @Override
  public ValueFiller getValueFiller() {
    return new ValueFiller() {
      private final MutableValueFloat mval = new MutableValueFloat();

      @Override
      public MutableValue getValue() {
        return mval;
      }

      @Override
      public void fillValue(int doc) {
        mval.value = floatVal(doc);
        mval.exists = exists(doc);
      }
    };
  }
}
