package org.apache.solr.search.function;

import org.apache.lucene.common.mutable.MutableValue;
import org.apache.lucene.common.mutable.MutableValueBool;
import org.apache.lucene.queries.function.DocValues;
import org.apache.lucene.queries.function.ValueSource;


public abstract class BoolDocValues extends DocValues {
  protected final ValueSource vs;

  public BoolDocValues(ValueSource vs) {
    this.vs = vs;
  }

  @Override
  public abstract boolean boolVal(int doc);

  @Override
  public byte byteVal(int doc) {
    return boolVal(doc) ? (byte)1 : (byte)0;
  }

  @Override
  public short shortVal(int doc) {
    return boolVal(doc) ? (short)1 : (short)0;
  }

  @Override
  public float floatVal(int doc) {
    return boolVal(doc) ? (float)1 : (float)0;
  }

  @Override
  public int intVal(int doc) {
    return boolVal(doc) ? 1 : 0;
  }

  @Override
  public long longVal(int doc) {
    return boolVal(doc) ? (long)1 : (long)0;
  }

  @Override
  public double doubleVal(int doc) {
    return boolVal(doc) ? (double)1 : (double)0;
  }

  @Override
  public String strVal(int doc) {
    return Boolean.toString(boolVal(doc));
  }

  @Override
  public Object objectVal(int doc) {
    return exists(doc) ? boolVal(doc) : null;
  }

  @Override
  public String toString(int doc) {
    return vs.description() + '=' + strVal(doc);
  }

  @Override
  public ValueFiller getValueFiller() {
    return new ValueFiller() {
      private final MutableValueBool mval = new MutableValueBool();

      @Override
      public MutableValue getValue() {
        return mval;
      }

      @Override
      public void fillValue(int doc) {
        mval.value = boolVal(doc);
        mval.exists = exists(doc);
      }
    };
  }
}
