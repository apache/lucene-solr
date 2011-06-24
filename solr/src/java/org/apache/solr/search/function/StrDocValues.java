package org.apache.solr.search.function;

import org.apache.lucene.util.BytesRef;
import org.apache.solr.search.MutableValue;
import org.apache.solr.search.MutableValueFloat;
import org.apache.solr.search.MutableValueStr;

public abstract class StrDocValues extends DocValues {
  protected final ValueSource vs;

  public StrDocValues(ValueSource vs) {
    this.vs = vs;
  }

  @Override
  public abstract String strVal(int doc);

  @Override
  public Object objectVal(int doc) {
    return exists(doc) ? strVal(doc) : null;
  }

  @Override
  public boolean boolVal(int doc) {
    return exists(doc);
  }

  @Override
  public String toString(int doc) {
    return vs.description() + "='" + strVal(doc) + "'";
  }

  @Override
  public ValueFiller getValueFiller() {
    return new ValueFiller() {
      private final MutableValueStr mval = new MutableValueStr();

      @Override
      public MutableValue getValue() {
        return mval;
      }

      @Override
      public void fillValue(int doc) {
        mval.exists = bytesVal(doc, mval.value);
      }
    };
  }
}
