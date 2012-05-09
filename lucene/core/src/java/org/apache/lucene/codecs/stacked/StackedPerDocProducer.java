package org.apache.lucene.codecs.stacked;

import java.io.IOException;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PerDocProducer;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SegmentReadState;

public class StackedPerDocProducer extends PerDocProducer {
  PerDocProducer original;
  
  public StackedPerDocProducer(PerDocProducer original, SegmentReadState state) {
    this.original = original;
  }

  @Override
  public void close() throws IOException {
    if (original != null) {
      original.close();
    }
  }
  
  @Override
  public DocValues docValues(String field) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }
  
}
