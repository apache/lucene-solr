package org.apache.lucene.codecs.stacked;

import java.io.IOException;
import java.util.Set;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PerDocConsumer;
import org.apache.lucene.codecs.PerDocProducer;
import org.apache.lucene.index.PerDocWriteState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;

public class StackedDocValuesFormat extends DocValuesFormat {
 
  DocValuesFormat original;
  
  public StackedDocValuesFormat(DocValuesFormat original) {
    this.original = original;
  }

  @Override
  public PerDocConsumer docsConsumer(PerDocWriteState state) throws IOException {
    return original.docsConsumer(state);
  }
  
  @Override
  public PerDocProducer docsProducer(SegmentReadState state) throws IOException {
    if (state.segmentInfo.getHasStacked()) {
      return new StackedPerDocProducer(original.docsProducer(state), state);
    } else {
      return original.docsProducer(state);
    }
  }
  
  @Override
  public void files(SegmentInfo info, Set<String> files) throws IOException {
    original.files(info, files);
    // XXX add overlay? or should've been done already at the other codec's level?
  }
  
}
