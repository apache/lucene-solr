package org.apache.lucene.codecs.stacked;

import java.io.IOException;
import java.util.Set;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene40.Lucene40PostingsFormat;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IOContext;

public class StackedPostingsFormat extends PostingsFormat {
  
  public static final String OVERLAY_EXT = "pov";

  private PostingsFormat original = new Lucene40PostingsFormat();
  
  protected StackedPostingsFormat(String name, PostingsFormat postingsFormat) {
    super(name);
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state)
      throws IOException {
    return original.fieldsConsumer(state);
  }
  
  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state)
      throws IOException {
    FieldsProducer producer = original.fieldsProducer(state);
    if (producer == null) {
      return null;
    }
    if (state.segmentInfo.getHasStacked()) {
      return new StackedFieldsProducer(producer, state);
    } else {
      return producer;
    }
  }
  
  @Override
  public void files(SegmentInfo segmentInfo, String segmentSuffix,
      Set<String> files) throws IOException {
    original.files(segmentInfo, segmentSuffix, files);
  }
  
}
