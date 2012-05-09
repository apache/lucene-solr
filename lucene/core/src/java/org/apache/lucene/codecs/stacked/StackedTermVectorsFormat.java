package org.apache.lucene.codecs.stacked;

import java.io.IOException;
import java.util.Set;

import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

public class StackedTermVectorsFormat extends TermVectorsFormat {
  
  public StackedTermVectorsFormat(TermVectorsFormat termVectorsFormat) {
    // TODO Auto-generated constructor stub
  }

  @Override
  public TermVectorsReader vectorsReader(SegmentReadState state)
      throws IOException {
    // TODO Auto-generated method stub
    return null;
  }
  
  @Override
  public TermVectorsWriter vectorsWriter(Directory directory, String segment,
      IOContext context) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }
  
  @Override
  public void files(SegmentInfo info, Set<String> files) throws IOException {
    // TODO Auto-generated method stub
    
  }
  
}
