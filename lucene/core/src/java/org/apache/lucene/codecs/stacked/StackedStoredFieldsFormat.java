package org.apache.lucene.codecs.stacked;

import java.io.IOException;
import java.util.Set;

import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

public class StackedStoredFieldsFormat extends StoredFieldsFormat {
  
  public StackedStoredFieldsFormat(StoredFieldsFormat storedFieldsFormat) {
    // TODO Auto-generated constructor stub
  }

  @Override
  public StoredFieldsReader fieldsReader(SegmentReadState state) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }
  
  @Override
  public StoredFieldsWriter fieldsWriter(Directory directory, String segment,
      IOContext context) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }
  
  @Override
  public void files(SegmentInfo info, Set<String> files) throws IOException {
    // TODO Auto-generated method stub
    
  }
  
}
