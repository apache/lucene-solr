package org.apache.lucene.codecs.preflexrw;

import java.io.IOException;

import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.lucene3x.Lucene3xStoredFieldsFormat;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

public class PreFlexRWStoredFieldsFormat extends Lucene3xStoredFieldsFormat {

  @Override
  public StoredFieldsWriter fieldsWriter(Directory directory, String segment, IOContext context) throws IOException {
    return new PreFlexRWStoredFieldsWriter(directory, segment, context);
  }
  
}
