package org.apache.lucene.backward_codecs.lucene80;

import java.io.IOException;
import org.apache.lucene.codecs.NormsConsumer;
import org.apache.lucene.index.SegmentWriteState;

public class Lucene80RWNormsFormat extends Lucene80NormsFormat {

  @Override
  public NormsConsumer normsConsumer(SegmentWriteState state) throws IOException {
    return new Lucene80NormsConsumer(
        state, DATA_CODEC, DATA_EXTENSION, METADATA_CODEC, METADATA_EXTENSION);
  }
}
