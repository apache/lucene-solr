package org.apache.lucene.codecs.lucene40;

import java.io.IOException;
import java.util.Set;

import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.MutableBits;

public class Lucene40LiveDocsFormat extends LiveDocsFormat {

  @Override
  public MutableBits newLiveDocs(int size) throws IOException {
    BitVector bitVector = new BitVector(size);
    bitVector.invertAll();
    return bitVector;
  }

  @Override
  public Bits readLiveDocs(Directory dir, SegmentInfo info, IOContext context) throws IOException {
    // nocommit: compute filename here
    return new BitVector(dir, info.getDelFileName(), context);
  }

  @Override
  public void writeLiveDocs(MutableBits bits, Directory dir, SegmentInfo info, IOContext context) throws IOException {
    // nocommit: compute filename here
    // nocommit: this api is ugly...
    ((BitVector)bits).write(dir, info.getDelFileName(), context);
  }

  @Override
  public void files(Directory dir, SegmentInfo info, Set<String> files) throws IOException {
    // nocommit: compute filename here
    if (info.hasDeletions()) {
      files.add(info.getDelFileName());
    }
  }
}
