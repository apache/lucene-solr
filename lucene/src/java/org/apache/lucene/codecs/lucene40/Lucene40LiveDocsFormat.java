package org.apache.lucene.codecs.lucene40;

import java.io.IOException;
import java.util.Set;

import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.MutableBits;

public class Lucene40LiveDocsFormat extends LiveDocsFormat {

  /** Extension of deletes */
  static final String DELETES_EXTENSION = "del";
  
  @Override
  public MutableBits newLiveDocs(int size) throws IOException {
    BitVector bitVector = new BitVector(size);
    bitVector.invertAll();
    return bitVector;
  }

  @Override
  public MutableBits newLiveDocs(Bits existing) throws IOException {
    final BitVector liveDocs = (BitVector) existing;
    return liveDocs.clone();
  }

  @Override
  public Bits readLiveDocs(Directory dir, SegmentInfo info, IOContext context) throws IOException {
    String filename = IndexFileNames.fileNameFromGeneration(info.name, DELETES_EXTENSION, info.getDelGen());
    final BitVector liveDocs = new BitVector(dir, filename, context);
    assert liveDocs.count() == info.docCount - info.getDelCount();
    assert liveDocs.length() == info.docCount;
    return liveDocs;
  }

  @Override
  public void writeLiveDocs(MutableBits bits, Directory dir, SegmentInfo info, IOContext context) throws IOException {
    String filename = IndexFileNames.fileNameFromGeneration(info.name, DELETES_EXTENSION, info.getDelGen());
    final BitVector liveDocs = (BitVector) bits;
    assert liveDocs.count() == info.docCount - info.getDelCount();
    assert liveDocs.length() == info.docCount;
    liveDocs.write(dir, filename, context);
  }

  @Override
  public void separateFiles(Directory dir, SegmentInfo info, Set<String> files) throws IOException {
    if (info.hasDeletions()) {
      files.add(IndexFileNames.fileNameFromGeneration(info.name, DELETES_EXTENSION, info.getDelGen()));
    }
  }
}
