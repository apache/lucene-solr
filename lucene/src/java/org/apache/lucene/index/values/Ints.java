package org.apache.lucene.index.values;

import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.values.PackedIntsImpl.IntsReader;
import org.apache.lucene.index.values.PackedIntsImpl.IntsWriter;
import org.apache.lucene.store.Directory;
//nocommit - add mmap version 
//nocommti - add bulk copy where possible
public class Ints {

  private Ints() {
  }
  
  public static void files(String id, Collection<String> files)
      throws IOException {
    files.add(IndexFileNames.segmentFileName(id, "",
        IndexFileNames.CSF_DATA_EXTENSION));
  }

  public static Writer getWriter(Directory dir, String id, boolean useFixedArray)
      throws IOException {
     //nocommit - implement fixed?!
    return new IntsWriter(dir, id);
  }

  public static Reader getReader(Directory dir, String id, boolean useFixedArray) throws IOException {
    return new IntsReader(dir, id);
  }
}
