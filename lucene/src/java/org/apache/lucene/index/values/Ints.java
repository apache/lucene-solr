package org.apache.lucene.index.values;

import java.io.IOException;

import org.apache.lucene.index.values.PackedIntsImpl.IntsReader;
import org.apache.lucene.index.values.PackedIntsImpl.IntsWriter;
import org.apache.lucene.store.Directory;
//TODO - add bulk copy where possible
public class Ints {

  private Ints() {
  }
  

  public static Writer getWriter(Directory dir, String id, boolean useFixedArray)
      throws IOException {
     //TODO - implement fixed?!
    return new IntsWriter(dir, id);
  }

  public static DocValues getValues(Directory dir, String id, boolean useFixedArray) throws IOException {
    return new IntsReader(dir, id);
  }
}
