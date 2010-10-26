package org.apache.lucene.index.codecs.docvalues;
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.values.DocValues;
import org.apache.lucene.index.values.ValuesAttribute;
import org.apache.lucene.index.values.Writer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

public abstract class DocValuesConsumer {
  public abstract void add(int docID, ValuesAttribute attr) throws IOException;

  public abstract void finish(int docCount) throws IOException;

  public abstract void files(Collection<String> files) throws IOException;
  
  public void merge(List<MergeState> states) throws IOException {
    for (MergeState state : states) {
      merge(state);
    }
  }
  
  protected abstract void merge(MergeState mergeState) throws IOException;
  
  
  public static class MergeState {
    public final DocValues reader;
    public final int docBase;
    public final int docCount;
    public final Bits bits;

    public MergeState(DocValues reader, int docBase, int docCount, Bits bits) {
      assert reader != null;
      this.reader = reader;
      this.docBase = docBase;
      this.docCount = docCount;
      this.bits = bits;
    }
  }

  public static DocValuesConsumer create(String segmentName, Directory directory,
      FieldInfo field, Comparator<BytesRef> comp) throws IOException {
    final String id = segmentName + "_" + field.number;
    return Writer.create(field.getIndexValues(), id, directory, comp);
  }
}
