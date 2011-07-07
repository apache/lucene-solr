package org.apache.lucene.index.codecs;

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
import java.util.Comparator;
import java.util.Set;

import org.apache.lucene.index.PerDocWriteState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

/** @lucene.experimental */
public abstract class Codec {
  public static final Codec[] EMPTY = new Codec[0];
  /** Unique name that's used to retrieve this codec when
   *  reading the index */
  public final String name;
  private boolean dvUseCompoundFile = true;
  private Comparator<BytesRef> docValuesSortComparator = BytesRef
      .getUTF8SortedAsUnicodeComparator();
  
  protected Codec(String name) {
    this.name = name;
  }

  /** Writes a new segment */
  public abstract FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException;

  public static void debug(String s, String desc) {
    if (desc != null) {
      System.out.println(Thread.currentThread().getName()+ " [" + desc + "]:" + s);
    } else {
      System.out.println(Thread.currentThread().getName() + ": " + s);
    }
  }
  public static void debug(String s) {
    debug(s, null);
  }

  /** Reads a segment.  NOTE: by the time this call
   *  returns, it must hold open any files it will need to
   *  use; else, those files may be deleted. */
  public abstract FieldsProducer fieldsProducer(SegmentReadState state) throws IOException;
  
  public abstract PerDocConsumer docsConsumer(PerDocWriteState state) throws IOException;
  
  public abstract PerDocValues docsProducer(SegmentReadState state) throws IOException;

  /**
   * Gathers files associated with this segment
   * 
   * @param dir the {@link Directory} this segment was written to
   * @param segmentInfo the {@link SegmentInfo} for this segment 
   * @param id the codec id within this segment
   * @param files the of files to add the codec files to.
   */
  public abstract void files(Directory dir, SegmentInfo segmentInfo, int id, Set<String> files) throws IOException;

  /** Records all file extensions this codec uses */
  public abstract void getExtensions(Set<String> extensions);
  

  /**
   * If set to <code>true</code> this codec will use a compound file for
   * IndexDocValues, otherwise each IndexDocValues field will create up to 2
   * files per segment.
   * <p>
   * NOTE: The default values is <code>true</code>.
   */
  public void setDocValuesUseCFS(boolean docValuesUseCFS) {
    this.dvUseCompoundFile = docValuesUseCFS;
  }

  /**
   * Returns <code>true</code> iff compound file should be used for
   * IndexDocValues, otherwise <code>false</code>.
   * 
   * @see #setDocValuesUseCFS(boolean);
   * @return <code>true</code> iff compound file should be used for
   *         IndexDocValues, otherwise <code>false</code>.
   */
  public boolean getDocValuesUseCFS() {
    return dvUseCompoundFile;
  }
  
  /**
   * Sets the {@link BytesRef} comparator for sorted IndexDocValue variants. The
   * default is {@link BytesRef#getUTF8SortedAsUnicodeComparator()}. *
   */
  public void setDocValuesSortComparator(
      Comparator<BytesRef> docValuesSortComparator) {
    this.docValuesSortComparator = docValuesSortComparator;
  }

  /**
   * Returns the {@link BytesRef} comparator for sorted IndexDocValue variants.
   * The default is {@link BytesRef#getUTF8SortedAsUnicodeComparator()}.
   */
  public Comparator<BytesRef> getDocValuesSortComparator() {
    return docValuesSortComparator;
  }
  
  @Override
  public String toString() {
    return name;
  }
}
