/*
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
package org.apache.lucene.search.suggest.document;

import java.io.IOException;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.fst.FST;

/**
 * A {@link PostingsFormat} which supports document suggestion based on indexed {@link
 * SuggestField}s. Document suggestion is based on an weighted FST which map analyzed terms of a
 * {@link SuggestField} to its surface form and document id.
 *
 * <p>Files:
 *
 * <ul>
 *   <li><code>.lkp</code>: <a href="#Completiondictionary">Completion Dictionary</a>
 *   <li><code>.cmp</code>: <a href="#Completionindex">Completion Index</a>
 * </ul>
 *
 * <p><a id="Completionictionary"></a>
 *
 * <h2>Completion Dictionary</h2>
 *
 * <p>The .lkp file contains an FST for each suggest field
 *
 * <ul>
 *   <li>CompletionDict (.lkp) --&gt; Header, FST<sup>NumSuggestFields</sup>, Footer
 *   <li>Header --&gt; {@link CodecUtil#writeHeader CodecHeader}
 *       <!-- TODO: should the FST output be mentioned at all? -->
 *   <li>FST --&gt; {@link FST FST&lt;Long, BytesRef&gt;}
 *   <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}
 * </ul>
 *
 * <p>Notes:
 *
 * <ul>
 *   <li>Header is a {@link CodecUtil#writeHeader CodecHeader} storing the version information for
 *       the Completion implementation.
 *   <li>FST maps all analyzed forms to surface forms of a SuggestField
 * </ul>
 *
 * <a id="Completionindex"></a>
 *
 * <h2>Completion Index</h2>
 *
 * <p>The .cmp file contains an index into the completion dictionary, so that it can be accessed
 * randomly.
 *
 * <ul>
 *   <li>CompletionIndex (.cmp) --&gt; Header, NumSuggestFields, Entry<sup>NumSuggestFields</sup>,
 *       Footer
 *   <li>Header --&gt; {@link CodecUtil#writeHeader CodecHeader}
 *   <li>NumSuggestFields --&gt; {@link DataOutput#writeVInt Uint32}
 *   <li>Entry --&gt; FieldNumber, CompletionDictionaryOffset, MinWeight, MaxWeight, Type
 *   <li>FieldNumber --&gt; {@link DataOutput#writeVInt Uint32}
 *   <li>CompletionDictionaryOffset --&gt; {@link DataOutput#writeVLong Uint64}
 *   <li>MinWeight --&gt; {@link DataOutput#writeVLong Uint64}
 *   <li>MaxWeight --&gt; {@link DataOutput#writeVLong Uint64}
 *   <li>Type --&gt; {@link DataOutput#writeByte Byte}
 *   <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}
 * </ul>
 *
 * <p>Notes:
 *
 * <ul>
 *   <li>Header is a {@link CodecUtil#writeHeader CodecHeader} storing the version information for
 *       the Completion implementation.
 *   <li>NumSuggestFields is the number of suggest fields indexed
 *   <li>FieldNumber is the fields number from {@link FieldInfos}. (.fnm)
 *   <li>CompletionDictionaryOffset is the file offset of a field's FST in CompletionDictionary
 *       (.lkp)
 *   <li>MinWeight and MaxWeight are the global minimum and maximum weight for the field
 *   <li>Type indicates if the suggester has context or not
 * </ul>
 *
 * @lucene.experimental
 */
public abstract class CompletionPostingsFormat extends PostingsFormat {

  static final int COMPLETION_CODEC_VERSION = 1;
  static final int COMPLETION_VERSION_CURRENT = COMPLETION_CODEC_VERSION;
  static final String INDEX_EXTENSION = "cmp";
  static final String DICT_EXTENSION = "lkp";

  /** An enum that allows to control if suggester FSTs are loaded into memory or read off-heap */
  public enum FSTLoadMode {
    /**
     * Always read FSTs from disk. NOTE: If this option is used the FST will be read off-heap even
     * if buffered directory implementations are used.
     */
    OFF_HEAP,
    /** Never read FSTs from disk ie. all suggest fields FSTs are loaded into memory */
    ON_HEAP,
    /**
     * Automatically make the decision if FSTs are read from disk depending if the segment read from
     * an MMAPDirectory
     */
    AUTO
  }

  private final FSTLoadMode fstLoadMode;

  /** Used only by core Lucene at read-time via Service Provider instantiation */
  public CompletionPostingsFormat(String name) {
    this(name, FSTLoadMode.ON_HEAP);
  }

  /**
   * Creates a {@link CompletionPostingsFormat} that will use the provided <code>fstLoadMode</code>
   * to determine if the completion FST should be loaded on or off heap.
   */
  public CompletionPostingsFormat(String name, FSTLoadMode fstLoadMode) {
    super(name);
    this.fstLoadMode = fstLoadMode;
  }

  /** Concrete implementation should specify the delegating postings format */
  protected abstract PostingsFormat delegatePostingsFormat();

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    PostingsFormat delegatePostingsFormat = delegatePostingsFormat();
    if (delegatePostingsFormat == null) {
      throw new UnsupportedOperationException(
          "Error - "
              + getClass().getName()
              + " has been constructed without a choice of PostingsFormat");
    }
    return new CompletionFieldsConsumer(getName(), delegatePostingsFormat, state);
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
    return new CompletionFieldsProducer(getName(), state, fstLoadMode);
  }
}
