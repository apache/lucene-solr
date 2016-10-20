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
package org.apache.lucene.codecs.lucene70;

import java.io.IOException;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.NormsConsumer;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.DataOutput;

/**
 * Lucene 7.0 Score normalization format.
 * <p>
 * Encodes normalization values by encoding each value with the minimum
 * number of bytes needed to represent the range (which can be zero).
 * <p>
 * Files:
 * <ol>
 *   <li><tt>.nvd</tt>: Norms data</li>
 *   <li><tt>.nvm</tt>: Norms metadata</li>
 * </ol>
 * <ol>
 *   <li><a name="nvm"></a>
 *   <p>The Norms metadata or .nvm file.</p>
 *   <p>For each norms field, this stores metadata, such as the offset into the 
 *      Norms data (.nvd)</p>
 *   <p>Norms metadata (.dvm) --&gt; Header,&lt;Entry&gt;<sup>NumFields</sup>,Footer</p>
 *   <ul>
 *     <li>Header --&gt; {@link CodecUtil#writeIndexHeader IndexHeader}</li>
 *     <li>Entry --&gt; FieldNumber, DocsWithFieldAddress, DocsWithFieldLength, NumDocsWithField, BytesPerNorm, NormsAddress</li>
 *     <li>FieldNumber --&gt; {@link DataOutput#writeInt Int32}</li>
 *     <li>DocsWithFieldAddress --&gt; {@link DataOutput#writeLong Int64}</li>
 *     <li>DocsWithFieldLength --&gt; {@link DataOutput#writeLong Int64}</li>
 *     <li>NumDocsWithField --&gt; {@link DataOutput#writeInt Int32}</li>
 *     <li>BytesPerNorm --&gt; {@link DataOutput#writeByte byte}</li>
 *     <li>NormsAddress --&gt; {@link DataOutput#writeLong Int64}</li>
 *     <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}</li>
 *   </ul>
 *   <p>FieldNumber of -1 indicates the end of metadata.</p>
 *   <p>NormsAddress is the pointer to the start of the data in the norms data (.nvd), or the singleton value 
 *      when BytesPerValue = 0. If BytesPerValue is different from 0 then there are NumDocsWithField values
 *      to read at that offset.</p>
 *   <p>DocsWithFieldAddress is the pointer to the start of the bit set containing documents that have a norm
 *      in the norms data (.nvd), or -2 if no documents have a norm value, or -1 if all documents have a norm
 *      value.</p>
 *   <p>DocsWithFieldLength is the number of bytes used to encode the set of documents that have a norm.</p>
 *   <li><a name="nvd"></a>
 *   <p>The Norms data or .nvd file.</p>
 *   <p>For each Norms field, this stores the actual per-document data (the heavy-lifting)</p>
 *   <p>Norms data (.nvd) --&gt; Header,&lt; Data &gt;<sup>NumFields</sup>,Footer</p>
 *   <ul>
 *     <li>Header --&gt; {@link CodecUtil#writeIndexHeader IndexHeader}</li>
 *     <li>DocsWithFieldData --&gt; Bit set of MaxDoc bits</li>
 *     <li>NormsData --&gt; {@link DataOutput#writeByte(byte) byte}<sup>NumDocsWithField * BytesPerValue</sup></li>
 *     <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}</li>
 *   </ul>
 * </ol>
 * @lucene.experimental
 */
public class Lucene70NormsFormat extends NormsFormat {

  /** Sole Constructor */
  public Lucene70NormsFormat() {}
  
  @Override
  public NormsConsumer normsConsumer(SegmentWriteState state) throws IOException {
    return new Lucene70NormsConsumer(state, DATA_CODEC, DATA_EXTENSION, METADATA_CODEC, METADATA_EXTENSION);
  }

  @Override
  public NormsProducer normsProducer(SegmentReadState state) throws IOException {
    return new Lucene70NormsProducer(state, DATA_CODEC, DATA_EXTENSION, METADATA_CODEC, METADATA_EXTENSION);
  }
  
  private static final String DATA_CODEC = "Lucene70NormsData";
  private static final String DATA_EXTENSION = "nvd";
  private static final String METADATA_CODEC = "Lucene70NormsMetadata";
  private static final String METADATA_EXTENSION = "nvm";
  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;
}
