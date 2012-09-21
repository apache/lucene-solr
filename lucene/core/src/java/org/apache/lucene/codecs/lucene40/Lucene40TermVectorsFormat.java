package org.apache.lucene.codecs.lucene40;

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

import java.io.IOException;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.DataOutput; // javadocs
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

/**
 * Lucene 4.0 Term Vectors format.
 * <p>Term Vector support is an optional on a field by field basis. It consists of
 * 3 files.</p>
 * <ol>
 * <li><a name="tvx" id="tvx"></a>
 * <p>The Document Index or .tvx file.</p>
 * <p>For each document, this stores the offset into the document data (.tvd) and
 * field data (.tvf) files.</p>
 * <p>DocumentIndex (.tvx) --&gt; Header,&lt;DocumentPosition,FieldPosition&gt;
 * <sup>NumDocs</sup></p>
 * <ul>
 *   <li>Header --&gt; {@link CodecUtil#writeHeader CodecHeader}</li>
 *   <li>DocumentPosition --&gt; {@link DataOutput#writeLong UInt64} (offset in the .tvd file)</li>
 *   <li>FieldPosition --&gt; {@link DataOutput#writeLong UInt64} (offset in the .tvf file)</li>
 * </ul>
 * </li>
 * <li><a name="tvd" id="tvd"></a>
 * <p>The Document or .tvd file.</p>
 * <p>This contains, for each document, the number of fields, a list of the fields
 * with term vector info and finally a list of pointers to the field information
 * in the .tvf (Term Vector Fields) file.</p>
 * <p>The .tvd file is used to map out the fields that have term vectors stored
 * and where the field information is in the .tvf file.</p>
 * <p>Document (.tvd) --&gt; Header,&lt;NumFields, FieldNums,
 * FieldPositions&gt; <sup>NumDocs</sup></p>
 * <ul>
 *   <li>Header --&gt; {@link CodecUtil#writeHeader CodecHeader}</li>
 *   <li>NumFields --&gt; {@link DataOutput#writeVInt VInt}</li>
 *   <li>FieldNums --&gt; &lt;FieldNumDelta&gt; <sup>NumFields</sup></li>
 *   <li>FieldNumDelta --&gt; {@link DataOutput#writeVInt VInt}</li>
 *   <li>FieldPositions --&gt; &lt;FieldPositionDelta&gt; <sup>NumFields-1</sup></li>
 *   <li>FieldPositionDelta --&gt; {@link DataOutput#writeVLong VLong}</li>
 * </ul>
 * </li>
 * <li><a name="tvf" id="tvf"></a>
 * <p>The Field or .tvf file.</p>
 * <p>This file contains, for each field that has a term vector stored, a list of
 * the terms, their frequencies and, optionally, position, offset, and payload
 * information.</p>
 * <p>Field (.tvf) --&gt; Header,&lt;NumTerms, Flags, TermFreqs&gt;
 * <sup>NumFields</sup></p>
 * <ul>
 *   <li>Header --&gt; {@link CodecUtil#writeHeader CodecHeader}</li>
 *   <li>NumTerms --&gt; {@link DataOutput#writeVInt VInt}</li>
 *   <li>Flags --&gt; {@link DataOutput#writeByte Byte}</li>
 *   <li>TermFreqs --&gt; &lt;TermText, TermFreq, Positions?, PayloadData?, Offsets?&gt;
 *       <sup>NumTerms</sup></li>
 *   <li>TermText --&gt; &lt;PrefixLength, Suffix&gt;</li>
 *   <li>PrefixLength --&gt; {@link DataOutput#writeVInt VInt}</li>
 *   <li>Suffix --&gt; {@link DataOutput#writeString String}</li>
 *   <li>TermFreq --&gt; {@link DataOutput#writeVInt VInt}</li>
 *   <li>Positions --&gt; &lt;PositionDelta PayloadLength?&gt;<sup>TermFreq</sup></li>
 *   <li>PositionDelta --&gt; {@link DataOutput#writeVInt VInt}</li>
 *   <li>PayloadLength --&gt; {@link DataOutput#writeVInt VInt}</li>
 *   <li>PayloadData --&gt; {@link DataOutput#writeByte Byte}<sup>NumPayloadBytes</sup></li>
 *   <li>Offsets --&gt; &lt;{@link DataOutput#writeVInt VInt}, {@link DataOutput#writeVInt VInt}&gt;<sup>TermFreq</sup></li>
 * </ul>
 * <p>Notes:</p>
 * <ul>
 * <li>Flags byte stores whether this term vector has position, offset, payload.
 * information stored.</li>
 * <li>Term byte prefixes are shared. The PrefixLength is the number of initial
 * bytes from the previous term which must be pre-pended to a term's suffix
 * in order to form the term's bytes. Thus, if the previous term's text was "bone"
 * and the term is "boy", the PrefixLength is two and the suffix is "y".</li>
 * <li>PositionDelta is, if payloads are disabled for the term's field, the
 * difference between the position of the current occurrence in the document and
 * the previous occurrence (or zero, if this is the first occurrence in this
 * document). If payloads are enabled for the term's field, then PositionDelta/2
 * is the difference between the current and the previous position. If payloads
 * are enabled and PositionDelta is odd, then PayloadLength is stored, indicating
 * the length of the payload at the current term position.</li>
 * <li>PayloadData is metadata associated with a term position. If
 * PayloadLength is stored at the current position, then it indicates the length
 * of this payload. If PayloadLength is not stored, then this payload has the same
 * length as the payload at the previous position. PayloadData encodes the 
 * concatenated bytes for all of a terms occurrences.</li>
 * <li>Offsets are stored as delta encoded VInts. The first VInt is the
 * startOffset, the second is the endOffset.</li>
 * </ul>
 * </li>
 * </ol>
 */
public class Lucene40TermVectorsFormat extends TermVectorsFormat {

  /** Sole constructor. */
  public Lucene40TermVectorsFormat() {
  }
  
  @Override
  public TermVectorsReader vectorsReader(Directory directory, SegmentInfo segmentInfo, FieldInfos fieldInfos, IOContext context) throws IOException {
    return new Lucene40TermVectorsReader(directory, segmentInfo, fieldInfos, context);
  }

  @Override
  public TermVectorsWriter vectorsWriter(Directory directory, SegmentInfo segmentInfo, IOContext context) throws IOException {
    return new Lucene40TermVectorsWriter(directory, segmentInfo.name, context);
  }
}
