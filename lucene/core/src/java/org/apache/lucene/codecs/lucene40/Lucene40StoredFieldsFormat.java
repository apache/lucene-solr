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
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.DataOutput; // javadocs
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

/** 
 * Lucene 4.0 Stored Fields Format.
 * <p>Stored fields are represented by two files:</p>
 * <ol>
 * <li><a name="field_index" id="field_index"></a>
 * <p>The field index, or <tt>.fdx</tt> file.</p>
 * <p>This is used to find the location within the field data file of the fields
 * of a particular document. Because it contains fixed-length data, this file may
 * be easily randomly accessed. The position of document <i>n</i> 's field data is
 * the {@link DataOutput#writeLong Uint64} at <i>n*8</i> in this file.</p>
 * <p>This contains, for each document, a pointer to its field data, as
 * follows:</p>
 * <ul>
 * <li>FieldIndex (.fdx) --&gt; &lt;Header&gt;, &lt;FieldValuesPosition&gt; <sup>SegSize</sup></li>
 * <li>Header --&gt; {@link CodecUtil#writeHeader CodecHeader}</li>
 * <li>FieldValuesPosition --&gt; {@link DataOutput#writeLong Uint64}</li>
 * </ul>
 * </li>
 * <li>
 * <p><a name="field_data" id="field_data"></a>The field data, or <tt>.fdt</tt> file.</p>
 * <p>This contains the stored fields of each document, as follows:</p>
 * <ul>
 * <li>FieldData (.fdt) --&gt; &lt;Header&gt;, &lt;DocFieldData&gt; <sup>SegSize</sup></li>
 * <li>Header --&gt; {@link CodecUtil#writeHeader CodecHeader}</li>
 * <li>DocFieldData --&gt; FieldCount, &lt;FieldNum, Bits, Value&gt;
 * <sup>FieldCount</sup></li>
 * <li>FieldCount --&gt; {@link DataOutput#writeVInt VInt}</li>
 * <li>FieldNum --&gt; {@link DataOutput#writeVInt VInt}</li>
 * <li>Bits --&gt; {@link DataOutput#writeByte Byte}</li>
 * <ul>
 * <li>low order bit reserved.</li>
 * <li>second bit is one for fields containing binary data</li>
 * <li>third bit reserved.</li>
 * <li>4th to 6th bit (mask: 0x7&lt;&lt;3) define the type of a numeric field:
 * <ul>
 * <li>all bits in mask are cleared if no numeric field at all</li>
 * <li>1&lt;&lt;3: Value is Int</li>
 * <li>2&lt;&lt;3: Value is Long</li>
 * <li>3&lt;&lt;3: Value is Int as Float (as of {@link Float#intBitsToFloat(int)}</li>
 * <li>4&lt;&lt;3: Value is Long as Double (as of {@link Double#longBitsToDouble(long)}</li>
 * </ul>
 * </li>
 * </ul>
 * <li>Value --&gt; String | BinaryValue | Int | Long (depending on Bits)</li>
 * <li>BinaryValue --&gt; ValueSize, &lt;{@link DataOutput#writeByte Byte}&gt;^ValueSize</li>
 * <li>ValueSize --&gt; {@link DataOutput#writeVInt VInt}</li>
 * </li>
 * </ul>
 * </ol>
 * @lucene.experimental */
public class Lucene40StoredFieldsFormat extends StoredFieldsFormat {

  /** Sole constructor. */
  public Lucene40StoredFieldsFormat() {
  }

  @Override
  public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo si,
      FieldInfos fn, IOContext context) throws IOException {
    return new Lucene40StoredFieldsReader(directory, si, fn, context);
  }

  @Override
  public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si,
      IOContext context) throws IOException {
    return new Lucene40StoredFieldsWriter(directory, si.name, context);
  }
}
