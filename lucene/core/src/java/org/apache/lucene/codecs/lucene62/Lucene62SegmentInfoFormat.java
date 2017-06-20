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
package org.apache.lucene.codecs.lucene62;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter; // javadocs
import org.apache.lucene.index.SegmentInfo; // javadocs
import org.apache.lucene.index.SegmentInfos; // javadocs
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataOutput; // javadocs
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Version;

/**
 * Lucene 6.2 Segment info format.
 * <p>
 * Files:
 * <ul>
 *   <li><tt>.si</tt>: Header, SegVersion, SegSize, IsCompoundFile, Diagnostics, Files, Attributes, IndexSort, Footer
 * </ul>
 * Data types:
 * <ul>
 *   <li>Header --&gt; {@link CodecUtil#writeIndexHeader IndexHeader}</li>
 *   <li>SegSize --&gt; {@link DataOutput#writeInt Int32}</li>
 *   <li>SegVersion --&gt; {@link DataOutput#writeString String}</li>
 *   <li>Files --&gt; {@link DataOutput#writeSetOfStrings Set&lt;String&gt;}</li>
 *   <li>Diagnostics,Attributes --&gt; {@link DataOutput#writeMapOfStrings Map&lt;String,String&gt;}</li>
 *   <li>IsCompoundFile --&gt; {@link DataOutput#writeByte Int8}</li>
 *   <li>IndexSort --&gt; {@link DataOutput#writeVInt Int32} count, followed by {@code count} SortField</li>
 *   <li>SortField --&gt; {@link DataOutput#writeString String} field name, followed by {@link DataOutput#writeVInt Int32} sort type ID,
 *       followed by {@link DataOutput#writeByte Int8} indicatating reversed sort, followed by a type-specific encoding of the optional missing value
 *   <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}</li>
 * </ul>
 * Field Descriptions:
 * <ul>
 *   <li>SegVersion is the code version that created the segment.</li>
 *   <li>SegSize is the number of documents contained in the segment index.</li>
 *   <li>IsCompoundFile records whether the segment is written as a compound file or
 *       not. If this is -1, the segment is not a compound file. If it is 1, the segment
 *       is a compound file.</li>
 *   <li>The Diagnostics Map is privately written by {@link IndexWriter}, as a debugging aid,
 *       for each segment it creates. It includes metadata like the current Lucene
 *       version, OS, Java version, why the segment was created (merge, flush,
 *       addIndexes), etc.</li>
 *   <li>Files is a list of files referred to by this segment.</li>
 * </ul>
 *
 * @see SegmentInfos
 * @lucene.experimental
 */
public class Lucene62SegmentInfoFormat extends SegmentInfoFormat {

  /** Sole constructor. */
  public Lucene62SegmentInfoFormat() {
  }

  @Override
  public SegmentInfo read(Directory dir, String segment, byte[] segmentID, IOContext context) throws IOException {
    final String fileName = IndexFileNames.segmentFileName(segment, "", Lucene62SegmentInfoFormat.SI_EXTENSION);
    try (ChecksumIndexInput input = dir.openChecksumInput(fileName, context)) {
      Throwable priorE = null;
      SegmentInfo si = null;
      try {
        int format = CodecUtil.checkIndexHeader(input, Lucene62SegmentInfoFormat.CODEC_NAME,
                                                Lucene62SegmentInfoFormat.VERSION_START,
                                                Lucene62SegmentInfoFormat.VERSION_CURRENT,
                                                segmentID, "");
        final Version version = Version.fromBits(input.readInt(), input.readInt(), input.readInt());

        final int docCount = input.readInt();
        if (docCount < 0) {
          throw new CorruptIndexException("invalid docCount: " + docCount, input);
        }
        final boolean isCompoundFile = input.readByte() == SegmentInfo.YES;

        final Map<String,String> diagnostics = input.readMapOfStrings();
        final Set<String> files = input.readSetOfStrings();
        final Map<String,String> attributes = input.readMapOfStrings();

        int numSortFields = input.readVInt();
        Sort indexSort;
        if (numSortFields > 0) {
          SortField[] sortFields = new SortField[numSortFields];
          for(int i=0;i<numSortFields;i++) {
            String fieldName = input.readString();
            int sortTypeID = input.readVInt();
            SortField.Type sortType;
            SortedSetSelector.Type sortedSetSelector = null;
            SortedNumericSelector.Type sortedNumericSelector = null;
            switch(sortTypeID) {
            case 0:
              sortType = SortField.Type.STRING;
              break;
            case 1:
              sortType = SortField.Type.LONG;
              break;
            case 2:
              sortType = SortField.Type.INT;
              break;
            case 3:
              sortType = SortField.Type.DOUBLE;
              break;
            case 4:
              sortType = SortField.Type.FLOAT;
              break;
            case 5:
              sortType = SortField.Type.STRING;
              byte selector = input.readByte();
              if (selector == 0) {
                sortedSetSelector = SortedSetSelector.Type.MIN;
              } else if (selector == 1) {
                sortedSetSelector = SortedSetSelector.Type.MAX;
              } else if (selector == 2) {
                sortedSetSelector = SortedSetSelector.Type.MIDDLE_MIN;
              } else if (selector == 3) {
                sortedSetSelector = SortedSetSelector.Type.MIDDLE_MAX;
              } else {
                throw new CorruptIndexException("invalid index SortedSetSelector ID: " + selector, input);
              }
              break;
            case 6:
              byte type = input.readByte();
              if (type == 0) {
                sortType = SortField.Type.LONG;
              } else if (type == 1) {
                sortType = SortField.Type.INT;
              } else if (type == 2) {
                sortType = SortField.Type.DOUBLE;
              } else if (type == 3) {
                sortType = SortField.Type.FLOAT;
              } else {
                throw new CorruptIndexException("invalid index SortedNumericSortField type ID: " + type, input);
              }
              byte numericSelector = input.readByte();
              if (numericSelector == 0) {
                sortedNumericSelector = SortedNumericSelector.Type.MIN;
              } else if (numericSelector == 1) {
                sortedNumericSelector = SortedNumericSelector.Type.MAX;
              } else {
                throw new CorruptIndexException("invalid index SortedNumericSelector ID: " + numericSelector, input);
              }
              break;
            default:
              throw new CorruptIndexException("invalid index sort field type ID: " + sortTypeID, input);
            }
            byte b = input.readByte();
            boolean reverse;
            if (b == 0) {
              reverse = true;
            } else if (b == 1) {
              reverse = false;
            } else {
              throw new CorruptIndexException("invalid index sort reverse: " + b, input);
            }

            if (sortedSetSelector != null) {
              sortFields[i] = new SortedSetSortField(fieldName, reverse, sortedSetSelector);
            } else if (sortedNumericSelector != null) {
              sortFields[i] = new SortedNumericSortField(fieldName, sortType, reverse, sortedNumericSelector);
            } else {
              sortFields[i] = new SortField(fieldName, sortType, reverse);
            }

            Object missingValue;
            b = input.readByte();
            if (b == 0) {
              missingValue = null;
            } else {
              switch(sortType) {
              case STRING:
                if (b == 1) {
                  missingValue = SortField.STRING_LAST;
                } else if (b == 2) {
                  missingValue = SortField.STRING_FIRST;
                } else {
                  throw new CorruptIndexException("invalid missing value flag: " + b, input);
                }
                break;
              case LONG:
                if (b != 1) {
                  throw new CorruptIndexException("invalid missing value flag: " + b, input);
                }
                missingValue = input.readLong();
                break;
              case INT:
                if (b != 1) {
                  throw new CorruptIndexException("invalid missing value flag: " + b, input);
                }
                missingValue = input.readInt();
                break;
              case DOUBLE:
                if (b != 1) {
                  throw new CorruptIndexException("invalid missing value flag: " + b, input);
                }
                missingValue = Double.longBitsToDouble(input.readLong());
                break;
              case FLOAT:
                if (b != 1) {
                  throw new CorruptIndexException("invalid missing value flag: " + b, input);
                }
                missingValue = Float.intBitsToFloat(input.readInt());
                break;
              default:
                throw new AssertionError("unhandled sortType=" + sortType);
              }
            }
            if (missingValue != null) {
              sortFields[i].setMissingValue(missingValue);
            }
          }
          indexSort = new Sort(sortFields);
        } else if (numSortFields < 0) {
          throw new CorruptIndexException("invalid index sort field count: " + numSortFields, input);
        } else {
          indexSort = null;
        }

        si = new SegmentInfo(dir, version, null, segment, docCount, isCompoundFile, null, diagnostics, segmentID, attributes, indexSort);
        si.setFiles(files);
      } catch (Throwable exception) {
        priorE = exception;
      } finally {
        CodecUtil.checkFooter(input, priorE);
      }
      return si;
    }
  }

  @Override
  public void write(Directory dir, SegmentInfo info, IOContext ioContext) throws IOException {
    throw new UnsupportedOperationException("This format can only be used for reading");
  }

  /** File extension used to store {@link SegmentInfo}. */
  public final static String SI_EXTENSION = "si";
  static final String CODEC_NAME = "Lucene62SegmentInfo";
  static final int VERSION_START = 0;
  static final int VERSION_MULTI_VALUED_SORT = 1;
  static final int VERSION_CURRENT = VERSION_MULTI_VALUED_SORT;
}
