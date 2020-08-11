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
import java.util.Set;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Version;

/**
 * Writable version of Lucene70SegmentInfoFormat for testing
 */
public class Lucene70RWSegmentInfoFormat extends Lucene70SegmentInfoFormat {

  @Override
  public void write(Directory dir, SegmentInfo si, IOContext ioContext) throws IOException {
    final String fileName = IndexFileNames.segmentFileName(si.name, "", Lucene70SegmentInfoFormat.SI_EXTENSION);

    try (IndexOutput output = dir.createOutput(fileName, ioContext)) {
      // Only add the file once we've successfully created it, else IFD assert can trip:
      si.addFile(fileName);
      CodecUtil.writeIndexHeader(output,
          Lucene70SegmentInfoFormat.CODEC_NAME,
          Lucene70SegmentInfoFormat.VERSION_CURRENT,
          si.getId(),
          "");
      Version version = si.getVersion();
      if (version.major < 7) {
        throw new IllegalArgumentException("invalid major version: should be >= 7 but got: " + version.major + " segment=" + si);
      }
      // Write the Lucene version that created this segment, since 3.1
      output.writeInt(version.major);
      output.writeInt(version.minor);
      output.writeInt(version.bugfix);

      // Write the min Lucene version that contributed docs to the segment, since 7.0
      if (si.getMinVersion() != null) {
        output.writeByte((byte) 1);
        Version minVersion = si.getMinVersion();
        output.writeInt(minVersion.major);
        output.writeInt(minVersion.minor);
        output.writeInt(minVersion.bugfix);
      } else {
        output.writeByte((byte) 0);
      }

      assert version.prerelease == 0;
      output.writeInt(si.maxDoc());

      output.writeByte((byte) (si.getUseCompoundFile() ? SegmentInfo.YES : SegmentInfo.NO));
      output.writeMapOfStrings(si.getDiagnostics());
      Set<String> files = si.files();
      for (String file : files) {
        if (!IndexFileNames.parseSegmentName(file).equals(si.name)) {
          throw new IllegalArgumentException("invalid files: expected segment=" + si.name + ", got=" + files);
        }
      }
      output.writeSetOfStrings(files);
      output.writeMapOfStrings(si.getAttributes());

      Sort indexSort = si.getIndexSort();
      int numSortFields = indexSort == null ? 0 : indexSort.getSort().length;
      output.writeVInt(numSortFields);
      for (int i = 0; i < numSortFields; ++i) {
        SortField sortField = indexSort.getSort()[i];
        SortField.Type sortType = sortField.getType();
        output.writeString(sortField.getField());
        int sortTypeID;
        switch (sortField.getType()) {
          case STRING:
            sortTypeID = 0;
            break;
          case LONG:
            sortTypeID = 1;
            break;
          case INT:
            sortTypeID = 2;
            break;
          case DOUBLE:
            sortTypeID = 3;
            break;
          case FLOAT:
            sortTypeID = 4;
            break;
          case CUSTOM:
            if (sortField instanceof SortedSetSortField) {
              sortTypeID = 5;
              sortType = SortField.Type.STRING;
            } else if (sortField instanceof SortedNumericSortField) {
              sortTypeID = 6;
              sortType = ((SortedNumericSortField) sortField).getNumericType();
            } else {
              throw new IllegalStateException("Unexpected SortedNumericSortField " + sortField);
            }
            break;
          default:
            throw new IllegalStateException("Unexpected sort type: " + sortField.getType());
        }
        output.writeVInt(sortTypeID);
        if (sortTypeID == 5) {
          SortedSetSortField ssf = (SortedSetSortField) sortField;
          if (ssf.getSelector() == SortedSetSelector.Type.MIN) {
            output.writeByte((byte) 0);
          } else if (ssf.getSelector() == SortedSetSelector.Type.MAX) {
            output.writeByte((byte) 1);
          } else if (ssf.getSelector() == SortedSetSelector.Type.MIDDLE_MIN) {
            output.writeByte((byte) 2);
          } else if (ssf.getSelector() == SortedSetSelector.Type.MIDDLE_MAX) {
            output.writeByte((byte) 3);
          } else {
            throw new IllegalStateException("Unexpected SortedSetSelector type: " + ssf.getSelector());
          }
        } else if (sortTypeID == 6) {
          SortedNumericSortField snsf = (SortedNumericSortField) sortField;
          if (snsf.getNumericType() == SortField.Type.LONG) {
            output.writeByte((byte) 0);
          } else if (snsf.getNumericType() == SortField.Type.INT) {
            output.writeByte((byte) 1);
          } else if (snsf.getNumericType() == SortField.Type.DOUBLE) {
            output.writeByte((byte) 2);
          } else if (snsf.getNumericType() == SortField.Type.FLOAT) {
            output.writeByte((byte) 3);
          } else {
            throw new IllegalStateException("Unexpected SortedNumericSelector type: " + snsf.getNumericType());
          }
          if (snsf.getSelector() == SortedNumericSelector.Type.MIN) {
            output.writeByte((byte) 0);
          } else if (snsf.getSelector() == SortedNumericSelector.Type.MAX) {
            output.writeByte((byte) 1);
          } else {
            throw new IllegalStateException("Unexpected sorted numeric selector type: " + snsf.getSelector());
          }
        }
        output.writeByte((byte) (sortField.getReverse() ? 0 : 1));

        // write missing value
        Object missingValue = sortField.getMissingValue();
        if (missingValue == null) {
          output.writeByte((byte) 0);
        } else {
          switch(sortType) {
            case STRING:
              if (missingValue == SortField.STRING_LAST) {
                output.writeByte((byte) 1);
              } else if (missingValue == SortField.STRING_FIRST) {
                output.writeByte((byte) 2);
              } else {
                throw new AssertionError("unrecognized missing value for STRING field \"" + sortField.getField() + "\": " + missingValue);
              }
              break;
            case LONG:
              output.writeByte((byte) 1);
              output.writeLong(((Long) missingValue).longValue());
              break;
            case INT:
              output.writeByte((byte) 1);
              output.writeInt(((Integer) missingValue).intValue());
              break;
            case DOUBLE:
              output.writeByte((byte) 1);
              output.writeLong(Double.doubleToLongBits(((Double) missingValue).doubleValue()));
              break;
            case FLOAT:
              output.writeByte((byte) 1);
              output.writeInt(Float.floatToIntBits(((Float) missingValue).floatValue()));
              break;
            default:
              throw new IllegalStateException("Unexpected sort type: " + sortField.getType());
          }
        }
      }

      CodecUtil.writeFooter(output);
    }
  }

}
