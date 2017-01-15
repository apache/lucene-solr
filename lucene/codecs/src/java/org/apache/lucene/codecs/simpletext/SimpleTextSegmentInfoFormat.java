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
package org.apache.lucene.codecs.simpletext;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;

/**
 * plain text segments file format.
 * <p>
 * <b>FOR RECREATIONAL USE ONLY</b>
 * @lucene.experimental
 */
public class SimpleTextSegmentInfoFormat extends SegmentInfoFormat {
  final static BytesRef SI_VERSION          = new BytesRef("    version ");
  final static BytesRef SI_DOCCOUNT         = new BytesRef("    number of documents ");
  final static BytesRef SI_USECOMPOUND      = new BytesRef("    uses compound file ");
  final static BytesRef SI_NUM_DIAG         = new BytesRef("    diagnostics ");
  final static BytesRef SI_DIAG_KEY         = new BytesRef("      key ");
  final static BytesRef SI_DIAG_VALUE       = new BytesRef("      value ");
  final static BytesRef SI_NUM_ATT          = new BytesRef("    attributes ");
  final static BytesRef SI_ATT_KEY          = new BytesRef("      key ");
  final static BytesRef SI_ATT_VALUE        = new BytesRef("      value ");
  final static BytesRef SI_NUM_FILES        = new BytesRef("    files ");
  final static BytesRef SI_FILE             = new BytesRef("      file ");
  final static BytesRef SI_ID               = new BytesRef("    id ");
  final static BytesRef SI_SORT             = new BytesRef("    sort ");
  final static BytesRef SI_SORT_FIELD       = new BytesRef("      field ");
  final static BytesRef SI_SORT_TYPE        = new BytesRef("      type ");
  final static BytesRef SI_SELECTOR_TYPE    = new BytesRef("      selector ");
  final static BytesRef SI_SORT_REVERSE     = new BytesRef("      reverse ");
  final static BytesRef SI_SORT_MISSING     = new BytesRef("      missing ");

  public static final String SI_EXTENSION = "si";
  
  @Override
  public SegmentInfo read(Directory directory, String segmentName, byte[] segmentID, IOContext context) throws IOException {
    BytesRefBuilder scratch = new BytesRefBuilder();
    String segFileName = IndexFileNames.segmentFileName(segmentName, "", SimpleTextSegmentInfoFormat.SI_EXTENSION);
    try (ChecksumIndexInput input = directory.openChecksumInput(segFileName, context)) {
      SimpleTextUtil.readLine(input, scratch);
      assert StringHelper.startsWith(scratch.get(), SI_VERSION);
      final Version version;
      try {
        version = Version.parse(readString(SI_VERSION.length, scratch));
      } catch (ParseException pe) {
        throw new CorruptIndexException("unable to parse version string: " + pe.getMessage(), input, pe);
      }
    
      SimpleTextUtil.readLine(input, scratch);
      assert StringHelper.startsWith(scratch.get(), SI_DOCCOUNT);
      final int docCount = Integer.parseInt(readString(SI_DOCCOUNT.length, scratch));
    
      SimpleTextUtil.readLine(input, scratch);
      assert StringHelper.startsWith(scratch.get(), SI_USECOMPOUND);
      final boolean isCompoundFile = Boolean.parseBoolean(readString(SI_USECOMPOUND.length, scratch));
    
      SimpleTextUtil.readLine(input, scratch);
      assert StringHelper.startsWith(scratch.get(), SI_NUM_DIAG);
      int numDiag = Integer.parseInt(readString(SI_NUM_DIAG.length, scratch));
      Map<String,String> diagnostics = new HashMap<>();

      for (int i = 0; i < numDiag; i++) {
        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch.get(), SI_DIAG_KEY);
        String key = readString(SI_DIAG_KEY.length, scratch);
      
        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch.get(), SI_DIAG_VALUE);
        String value = readString(SI_DIAG_VALUE.length, scratch);
        diagnostics.put(key, value);
      }
      
      SimpleTextUtil.readLine(input, scratch);
      assert StringHelper.startsWith(scratch.get(), SI_NUM_ATT);
      int numAtt = Integer.parseInt(readString(SI_NUM_ATT.length, scratch));
      Map<String,String> attributes = new HashMap<>(numAtt);

      for (int i = 0; i < numAtt; i++) {
        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch.get(), SI_ATT_KEY);
        String key = readString(SI_ATT_KEY.length, scratch);
      
        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch.get(), SI_ATT_VALUE);
        String value = readString(SI_ATT_VALUE.length, scratch);
        attributes.put(key, value);
      }
      
      SimpleTextUtil.readLine(input, scratch);
      assert StringHelper.startsWith(scratch.get(), SI_NUM_FILES);
      int numFiles = Integer.parseInt(readString(SI_NUM_FILES.length, scratch));
      Set<String> files = new HashSet<>();

      for (int i = 0; i < numFiles; i++) {
        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch.get(), SI_FILE);
        String fileName = readString(SI_FILE.length, scratch);
        files.add(fileName);
      }
      
      SimpleTextUtil.readLine(input, scratch);
      assert StringHelper.startsWith(scratch.get(), SI_ID);
      final byte[] id = Arrays.copyOfRange(scratch.bytes(), SI_ID.length, scratch.length());
      
      if (!Arrays.equals(segmentID, id)) {
        throw new CorruptIndexException("file mismatch, expected: " + StringHelper.idToString(segmentID)
                                                        + ", got: " + StringHelper.idToString(id), input);
      }

      SimpleTextUtil.readLine(input, scratch);
      assert StringHelper.startsWith(scratch.get(), SI_SORT);
      final int numSortFields = Integer.parseInt(readString(SI_SORT.length, scratch));
      SortField[] sortField = new SortField[numSortFields];
      for (int i = 0; i < numSortFields; ++i) {
        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch.get(), SI_SORT_FIELD);
        final String field = readString(SI_SORT_FIELD.length, scratch);

        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch.get(), SI_SORT_TYPE);
        final String typeAsString = readString(SI_SORT_TYPE.length, scratch);

        final SortField.Type type;
        SortedSetSelector.Type selectorSet = null;
        SortedNumericSelector.Type selectorNumeric = null;
        switch (typeAsString) {
          case "string":
            type = SortField.Type.STRING;
            break;
          case "long":
            type = SortField.Type.LONG;
            break;
          case "int":
            type = SortField.Type.INT;
            break;
          case "double":
            type = SortField.Type.DOUBLE;
            break;
          case "float":
            type = SortField.Type.FLOAT;
            break;
          case "multi_valued_string":
            type = SortField.Type.STRING;
            selectorSet = readSetSelector(input, scratch);
            break;
          case "multi_valued_long":
            type = SortField.Type.LONG;
            selectorNumeric = readNumericSelector(input, scratch);
            break;
          case "multi_valued_int":
            type = SortField.Type.INT;
            selectorNumeric = readNumericSelector(input, scratch);
            break;
          case "multi_valued_double":
            type = SortField.Type.DOUBLE;
            selectorNumeric = readNumericSelector(input, scratch);
            break;
          case "multi_valued_float":
            type = SortField.Type.FLOAT;
            selectorNumeric = readNumericSelector(input, scratch);
            break;
          default:
            throw new CorruptIndexException("unable to parse sort type string: " + typeAsString, input);
        }

        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch.get(), SI_SORT_REVERSE);
        final boolean reverse = Boolean.parseBoolean(readString(SI_SORT_REVERSE.length, scratch));

        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch.get(), SI_SORT_MISSING);
        final String missingLastAsString = readString(SI_SORT_MISSING.length, scratch);
        final Object missingValue;
        switch (type) {
          case STRING:
            switch (missingLastAsString) {
              case "null":
                missingValue = null;
                break;
              case "first":
                missingValue = SortField.STRING_FIRST;
                break;
              case "last":
                missingValue = SortField.STRING_LAST;
                break;
              default:
                throw new CorruptIndexException("unable to parse missing string: " + typeAsString, input);
            }
            break;
          case LONG:
            switch (missingLastAsString) {
              case "null":
                missingValue = null;
                break;
              default:
                missingValue = Long.parseLong(missingLastAsString);
                break;
            }
            break;
          case INT:
            switch (missingLastAsString) {
              case "null":
                missingValue = null;
                break;
              default:
                missingValue = Integer.parseInt(missingLastAsString);
                break;
            }
            break;
          case DOUBLE:
            switch (missingLastAsString) {
              case "null":
                missingValue = null;
                break;
              default:
                missingValue = Double.parseDouble(missingLastAsString);
                break;
            }
            break;
          case FLOAT:
            switch (missingLastAsString) {
              case "null":
                missingValue = null;
                break;
              default:
                missingValue = Float.parseFloat(missingLastAsString);
                break;
            }
            break;
          default:
            throw new AssertionError();
        }
        if (selectorSet != null) {
          sortField[i] = new SortedSetSortField(field, reverse);
        } else if (selectorNumeric != null) {
          sortField[i] = new SortedNumericSortField(field, type, reverse);
        } else {
          sortField[i] = new SortField(field, type, reverse);
        }
        if (missingValue != null) {
          sortField[i].setMissingValue(missingValue);
        }
      }
      Sort indexSort = sortField.length == 0 ? null : new Sort(sortField);

      SimpleTextUtil.checkFooter(input);

      SegmentInfo info = new SegmentInfo(directory, version, segmentName, docCount,
                                         isCompoundFile, null, Collections.unmodifiableMap(diagnostics),
                                         id, Collections.unmodifiableMap(attributes), indexSort);
      info.setFiles(files);
      return info;
    }
  }

  private String readString(int offset, BytesRefBuilder scratch) {
    return new String(scratch.bytes(), offset, scratch.length()-offset, StandardCharsets.UTF_8);
  }

  private SortedSetSelector.Type readSetSelector(IndexInput input, BytesRefBuilder scratch) throws IOException {
    SimpleTextUtil.readLine(input, scratch);
    assert StringHelper.startsWith(scratch.get(), SI_SELECTOR_TYPE);
    final String selectorAsString = readString(SI_SELECTOR_TYPE.length, scratch);
    switch (selectorAsString) {
      case "min":
        return SortedSetSelector.Type.MIN;
      case "middle_min":
        return SortedSetSelector.Type.MIDDLE_MIN;
      case "middle_max":
        return SortedSetSelector.Type.MIDDLE_MAX;
      case "max":
        return SortedSetSelector.Type.MAX;
      default:
        throw new CorruptIndexException("unable to parse SortedSetSelector type: " + selectorAsString, input);
    }
  }

  private SortedNumericSelector.Type readNumericSelector(IndexInput input, BytesRefBuilder scratch) throws IOException {
    SimpleTextUtil.readLine(input, scratch);
    assert StringHelper.startsWith(scratch.get(), SI_SELECTOR_TYPE);
    final String selectorAsString = readString(SI_SELECTOR_TYPE.length, scratch);
    switch (selectorAsString) {
      case "min":
        return SortedNumericSelector.Type.MIN;
      case "max":
        return SortedNumericSelector.Type.MAX;
      default:
        throw new CorruptIndexException("unable to parse SortedNumericSelector type: " + selectorAsString, input);
    }
  }
  
  @Override
  public void write(Directory dir, SegmentInfo si, IOContext ioContext) throws IOException {

    String segFileName = IndexFileNames.segmentFileName(si.name, "", SimpleTextSegmentInfoFormat.SI_EXTENSION);

    try (IndexOutput output = dir.createOutput(segFileName, ioContext)) {
      // Only add the file once we've successfully created it, else IFD assert can trip:
      si.addFile(segFileName);
      BytesRefBuilder scratch = new BytesRefBuilder();
    
      SimpleTextUtil.write(output, SI_VERSION);
      SimpleTextUtil.write(output, si.getVersion().toString(), scratch);
      SimpleTextUtil.writeNewline(output);
    
      SimpleTextUtil.write(output, SI_DOCCOUNT);
      SimpleTextUtil.write(output, Integer.toString(si.maxDoc()), scratch);
      SimpleTextUtil.writeNewline(output);
    
      SimpleTextUtil.write(output, SI_USECOMPOUND);
      SimpleTextUtil.write(output, Boolean.toString(si.getUseCompoundFile()), scratch);
      SimpleTextUtil.writeNewline(output);
    
      Map<String,String> diagnostics = si.getDiagnostics();
      int numDiagnostics = diagnostics == null ? 0 : diagnostics.size();
      SimpleTextUtil.write(output, SI_NUM_DIAG);
      SimpleTextUtil.write(output, Integer.toString(numDiagnostics), scratch);
      SimpleTextUtil.writeNewline(output);
    
      if (numDiagnostics > 0) {
        for (Map.Entry<String,String> diagEntry : diagnostics.entrySet()) {
          SimpleTextUtil.write(output, SI_DIAG_KEY);
          SimpleTextUtil.write(output, diagEntry.getKey(), scratch);
          SimpleTextUtil.writeNewline(output);
        
          SimpleTextUtil.write(output, SI_DIAG_VALUE);
          SimpleTextUtil.write(output, diagEntry.getValue(), scratch);
          SimpleTextUtil.writeNewline(output);
        }
      }
      
      Map<String,String> attributes = si.getAttributes();
      SimpleTextUtil.write(output, SI_NUM_ATT);
      SimpleTextUtil.write(output, Integer.toString(attributes.size()), scratch);
      SimpleTextUtil.writeNewline(output);
    
      for (Map.Entry<String,String> attEntry : attributes.entrySet()) {
        SimpleTextUtil.write(output, SI_ATT_KEY);
        SimpleTextUtil.write(output, attEntry.getKey(), scratch);
        SimpleTextUtil.writeNewline(output);
        
        SimpleTextUtil.write(output, SI_ATT_VALUE);
        SimpleTextUtil.write(output, attEntry.getValue(), scratch);
        SimpleTextUtil.writeNewline(output);
      }
      
      Set<String> files = si.files();
      int numFiles = files == null ? 0 : files.size();
      SimpleTextUtil.write(output, SI_NUM_FILES);
      SimpleTextUtil.write(output, Integer.toString(numFiles), scratch);
      SimpleTextUtil.writeNewline(output);

      if (numFiles > 0) {
        for(String fileName : files) {
          SimpleTextUtil.write(output, SI_FILE);
          SimpleTextUtil.write(output, fileName, scratch);
          SimpleTextUtil.writeNewline(output);
        }
      }

      SimpleTextUtil.write(output, SI_ID);
      SimpleTextUtil.write(output, new BytesRef(si.getId()));
      SimpleTextUtil.writeNewline(output);
      
      Sort indexSort = si.getIndexSort();
      SimpleTextUtil.write(output, SI_SORT);
      final int numSortFields = indexSort == null ? 0 : indexSort.getSort().length;
      SimpleTextUtil.write(output, Integer.toString(numSortFields), scratch);
      SimpleTextUtil.writeNewline(output);
      for (int i = 0; i < numSortFields; ++i) {
        final SortField sortField = indexSort.getSort()[i];

        SimpleTextUtil.write(output, SI_SORT_FIELD);
        SimpleTextUtil.write(output, sortField.getField(), scratch);
        SimpleTextUtil.writeNewline(output);

        SimpleTextUtil.write(output, SI_SORT_TYPE);
        final String sortTypeString;
        final SortField.Type sortType;
        final boolean multiValued;
        if (sortField instanceof SortedSetSortField) {
          sortType = SortField.Type.STRING;
          multiValued = true;
        } else if (sortField instanceof SortedNumericSortField) {
          sortType = ((SortedNumericSortField) sortField).getNumericType();
          multiValued = true;
        } else {
          sortType = sortField.getType();
          multiValued = false;
        }
        switch (sortType) {
          case STRING:
              if (multiValued) {
                sortTypeString = "multi_valued_string";
              } else {
                sortTypeString = "string";
              }
            break;
          case LONG:
            if (multiValued) {
              sortTypeString = "multi_valued_long";
            } else {
              sortTypeString = "long";
            }
            break;
          case INT:
            if (multiValued) {
              sortTypeString = "multi_valued_int";
            } else {
              sortTypeString = "int";
            }
            break;
          case DOUBLE:
            if (multiValued) {
              sortTypeString = "multi_valued_double";
            } else {
              sortTypeString = "double";
            }
            break;
          case FLOAT:
            if (multiValued) {
              sortTypeString = "multi_valued_float";
            } else {
              sortTypeString = "float";
            }
            break;
          default:
            throw new IllegalStateException("Unexpected sort type: " + sortField.getType());
        }
        SimpleTextUtil.write(output, sortTypeString, scratch);
        SimpleTextUtil.writeNewline(output);

        if (sortField instanceof SortedSetSortField) {
          SortedSetSelector.Type selector = ((SortedSetSortField) sortField).getSelector();
          final String selectorString;
          if (selector == SortedSetSelector.Type.MIN) {
            selectorString = "min";
          } else if (selector == SortedSetSelector.Type.MIDDLE_MIN) {
            selectorString = "middle_min";
          } else if (selector == SortedSetSelector.Type.MIDDLE_MAX) {
            selectorString = "middle_max";
          } else if (selector == SortedSetSelector.Type.MAX) {
            selectorString = "max";
          } else {
            throw new IllegalStateException("Unexpected SortedSetSelector type selector: " + selector);
          }
          SimpleTextUtil.write(output, SI_SELECTOR_TYPE);
          SimpleTextUtil.write(output, selectorString, scratch);
          SimpleTextUtil.writeNewline(output);
        } else if (sortField instanceof SortedNumericSortField) {
          SortedNumericSelector.Type selector = ((SortedNumericSortField) sortField).getSelector();
          final String selectorString;
          if (selector == SortedNumericSelector.Type.MIN) {
            selectorString = "min";
          } else if (selector == SortedNumericSelector.Type.MAX) {
            selectorString = "max";
          } else {
            throw new IllegalStateException("Unexpected SortedNumericSelector type selector: " + selector);
          }
          SimpleTextUtil.write(output, SI_SELECTOR_TYPE);
          SimpleTextUtil.write(output, selectorString, scratch);
          SimpleTextUtil.writeNewline(output);
        }

        SimpleTextUtil.write(output, SI_SORT_REVERSE);
        SimpleTextUtil.write(output, Boolean.toString(sortField.getReverse()), scratch);
        SimpleTextUtil.writeNewline(output);

        SimpleTextUtil.write(output, SI_SORT_MISSING);
        final Object missingValue = sortField.getMissingValue();
        final String missing;
        if (missingValue == null) {
          missing = "null";
        } else if (missingValue == SortField.STRING_FIRST) {
          missing = "first";
        } else if (missingValue == SortField.STRING_LAST) {
          missing = "last";
        } else {
          missing = missingValue.toString();
        }
        SimpleTextUtil.write(output, missing, scratch);
        SimpleTextUtil.writeNewline(output);
      }
      
      SimpleTextUtil.writeChecksum(output, scratch);
    }
  }
}
