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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexSorter;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SortFieldProvider;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;

/**
 * plain text segments file format.
 *
 * <p><b>FOR RECREATIONAL USE ONLY</b>
 *
 * @lucene.experimental
 */
public class SimpleTextSegmentInfoFormat extends SegmentInfoFormat {
  static final BytesRef SI_VERSION = new BytesRef("    version ");
  static final BytesRef SI_MIN_VERSION = new BytesRef("    min version ");
  static final BytesRef SI_DOCCOUNT = new BytesRef("    number of documents ");
  static final BytesRef SI_USECOMPOUND = new BytesRef("    uses compound file ");
  static final BytesRef SI_NUM_DIAG = new BytesRef("    diagnostics ");
  static final BytesRef SI_DIAG_KEY = new BytesRef("      key ");
  static final BytesRef SI_DIAG_VALUE = new BytesRef("      value ");
  static final BytesRef SI_NUM_ATT = new BytesRef("    attributes ");
  static final BytesRef SI_ATT_KEY = new BytesRef("      key ");
  static final BytesRef SI_ATT_VALUE = new BytesRef("      value ");
  static final BytesRef SI_NUM_FILES = new BytesRef("    files ");
  static final BytesRef SI_FILE = new BytesRef("      file ");
  static final BytesRef SI_ID = new BytesRef("    id ");
  static final BytesRef SI_SORT = new BytesRef("    sort ");
  static final BytesRef SI_SORT_TYPE = new BytesRef("      type ");
  static final BytesRef SI_SORT_NAME = new BytesRef("      name ");
  static final BytesRef SI_SORT_BYTES = new BytesRef("      bytes ");

  public static final String SI_EXTENSION = "si";

  @Override
  public SegmentInfo read(
      Directory directory, String segmentName, byte[] segmentID, IOContext context)
      throws IOException {
    BytesRefBuilder scratch = new BytesRefBuilder();
    String segFileName =
        IndexFileNames.segmentFileName(segmentName, "", SimpleTextSegmentInfoFormat.SI_EXTENSION);
    try (ChecksumIndexInput input = directory.openChecksumInput(segFileName, context)) {
      SimpleTextUtil.readLine(input, scratch);
      assert StringHelper.startsWith(scratch.get(), SI_VERSION);
      final Version version;
      try {
        version = Version.parse(readString(SI_VERSION.length, scratch));
      } catch (ParseException pe) {
        throw new CorruptIndexException(
            "unable to parse version string: " + pe.getMessage(), input, pe);
      }

      SimpleTextUtil.readLine(input, scratch);
      assert StringHelper.startsWith(scratch.get(), SI_MIN_VERSION);
      Version minVersion;
      try {
        String versionString = readString(SI_MIN_VERSION.length, scratch);
        if (versionString.equals("null")) {
          minVersion = null;
        } else {
          minVersion = Version.parse(versionString);
        }
      } catch (ParseException pe) {
        throw new CorruptIndexException(
            "unable to parse version string: " + pe.getMessage(), input, pe);
      }

      SimpleTextUtil.readLine(input, scratch);
      assert StringHelper.startsWith(scratch.get(), SI_DOCCOUNT);
      final int docCount = Integer.parseInt(readString(SI_DOCCOUNT.length, scratch));

      SimpleTextUtil.readLine(input, scratch);
      assert StringHelper.startsWith(scratch.get(), SI_USECOMPOUND);
      final boolean isCompoundFile =
          Boolean.parseBoolean(readString(SI_USECOMPOUND.length, scratch));

      SimpleTextUtil.readLine(input, scratch);
      assert StringHelper.startsWith(scratch.get(), SI_NUM_DIAG);
      int numDiag = Integer.parseInt(readString(SI_NUM_DIAG.length, scratch));
      Map<String, String> diagnostics = new HashMap<>();

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
      Map<String, String> attributes = new HashMap<>(numAtt);

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
      final byte[] id = ArrayUtil.copyOfSubArray(scratch.bytes(), SI_ID.length, scratch.length());

      if (!Arrays.equals(segmentID, id)) {
        throw new CorruptIndexException(
            "file mismatch, expected: "
                + StringHelper.idToString(segmentID)
                + ", got: "
                + StringHelper.idToString(id),
            input);
      }

      SimpleTextUtil.readLine(input, scratch);
      assert StringHelper.startsWith(scratch.get(), SI_SORT);
      final int numSortFields = Integer.parseInt(readString(SI_SORT.length, scratch));
      SortField[] sortField = new SortField[numSortFields];
      for (int i = 0; i < numSortFields; ++i) {
        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch.get(), SI_SORT_NAME);
        final String provider = readString(SI_SORT_NAME.length, scratch);

        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch.get(), SI_SORT_TYPE);

        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch.get(), SI_SORT_BYTES);
        BytesRef serializedSort =
            SimpleTextUtil.fromBytesRefString(readString(SI_SORT_BYTES.length, scratch));
        final ByteArrayDataInput bytes =
            new ByteArrayDataInput(
                serializedSort.bytes, serializedSort.offset, serializedSort.length);
        sortField[i] = SortFieldProvider.forName(provider).readSortField(bytes);
        assert bytes.eof();
      }
      Sort indexSort = sortField.length == 0 ? null : new Sort(sortField);

      SimpleTextUtil.checkFooter(input);

      SegmentInfo info =
          new SegmentInfo(
              directory,
              version,
              minVersion,
              segmentName,
              docCount,
              isCompoundFile,
              null,
              diagnostics,
              id,
              attributes,
              indexSort);
      info.setFiles(files);
      return info;
    }
  }

  private String readString(int offset, BytesRefBuilder scratch) {
    return new String(scratch.bytes(), offset, scratch.length() - offset, StandardCharsets.UTF_8);
  }

  @Override
  public void write(Directory dir, SegmentInfo si, IOContext ioContext) throws IOException {

    String segFileName =
        IndexFileNames.segmentFileName(si.name, "", SimpleTextSegmentInfoFormat.SI_EXTENSION);

    try (IndexOutput output = dir.createOutput(segFileName, ioContext)) {
      // Only add the file once we've successfully created it, else IFD assert can trip:
      si.addFile(segFileName);
      BytesRefBuilder scratch = new BytesRefBuilder();

      SimpleTextUtil.write(output, SI_VERSION);
      SimpleTextUtil.write(output, si.getVersion().toString(), scratch);
      SimpleTextUtil.writeNewline(output);

      SimpleTextUtil.write(output, SI_MIN_VERSION);
      if (si.getMinVersion() == null) {
        SimpleTextUtil.write(output, "null", scratch);
      } else {
        SimpleTextUtil.write(output, si.getMinVersion().toString(), scratch);
      }
      SimpleTextUtil.writeNewline(output);

      SimpleTextUtil.write(output, SI_DOCCOUNT);
      SimpleTextUtil.write(output, Integer.toString(si.maxDoc()), scratch);
      SimpleTextUtil.writeNewline(output);

      SimpleTextUtil.write(output, SI_USECOMPOUND);
      SimpleTextUtil.write(output, Boolean.toString(si.getUseCompoundFile()), scratch);
      SimpleTextUtil.writeNewline(output);

      Map<String, String> diagnostics = si.getDiagnostics();
      int numDiagnostics = diagnostics == null ? 0 : diagnostics.size();
      SimpleTextUtil.write(output, SI_NUM_DIAG);
      SimpleTextUtil.write(output, Integer.toString(numDiagnostics), scratch);
      SimpleTextUtil.writeNewline(output);

      if (numDiagnostics > 0) {
        for (Map.Entry<String, String> diagEntry : diagnostics.entrySet()) {
          SimpleTextUtil.write(output, SI_DIAG_KEY);
          SimpleTextUtil.write(output, diagEntry.getKey(), scratch);
          SimpleTextUtil.writeNewline(output);

          SimpleTextUtil.write(output, SI_DIAG_VALUE);
          SimpleTextUtil.write(output, diagEntry.getValue(), scratch);
          SimpleTextUtil.writeNewline(output);
        }
      }

      Map<String, String> attributes = si.getAttributes();
      SimpleTextUtil.write(output, SI_NUM_ATT);
      SimpleTextUtil.write(output, Integer.toString(attributes.size()), scratch);
      SimpleTextUtil.writeNewline(output);

      for (Map.Entry<String, String> attEntry : attributes.entrySet()) {
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
        for (String fileName : files) {
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
        IndexSorter sorter = sortField.getIndexSorter();
        if (sorter == null) {
          throw new IllegalStateException("Cannot serialize sort " + sortField);
        }

        SimpleTextUtil.write(output, SI_SORT_NAME);
        SimpleTextUtil.write(output, sorter.getProviderName(), scratch);
        SimpleTextUtil.writeNewline(output);

        SimpleTextUtil.write(output, SI_SORT_TYPE);
        SimpleTextUtil.write(output, sortField.toString(), scratch);
        SimpleTextUtil.writeNewline(output);

        SimpleTextUtil.write(output, SI_SORT_BYTES);
        BytesRefOutput b = new BytesRefOutput();
        SortFieldProvider.write(sortField, b);
        SimpleTextUtil.write(output, b.bytes.get().toString(), scratch);
        SimpleTextUtil.writeNewline(output);
      }

      SimpleTextUtil.writeChecksum(output, scratch);
    }
  }

  static class BytesRefOutput extends DataOutput {

    final BytesRefBuilder bytes = new BytesRefBuilder();

    @Override
    public void writeByte(byte b) {
      bytes.append(b);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) {
      bytes.append(b, offset, length);
    }
  }
}
