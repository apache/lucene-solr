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

package org.apache.lucene.codecs.uniformsplit.sharedterms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.lucene50.DeltaBaseTermStateSerializer;
import org.apache.lucene.codecs.uniformsplit.BlockHeader;
import org.apache.lucene.codecs.uniformsplit.BlockLine;
import org.apache.lucene.codecs.uniformsplit.FieldMetadata;
import org.apache.lucene.codecs.uniformsplit.TermBytes;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

/**
 * Represents a term and its details stored in the {@link BlockTermState}.
 * It is an extension of {@link BlockLine} for the Shared Terms format. This
 * means the line contains a term and all its fields {@link org.apache.lucene.index.TermState}s.
 */
public class STBlockLine extends BlockLine {

  /**
   * Only used for writing.
   */
  protected final List<FieldMetadataTermState> termStates;

  public STBlockLine(TermBytes termBytes, List<FieldMetadataTermState> termStates) {
    super(termBytes, null);
    assert !termStates.isEmpty();
    this.termStates = new ArrayList<>(termStates);
  }

  public void collectFields(Collection<FieldMetadata> collector) {
    for (FieldMetadataTermState fieldTermState : termStates) {
      collector.add(fieldTermState.fieldMetadata);
    }
  }

  /**
   * Reads block lines encoded incrementally, with all fields corresponding
   * to the term of the line.
   * <p>
   * This class extends {@link BlockLine.Serializer}, so it keeps a state of the
   * previous term read to decode the next term.
   */
  public static class Serializer extends BlockLine.Serializer {

    public static void writeLineTermStates(DataOutput termStatesOutput, STBlockLine line,
                                    DeltaBaseTermStateSerializer encoder) throws IOException {

      FieldMetadataTermState fieldMetadataTermState;
      int size = line.termStates.size();
      assert size > 0 : "not valid block line with :" + size + " lines.";
      if (size == 1) {
        //write the field id in negative and the details
        int fieldID = line.termStates.get(0).fieldMetadata.getFieldInfo().number;
        termStatesOutput.writeZInt(-fieldID);
        fieldMetadataTermState = line.termStates.get(0);
        encoder.writeTermState(termStatesOutput, fieldMetadataTermState.fieldMetadata.getFieldInfo(), fieldMetadataTermState.state);
        return;
      }

      termStatesOutput.writeZInt(size);
      // first iteration write the fields ids
      for (int i = 0; i < size; i++) {
        fieldMetadataTermState = line.termStates.get(i);
        termStatesOutput.writeVInt(fieldMetadataTermState.fieldMetadata.getFieldInfo().number);
      }
      // second iteration write the corresponding metadata
      for (int i = 0; i < size; i++) {
        fieldMetadataTermState = line.termStates.get(i);
        encoder.writeTermState(termStatesOutput, fieldMetadataTermState.fieldMetadata.getFieldInfo(), fieldMetadataTermState.state);
      }
    }

    public static BlockTermState readTermStateForField(int fieldId, DataInput termStatesInput,
                                                DeltaBaseTermStateSerializer termStateSerializer,
                                                BlockHeader blockHeader, FieldInfos fieldInfos,
                                                BlockTermState reuse) throws IOException {
      assert fieldId >= 0;
      int numFields = termStatesInput.readZInt();
      if (numFields <= 0) {
        int readFieldId = -numFields;
        if (fieldId == readFieldId) {
          return termStateSerializer.readTermState(blockHeader.getBaseDocsFP(), blockHeader.getBasePositionsFP(),
              blockHeader.getBasePayloadsFP(), termStatesInput, fieldInfos.fieldInfo(readFieldId), reuse);
        }
        return null;
      }

      // There are multiple fields for the term.
      // We have to read all the field ids (aka field numbers) sequentially.
      // Then if the required field is in the list, we have to read all the TermState
      // sequentially. This could be optimized with a jump-to-middle offset
      // for example, but we don't need that currently.

      boolean isFieldInList = false;
      int[] readFieldIds = new int[numFields];
      for (int i = 0; i < numFields; i++) {
        int readFieldId = termStatesInput.readVInt();
        if (!isFieldInList && readFieldId > fieldId) {
          // As the list of fieldIds is sorted we can return early if we find fieldId greater than the seeked one.
          // But if we found the seeked one, we have to read all the list to get to the term state part afterward (there is no jump offset).
          return null;
        }
        isFieldInList |= readFieldId == fieldId;
        readFieldIds[i] = readFieldId;
      }
      if (isFieldInList) {
        for (int readFieldId : readFieldIds) {
          BlockTermState termState = termStateSerializer.readTermState(blockHeader.getBaseDocsFP(), blockHeader.getBasePositionsFP(),
              blockHeader.getBasePayloadsFP(), termStatesInput, fieldInfos.fieldInfo(readFieldId), reuse);
          if (fieldId == readFieldId) {
            return termState;
          }
        }
      }
      return null;
    }

    /**
     * @param fieldTermStatesMap Map filled with the term states for each field. It is cleared first.
     * @see #readTermStateForField
     */
    public static void readFieldTermStatesMap(DataInput termStatesInput,
                                       DeltaBaseTermStateSerializer termStateSerializer,
                                       BlockHeader blockHeader,
                                       FieldInfos fieldInfos,
                                       Map<String, BlockTermState> fieldTermStatesMap) throws IOException {
      fieldTermStatesMap.clear();
      int numFields = termStatesInput.readZInt();
      if (numFields <= 0) {
        int fieldId = -numFields;
        fieldTermStatesMap.put(fieldInfos.fieldInfo(fieldId).name, termStateSerializer.readTermState(blockHeader.getBaseDocsFP(), blockHeader.getBasePositionsFP(),
            blockHeader.getBasePayloadsFP(), termStatesInput, fieldInfos.fieldInfo(fieldId), null));
        return;
      }
      for (int fieldId : readFieldIds(termStatesInput, numFields)) {
        fieldTermStatesMap.put(fieldInfos.fieldInfo(fieldId).name, termStateSerializer.readTermState(blockHeader.getBaseDocsFP(), blockHeader.getBasePositionsFP(),
            blockHeader.getBasePayloadsFP(), termStatesInput, fieldInfos.fieldInfo(fieldId), null));
      }
    }

    public static int[] readFieldIds(DataInput termStatesInput, int numFields) throws IOException {
      int[] fieldIds = new int[numFields];
      for (int i = 0; i < numFields; i++) {
        fieldIds[i] = termStatesInput.readVInt();
      }
      return fieldIds;
    }
  }
}
