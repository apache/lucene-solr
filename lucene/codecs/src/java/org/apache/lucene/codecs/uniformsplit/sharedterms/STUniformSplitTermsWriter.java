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

import static org.apache.lucene.codecs.uniformsplit.sharedterms.STUniformSplitPostingsFormat.NAME;
import static org.apache.lucene.codecs.uniformsplit.sharedterms.STUniformSplitPostingsFormat.TERMS_BLOCKS_EXTENSION;
import static org.apache.lucene.codecs.uniformsplit.sharedterms.STUniformSplitPostingsFormat.TERMS_DICTIONARY_EXTENSION;
import static org.apache.lucene.codecs.uniformsplit.sharedterms.STUniformSplitPostingsFormat.VERSION_CURRENT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.uniformsplit.BlockEncoder;
import org.apache.lucene.codecs.uniformsplit.FSTDictionary;
import org.apache.lucene.codecs.uniformsplit.FieldMetadata;
import org.apache.lucene.codecs.uniformsplit.IndexDictionary;
import org.apache.lucene.codecs.uniformsplit.UniformSplitTermsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;

/**
 * Extends {@link UniformSplitTermsWriter} by sharing all the fields terms in the same dictionary
 * and by writing all the fields of a term in the same block line.
 *
 * <p>The {@link STUniformSplitPostingsFormat#TERMS_BLOCKS_EXTENSION block file} contains all the
 * term blocks for all fields. Each block line, for a single term, may have multiple fields {@link
 * org.apache.lucene.index.TermState}. The block file also contains the fields metadata at the end
 * of the file.
 *
 * <p>The {@link STUniformSplitPostingsFormat#TERMS_DICTIONARY_EXTENSION dictionary file} contains a
 * single trie ({@link org.apache.lucene.util.fst.FST} bytes) for all fields.
 *
 * <p>This structure is adapted when there are lots of fields. In this case the shared-terms
 * dictionary trie is much smaller.
 *
 * <p>This {@link org.apache.lucene.codecs.FieldsConsumer} requires a custom {@link
 * #merge(MergeState, NormsProducer)} method for efficiency. The regular merge would scan all the
 * fields sequentially, which internally would scan the whole shared-terms dictionary as many times
 * as there are fields. Whereas the custom merge directly scans the internal shared-terms dictionary
 * of all segments to merge, thus scanning once whatever the number of fields is.
 *
 * @lucene.experimental
 */
public class STUniformSplitTermsWriter extends UniformSplitTermsWriter {

  public STUniformSplitTermsWriter(
      PostingsWriterBase postingsWriter, SegmentWriteState state, BlockEncoder blockEncoder)
      throws IOException {
    this(
        postingsWriter,
        state,
        DEFAULT_TARGET_NUM_BLOCK_LINES,
        DEFAULT_DELTA_NUM_LINES,
        blockEncoder);
  }

  public STUniformSplitTermsWriter(
      PostingsWriterBase postingsWriter,
      SegmentWriteState state,
      int targetNumBlockLines,
      int deltaNumLines,
      BlockEncoder blockEncoder)
      throws IOException {
    this(
        postingsWriter,
        state,
        targetNumBlockLines,
        deltaNumLines,
        blockEncoder,
        FieldMetadata.Serializer.INSTANCE,
        NAME,
        VERSION_CURRENT,
        TERMS_BLOCKS_EXTENSION,
        TERMS_DICTIONARY_EXTENSION);
  }

  protected STUniformSplitTermsWriter(
      PostingsWriterBase postingsWriter,
      SegmentWriteState state,
      int targetNumBlockLines,
      int deltaNumLines,
      BlockEncoder blockEncoder,
      FieldMetadata.Serializer fieldMetadataWriter,
      String codecName,
      int versionCurrent,
      String termsBlocksExtension,
      String dictionaryExtension)
      throws IOException {
    super(
        postingsWriter,
        state,
        targetNumBlockLines,
        deltaNumLines,
        blockEncoder,
        fieldMetadataWriter,
        codecName,
        versionCurrent,
        termsBlocksExtension,
        dictionaryExtension);
  }

  @Override
  public void write(Fields fields, NormsProducer normsProducer) throws IOException {
    writeSegment(
        (blockWriter, dictionaryBuilder) ->
            writeSingleSegment(fields, normsProducer, blockWriter, dictionaryBuilder));
  }

  /**
   * Writes the new segment with the provided {@link SharedTermsWriter}, which can be either a
   * single segment writer, or a multiple segment merging writer.
   */
  private void writeSegment(SharedTermsWriter termsWriter) throws IOException {
    STBlockWriter blockWriter =
        new STBlockWriter(blockOutput, targetNumBlockLines, deltaNumLines, blockEncoder);
    IndexDictionary.Builder dictionaryBuilder = new FSTDictionary.Builder();
    Collection<FieldMetadata> fieldMetadataList =
        termsWriter.writeSharedTerms(blockWriter, dictionaryBuilder);
    blockWriter.finishLastBlock(dictionaryBuilder);
    int fieldsNumber = writeFieldMetadataList(fieldMetadataList);
    writeDictionary(fieldsNumber, dictionaryBuilder);
  }

  private Collection<FieldMetadata> writeSingleSegment(
      Fields fields,
      NormsProducer normsProducer,
      STBlockWriter blockWriter,
      IndexDictionary.Builder dictionaryBuilder)
      throws IOException {
    List<FieldMetadata> fieldMetadataList =
        createFieldMetadataList(new FieldsIterator(fields, fieldInfos), maxDoc);
    TermIteratorQueue<FieldTerms> fieldTermsQueue =
        createFieldTermsQueue(fields, fieldMetadataList);
    List<TermIterator<FieldTerms>> groupedFieldTerms = new ArrayList<>(fieldTermsQueue.size());
    List<FieldMetadataTermState> termStates = new ArrayList<>(fieldTermsQueue.size());

    while (fieldTermsQueue.size() != 0) {
      TermIterator<FieldTerms> topFieldTerms = fieldTermsQueue.popTerms();
      BytesRef term = BytesRef.deepCopyOf(topFieldTerms.term);
      groupByTerm(fieldTermsQueue, topFieldTerms, groupedFieldTerms);
      writePostingLines(term, groupedFieldTerms, normsProducer, termStates);
      blockWriter.addLine(term, termStates, dictionaryBuilder);
      nextTermForIterators(groupedFieldTerms, fieldTermsQueue);
    }
    return fieldMetadataList;
  }

  private List<FieldMetadata> createFieldMetadataList(Iterator<FieldInfo> fieldInfos, int maxDoc) {
    List<FieldMetadata> fieldMetadataList = new ArrayList<>();
    while (fieldInfos.hasNext()) {
      FieldMetadata fieldMetadata = new FieldMetadata(fieldInfos.next(), maxDoc);
      fieldMetadata.setDictionaryStartFP(dictionaryOutput.getFilePointer());
      fieldMetadataList.add(fieldMetadata);
    }
    return fieldMetadataList;
  }

  private TermIteratorQueue<FieldTerms> createFieldTermsQueue(
      Fields fields, List<FieldMetadata> fieldMetadataList) throws IOException {
    TermIteratorQueue<FieldTerms> fieldQueue = new TermIteratorQueue<>(fieldMetadataList.size());
    for (FieldMetadata fieldMetadata : fieldMetadataList) {
      Terms terms = fields.terms(fieldMetadata.getFieldInfo().name);
      if (terms != null) {
        FieldTerms fieldTerms = new FieldTerms(fieldMetadata, terms.iterator());
        if (fieldTerms.nextTerm()) {
          // There is at least one term for the field.
          fieldQueue.add(fieldTerms);
        }
      }
    }
    return fieldQueue;
  }

  private <T> void groupByTerm(
      TermIteratorQueue<T> termIteratorQueue,
      TermIterator<T> topTermIterator,
      List<TermIterator<T>> groupedTermIterators) {
    groupedTermIterators.clear();
    groupedTermIterators.add(topTermIterator);
    while (termIteratorQueue.size() != 0) {
      TermIterator<T> termIterator = termIteratorQueue.top();
      if (topTermIterator.term.compareTo(termIterator.term) != 0) {
        return;
      }
      // Same term for another iterator. Combine the iterators.
      groupedTermIterators.add(termIterator);
      termIteratorQueue.pop();
    }
  }

  private void writePostingLines(
      BytesRef term,
      List<? extends TermIterator<FieldTerms>> groupedFieldTerms,
      NormsProducer normsProducer,
      List<FieldMetadataTermState> termStates)
      throws IOException {
    termStates.clear();
    for (TermIterator<FieldTerms> fieldTermIterator : groupedFieldTerms) {
      FieldTerms fieldTerms = (FieldTerms) fieldTermIterator;
      postingsWriter.setField(fieldTerms.fieldMetadata.getFieldInfo());
      BlockTermState blockTermState =
          writePostingLine(fieldTerms.termsEnum, fieldTerms.fieldMetadata, normsProducer);
      if (blockTermState != null) {
        fieldTerms.fieldMetadata.setLastTerm(term);
        termStates.add(new FieldMetadataTermState(fieldTerms.fieldMetadata, blockTermState));
      }
    }
  }

  private <T> void nextTermForIterators(
      List<? extends TermIterator<T>> termIterators, TermIteratorQueue<T> termIteratorQueue)
      throws IOException {
    for (TermIterator<T> termIterator : termIterators) {
      if (termIterator.nextTerm()) {
        // There is a next term for the iterator. Add it to the priority queue.
        termIteratorQueue.add(termIterator);
      }
    }
  }

  private int writeFieldMetadataList(Collection<FieldMetadata> fieldMetadataList)
      throws IOException {
    ByteBuffersDataOutput fieldsOutput = new ByteBuffersDataOutput();
    int fieldsNumber = 0;
    for (FieldMetadata fieldMetadata : fieldMetadataList) {
      if (fieldMetadata.getNumTerms() > 0) {
        fieldMetadataWriter.write(fieldsOutput, fieldMetadata);
        fieldsNumber++;
      }
    }
    writeFieldsMetadata(fieldsNumber, fieldsOutput);
    return fieldsNumber;
  }

  protected void writeDictionary(int fieldsNumber, IndexDictionary.Builder dictionaryBuilder)
      throws IOException {
    if (fieldsNumber > 0) {
      writeDictionary(dictionaryBuilder);
    }
    CodecUtil.writeFooter(dictionaryOutput);
  }

  @Override
  public void merge(MergeState mergeState, NormsProducer normsProducer) throws IOException {
    if (mergeState.needsIndexSort) {
      // This custom merging does not support sorted index.
      // Fall back to the default merge, which is inefficient for this postings format.
      super.merge(mergeState, normsProducer);
      return;
    }
    FieldsProducer[] fieldsProducers = mergeState.fieldsProducers;
    List<TermIterator<SegmentTerms>> segmentTermsList = new ArrayList<>(fieldsProducers.length);
    for (int segmentIndex = 0; segmentIndex < fieldsProducers.length; segmentIndex++) {
      FieldsProducer fieldsProducer = fieldsProducers[segmentIndex];
      // Iterate the FieldInfo provided by mergeState.fieldInfos because they may be
      // filtered by PerFieldMergeState.
      for (FieldInfo fieldInfo : mergeState.fieldInfos[segmentIndex]) {
        // Iterate all fields only the get the *first* Terms instanceof STUniformSplitTerms.
        // See the break below.
        Terms terms = fieldsProducer.terms(fieldInfo.name);
        if (terms != null) {
          if (!(terms instanceof STUniformSplitTerms)) {
            // Terms is not directly an instance of STUniformSplitTerms, it is wrapped/filtered.
            // Fall back to the default merge, which is inefficient for this postings format.
            super.merge(mergeState, normsProducer);
            return;
          }
          STUniformSplitTerms sharedTerms = (STUniformSplitTerms) terms;
          segmentTermsList.add(
              new SegmentTerms(
                  segmentIndex,
                  sharedTerms.createMergingBlockReader(),
                  mergeState.docMaps[segmentIndex]));
          // We have the STUniformSplitTerms for the segment. Break the field
          // loop to iterate the next segment.
          break;
        }
      }
    }
    writeSegment(
        (blockWriter, dictionaryBuilder) ->
            mergeSegments(
                mergeState, normsProducer, segmentTermsList, blockWriter, dictionaryBuilder));
  }

  private Collection<FieldMetadata> mergeSegments(
      MergeState mergeState,
      NormsProducer normsProducer,
      List<TermIterator<SegmentTerms>> segmentTermsList,
      STBlockWriter blockWriter,
      IndexDictionary.Builder dictionaryBuilder)
      throws IOException {
    List<FieldMetadata> fieldMetadataList =
        createFieldMetadataList(
            mergeState.mergeFieldInfos.iterator(), mergeState.segmentInfo.maxDoc());
    Map<String, MergingFieldTerms> fieldTermsMap =
        createMergingFieldTermsMap(fieldMetadataList, mergeState.fieldsProducers.length);
    TermIteratorQueue<SegmentTerms> segmentTermsQueue = createSegmentTermsQueue(segmentTermsList);
    List<TermIterator<SegmentTerms>> groupedSegmentTerms = new ArrayList<>(segmentTermsList.size());
    Map<String, List<SegmentPostings>> fieldPostingsMap =
        new HashMap<>(mergeState.fieldInfos.length);
    List<MergingFieldTerms> groupedFieldTerms = new ArrayList<>(mergeState.fieldInfos.length);
    List<FieldMetadataTermState> termStates = new ArrayList<>(mergeState.fieldInfos.length);

    while (segmentTermsQueue.size() != 0) {
      TermIterator<SegmentTerms> topSegmentTerms = segmentTermsQueue.popTerms();
      BytesRef term = BytesRef.deepCopyOf(topSegmentTerms.term);
      groupByTerm(segmentTermsQueue, topSegmentTerms, groupedSegmentTerms);
      combineSegmentsFields(groupedSegmentTerms, fieldPostingsMap);
      combinePostingsPerField(term, fieldTermsMap, fieldPostingsMap, groupedFieldTerms);
      writePostingLines(term, groupedFieldTerms, normsProducer, termStates);
      blockWriter.addLine(term, termStates, dictionaryBuilder);
      nextTermForIterators(groupedSegmentTerms, segmentTermsQueue);
    }
    return fieldMetadataList;
  }

  private Map<String, MergingFieldTerms> createMergingFieldTermsMap(
      List<FieldMetadata> fieldMetadataList, int numSegments) {
    Map<String, MergingFieldTerms> fieldTermsMap = new HashMap<>(fieldMetadataList.size() * 2);
    for (FieldMetadata fieldMetadata : fieldMetadataList) {
      FieldInfo fieldInfo = fieldMetadata.getFieldInfo();
      fieldTermsMap.put(
          fieldInfo.name,
          new MergingFieldTerms(
              fieldMetadata, new STMergingTermsEnum(fieldInfo.name, numSegments)));
    }
    return fieldTermsMap;
  }

  private TermIteratorQueue<SegmentTerms> createSegmentTermsQueue(
      List<TermIterator<SegmentTerms>> segmentTermsList) throws IOException {
    TermIteratorQueue<SegmentTerms> segmentQueue = new TermIteratorQueue<>(segmentTermsList.size());
    for (TermIterator<SegmentTerms> segmentTerms : segmentTermsList) {
      if (segmentTerms.nextTerm()) {
        // There is at least one term in the segment
        segmentQueue.add(segmentTerms);
      }
    }
    return segmentQueue;
  }

  private void combineSegmentsFields(
      List<TermIterator<SegmentTerms>> groupedSegmentTerms,
      Map<String, List<SegmentPostings>> fieldPostingsMap) {
    fieldPostingsMap.clear();
    for (TermIterator<SegmentTerms> segmentTermIterator : groupedSegmentTerms) {
      SegmentTerms segmentTerms = (SegmentTerms) segmentTermIterator;
      for (Map.Entry<String, BlockTermState> fieldTermState :
          segmentTerms.fieldTermStatesMap.entrySet()) {
        List<SegmentPostings> segmentPostingsList = fieldPostingsMap.get(fieldTermState.getKey());
        if (segmentPostingsList == null) {
          segmentPostingsList = new ArrayList<>(groupedSegmentTerms.size());
          fieldPostingsMap.put(fieldTermState.getKey(), segmentPostingsList);
        }
        segmentPostingsList.add(
            new SegmentPostings(
                segmentTerms.segmentIndex,
                fieldTermState.getValue(),
                segmentTerms.mergingBlockReader,
                segmentTerms.docMap));
      }
    }
  }

  private void combinePostingsPerField(
      BytesRef term,
      Map<String, MergingFieldTerms> fieldTermsMap,
      Map<String, List<SegmentPostings>> fieldPostingsMap,
      List<MergingFieldTerms> groupedFieldTerms) {
    groupedFieldTerms.clear();
    for (Map.Entry<String, List<SegmentPostings>> fieldPostingsEntry :
        fieldPostingsMap.entrySet()) {
      // The field defined in fieldPostingsMap comes from the FieldInfos of the SegmentReadState.
      // The fieldTermsMap contains entries for fields coming from the SegmentMergeSate.
      // So it is possible that the field is not present in fieldTermsMap because it is removed.
      MergingFieldTerms fieldTerms = fieldTermsMap.get(fieldPostingsEntry.getKey());
      if (fieldTerms != null) {
        fieldTerms.resetIterator(term, fieldPostingsEntry.getValue());
        groupedFieldTerms.add(fieldTerms);
      }
    }
    // Keep the fields ordered by their number in the target merge segment.
    groupedFieldTerms.sort(
        Comparator.comparingInt(fieldTerms -> fieldTerms.fieldMetadata.getFieldInfo().number));
  }

  private interface SharedTermsWriter {
    Collection<FieldMetadata> writeSharedTerms(
        STBlockWriter blockWriter, IndexDictionary.Builder dictionaryBuilder) throws IOException;
  }

  final class SegmentPostings {

    final int segmentIndex;
    final BlockTermState termState;
    final STMergingBlockReader mergingBlockReader;
    final MergeState.DocMap docMap;

    SegmentPostings(
        int segmentIndex,
        BlockTermState termState,
        STMergingBlockReader mergingBlockReader,
        MergeState.DocMap docMap) {
      this.segmentIndex = segmentIndex;
      this.termState = termState;
      this.mergingBlockReader = mergingBlockReader;
      this.docMap = docMap;
    }

    PostingsEnum getPostings(String fieldName, PostingsEnum reuse, int flags) throws IOException {
      return mergingBlockReader.postings(fieldName, termState, reuse, flags);
    }
  }

  private class TermIteratorQueue<T> extends PriorityQueue<TermIterator<T>> {

    TermIteratorQueue(int numFields) {
      super(numFields);
    }

    @Override
    protected boolean lessThan(TermIterator<T> a, TermIterator<T> b) {
      return a.compareTo(b) < 0;
    }

    TermIterator<T> popTerms() {
      TermIterator<T> topTerms = pop();
      assert topTerms != null;
      assert topTerms.term != null;
      return topTerms;
    }
  }

  private abstract class TermIterator<T> implements Comparable<TermIterator<T>> {

    BytesRef term;

    abstract boolean nextTerm() throws IOException;

    @Override
    public int compareTo(TermIterator<T> other) {
      assert term != null : "Should not be compared when the iterator is exhausted";
      int comparison = term.compareTo(other.term);
      if (comparison == 0) {
        return compareSecondary(other);
      }
      return comparison;
    }

    abstract int compareSecondary(TermIterator<T> other);
  }

  private class FieldTerms extends TermIterator<FieldTerms> {

    final FieldMetadata fieldMetadata;
    final TermsEnum termsEnum;

    FieldTerms(FieldMetadata fieldMetadata, TermsEnum termsEnum) {
      this.fieldMetadata = fieldMetadata;
      this.termsEnum = termsEnum;
    }

    @Override
    boolean nextTerm() throws IOException {
      term = termsEnum.next();
      return term != null;
    }

    @Override
    int compareSecondary(TermIterator<FieldTerms> other) {
      return Integer.compare(
          fieldMetadata.getFieldInfo().number,
          ((FieldTerms) other).fieldMetadata.getFieldInfo().number);
    }
  }

  private class MergingFieldTerms extends FieldTerms {

    MergingFieldTerms(FieldMetadata fieldMetadata, STMergingTermsEnum termsEnum) {
      super(fieldMetadata, termsEnum);
    }

    void resetIterator(BytesRef term, List<SegmentPostings> segmentPostingsList) {
      ((STMergingTermsEnum) termsEnum).reset(term, segmentPostingsList);
    }
  }

  private class SegmentTerms extends TermIterator<SegmentTerms> {

    private final Integer segmentIndex;
    private final STMergingBlockReader mergingBlockReader;
    private final Map<String, BlockTermState> fieldTermStatesMap;
    private final MergeState.DocMap docMap;

    SegmentTerms(
        int segmentIndex, STMergingBlockReader mergingBlockReader, MergeState.DocMap docMap) {
      this.segmentIndex = segmentIndex;
      this.mergingBlockReader = mergingBlockReader;
      this.docMap = docMap;
      this.fieldTermStatesMap = new HashMap<>();
    }

    @Override
    boolean nextTerm() throws IOException {
      term = mergingBlockReader.next();
      if (term == null) {
        return false;
      }
      mergingBlockReader.readFieldTermStatesMap(fieldTermStatesMap);
      return true;
    }

    @Override
    int compareSecondary(TermIterator<SegmentTerms> other) {
      return Integer.compare(segmentIndex, ((SegmentTerms) other).segmentIndex);
    }
  }

  private static class FieldsIterator implements Iterator<FieldInfo> {

    private final Iterator<String> fieldNames;
    private final FieldInfos fieldInfos;

    FieldsIterator(Fields fields, FieldInfos fieldInfos) {
      this.fieldNames = fields.iterator();
      this.fieldInfos = fieldInfos;
    }

    @Override
    public boolean hasNext() {
      return fieldNames.hasNext();
    }

    @Override
    public FieldInfo next() {
      return fieldInfos.fieldInfo(fieldNames.next());
    }
  }
}
