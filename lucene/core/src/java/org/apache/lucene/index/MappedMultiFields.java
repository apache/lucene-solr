package org.apache.lucene.index;

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

import org.apache.lucene.util.Bits;

import static org.apache.lucene.index.FilterLeafReader.FilterFields;
import static org.apache.lucene.index.FilterLeafReader.FilterTerms;
import static org.apache.lucene.index.FilterLeafReader.FilterTermsEnum;

/** A {@link Fields} implementation that merges multiple
 *  Fields into one, and maps around deleted documents.
 *  This is used for merging. 
 *  @lucene.internal
 */
public class MappedMultiFields extends FilterFields {
  final MergeState mergeState;

  /** Create a new MappedMultiFields for merging, based on the supplied
   * mergestate and merged view of terms. */
  public MappedMultiFields(MergeState mergeState, MultiFields multiFields) {
    super(multiFields);
    this.mergeState = mergeState;
  }

  @Override
  public Terms terms(String field) throws IOException {
    MultiTerms terms = (MultiTerms) in.terms(field);
    if (terms == null) {
      return null;
    } else {
      return new MappedMultiTerms(mergeState, terms);
    }
  }

  private static class MappedMultiTerms extends FilterTerms {
    final MergeState mergeState;

    public MappedMultiTerms(MergeState mergeState, MultiTerms multiTerms) {
      super(multiTerms);
      this.mergeState = mergeState;
    }

    @Override
    public TermsEnum iterator(TermsEnum reuse) throws IOException {
      return new MappedMultiTermsEnum(mergeState, (MultiTermsEnum) in.iterator(reuse));
    }

    @Override
    public long size() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getSumTotalTermFreq() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getSumDocFreq() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getDocCount() throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  private static class MappedMultiTermsEnum extends FilterTermsEnum {
    final MergeState mergeState;

    public MappedMultiTermsEnum(MergeState mergeState, MultiTermsEnum multiTermsEnum) {
      super(multiTermsEnum);
      this.mergeState = mergeState;
    }

    @Override
    public int docFreq() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long totalTermFreq() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags) throws IOException {
      if (liveDocs != null) {
        throw new IllegalArgumentException("liveDocs must be null");
      }
      MappingMultiDocsEnum mappingDocsEnum;
      if (reuse instanceof MappingMultiDocsEnum) {
        mappingDocsEnum = (MappingMultiDocsEnum) reuse;
      } else {
        mappingDocsEnum = new MappingMultiDocsEnum(mergeState);
      }
      
      MultiDocsEnum docsEnum = (MultiDocsEnum) in.docs(liveDocs, mappingDocsEnum.multiDocsEnum, flags);
      mappingDocsEnum.reset(docsEnum);
      return mappingDocsEnum;
    }

    @Override
    public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags) throws IOException {
      if (liveDocs != null) {
        throw new IllegalArgumentException("liveDocs must be null");
      }
      MappingMultiDocsAndPositionsEnum mappingDocsAndPositionsEnum;
      if (reuse instanceof MappingMultiDocsAndPositionsEnum) {
        mappingDocsAndPositionsEnum = (MappingMultiDocsAndPositionsEnum) reuse;
      } else {
        mappingDocsAndPositionsEnum = new MappingMultiDocsAndPositionsEnum(mergeState);
      }
      
      MultiDocsAndPositionsEnum docsAndPositionsEnum = (MultiDocsAndPositionsEnum) in.docsAndPositions(liveDocs, mappingDocsAndPositionsEnum.multiDocsAndPositionsEnum, flags);
      mappingDocsAndPositionsEnum.reset(docsAndPositionsEnum);
      return mappingDocsAndPositionsEnum;
    }
  }
}
