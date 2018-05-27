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

package org.apache.lucene.luke.models.documents;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.luke.models.LukeModel;
import org.apache.lucene.luke.models.LukeException;
import org.apache.lucene.luke.util.BytesRefUtils;
import org.apache.lucene.luke.util.IndexUtils;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public final class DocumentsImpl extends LukeModel implements Documents {

  private static final Logger logger = LoggerFactory.getLogger(DocumentsImpl.class);

  private final TermVectorsAdapter tvAdapter;

  private final DocValuesAdapter dvAdapter;

  private String curField;

  private TermsEnum tenum;

  private PostingsEnum penum;

  /**
   * Constructs an DocumentsImpl that holds given {@link IndexReader}.
   * @param reader - the index reader
   */
  public DocumentsImpl(@Nonnull IndexReader reader) {
    super(reader);
    this.tvAdapter = new TermVectorsAdapter(reader);
    this.dvAdapter = new DocValuesAdapter(reader);
  }

  @Override
  public int getMaxDoc() {
    return reader.maxDoc();
  }

  @Override
  public boolean isLive(int docid) {
    return liveDocs == null || liveDocs.get(docid);
  }

  @Override
  public List<DocumentField> getDocumentFields(int docid) {
    if (!isLive(docid)) {
      logger.info("Doc #{} was deleted", docid);
      return Collections.emptyList();
    }

    List<DocumentField> res = new ArrayList<>();

    try {
      Document doc = reader.document(docid);

      for (FieldInfo finfo : IndexUtils.getFieldInfos(reader)) {
        // iterate all fields for this document
        IndexableField[] fields = doc.getFields(finfo.name);
        if (fields.length == 0) {
          // no stored data is available
          res.add(DocumentField.of(finfo, reader, docid));
        } else {
          for (IndexableField field : fields) {
            res.add(DocumentField.of(finfo, field, reader, docid));
          }
        }
      }

    } catch (IOException e) {
      throw new LukeException(String.format("Fields information not available for doc %d.", docid), e);
    }

    return res;
  }

  @Override
  public String getCurrentField() {
    return curField;
  }

  @Override
  public Optional<Term> firstTerm(@Nonnull String field) {
    try {
      Terms terms = IndexUtils.getTerms(reader, field);

      if (terms == null) {
        // no such field?
        resetCurrentField();
        resetTermsIterator();
        logger.warn("Terms not available for field: {}.", field);
        return Optional.empty();
      } else {
        setCurrentField(field);
        setTermsIterator(terms.iterator());

        if (tenum.next() == null) {
          // no term available for this field
          resetTermsIterator();
          logger.warn("No term available for field: {}.", field);
          return Optional.empty();
        } else {
          return Optional.of(new Term(curField, tenum.term()));
        }
      }

    } catch (IOException e) {
      resetTermsIterator();
      throw new LukeException(String.format("Terms not available for field: %s.", field), e);
    } finally {
      // discard current postings enum
      resetPostingsIterator();
    }
  }

  @Override
  public Optional<Term> nextTerm() {
    if (tenum == null) {
      // terms enum not initialized
      logger.warn("Terms enum un-positioned.");
      return Optional.empty();
    }

    try {
      if (tenum.next() == null) {
        // end of the iterator
        resetTermsIterator();
        logger.info("Reached the end of the term iterator for field: {}.", curField);
        return Optional.empty();

      } else {
        return Optional.of(new Term(curField, tenum.term()));
      }
    } catch (IOException e) {
      resetTermsIterator();
      throw new LukeException(String.format("Terms not available for field: %s.", curField), e);
    } finally {
      // discard current postings enum
      resetPostingsIterator();
    }
  }

  @Override
  public Optional<Term> seekTerm(@Nonnull String termText) {
    if (curField == null) {
      // field is not selected
      logger.warn("Field not selected.");
      return Optional.empty();
    }

    try {
      Terms terms = IndexUtils.getTerms(reader, curField);
      setTermsIterator(terms.iterator());

      if (tenum.seekCeil(new BytesRef(termText)) == TermsEnum.SeekStatus.END) {
        // reached to the end of the iterator
        resetTermsIterator();
        logger.info("Reached the end of the term iterator for field: {}.", curField);
        return Optional.empty();
      } else {
        return Optional.of(new Term(curField, tenum.term()));
      }
    } catch (IOException e) {
      resetTermsIterator();
      throw new LukeException(String.format("Terms not available for field: %s.", curField), e);
    } finally {
      // discard current postings enum
      resetPostingsIterator();
    }
  }

  @Override
  public Optional<Integer> firstTermDoc() {
    if (tenum == null) {
      // terms enum is not set
      logger.warn("Terms enum un-positioned.");
      return Optional.empty();
    }

    try {
      setPostingsIterator(tenum.postings(penum, PostingsEnum.ALL));

      if (penum.nextDoc() == PostingsEnum.NO_MORE_DOCS) {
        // no docs available for this term
        resetPostingsIterator();
        logger.warn("No docs available for term: {} in field: {}.", BytesRefUtils.decode(tenum.term()), curField);
        return Optional.empty();
      } else {
        return Optional.of(penum.docID());
      }
    } catch (IOException e) {
      resetPostingsIterator();
      throw new LukeException(String.format("Term docs not available for field: %s.", curField), e);
    }
  }

  @Override
  public Optional<Integer> nextTermDoc() {
    if (penum == null) {
      // postings enum is not initialized
      logger.warn("Postings enum un-positioned for field: {}.", curField);
      return Optional.empty();
    }

    try {
      if (penum.nextDoc() == PostingsEnum.NO_MORE_DOCS) {
        // end of the iterator
        resetPostingsIterator();
        logger.info("Reached the end of the postings iterator for term: {} in field: {}", BytesRefUtils.decode(tenum.term()), curField);
        return Optional.empty();
      } else {
        return Optional.of(penum.docID());
      }
    } catch (IOException e) {
      resetPostingsIterator();
      throw new LukeException(String.format("Term docs not available for field: %s.", curField), e);
    }
  }

  @Override
  public List<TermPosting> getTermPositions() {
    if (penum == null) {
      // postings enum is not initialized
      logger.warn("Postings enum un-positioned for field: {}.", curField);
      return Collections.emptyList();
    }

    List<TermPosting> res = new ArrayList<>();

    try {
      int freq = penum.freq();

      for (int i = 0; i < freq; i++) {
        int position = penum.nextPosition();
        if (position < 0) {
          // no position information available
          continue;
        }
        TermPosting posting = TermPosting.of(position, penum);
        res.add(posting);
      }

    } catch (IOException e) {
      throw new LukeException(String.format("Postings not available for field %s.", curField), e);
    }

    return res;
  }


  @Override
  public Optional<Integer> getDocFreq() {
    if (tenum == null) {
      // terms enum is not initialized
      logger.warn("Terms enum un-positioned for field: {}.", curField);
      return Optional.empty();
    }

    try {
      return Optional.of(tenum.docFreq());
    } catch (IOException e) {
      throw new LukeException(String.format("Doc frequency not available for field: %s.", curField), e);
    }
  }

  @Override
  public List<TermVectorEntry> getTermVectors(int docid, String field) {
    try {
      return tvAdapter.getTermVector(docid, field);
    } catch (IOException e) {
      throw new LukeException(String.format("Term vector not available for doc: #%d and field: %s", docid, field), e);
    }
  }

  @Override
  public Optional<DocValues> getDocValues(int docid, String field) {
    try {
      return dvAdapter.getDocValues(docid, field);
    } catch (IOException e) {
      throw new LukeException(String.format("Doc values not available for doc: #%d and field: %s", docid, field), e);
    }
  }

  private void resetCurrentField() {
    this.curField = null;
  }

  private void setCurrentField(String field) {
    this.curField = field;
  }

  private void resetTermsIterator() {
    this.tenum = null;
  }

  private void setTermsIterator(TermsEnum tenum) {
    this.tenum = tenum;
  }

  private void resetPostingsIterator() {
    this.penum = null;
  }

  private void setPostingsIterator(PostingsEnum penum) {
    this.penum = penum;
  }

}
