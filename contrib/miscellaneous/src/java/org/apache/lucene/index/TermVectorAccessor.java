package org.apache.lucene.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Collection;
import java.util.Iterator;
/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */


/**
 * Transparent access to the vector space model,
 * either via TermFreqVector or by resolving it from the inverted index.
 * <p/>
 * Resolving a term vector from a large index can be a time consuming process.
 * <p/>
 * Warning! This class is not thread safe!
 */
public class TermVectorAccessor {

  public TermVectorAccessor() {
  }

  /**
   * Instance reused to save garbage collector some time
   */
  private TermVectorMapperDecorator decoratedMapper = new TermVectorMapperDecorator();


  /**
   * Visits the TermVectorMapper and populates it with terms available for a given document,
   * either via a vector created at index time or by resolving them from the inverted index.
   *
   * @param indexReader    Index source
   * @param documentNumber Source document to access
   * @param fieldName      Field to resolve
   * @param mapper         Mapper to be mapped with data
   * @throws IOException
   */
  public void accept(IndexReader indexReader, int documentNumber, String fieldName, TermVectorMapper mapper) throws IOException {

    fieldName = fieldName.intern();

    decoratedMapper.decorated = mapper;
    decoratedMapper.termVectorStored = false;

    indexReader.getTermFreqVector(documentNumber, fieldName, decoratedMapper);

    if (!decoratedMapper.termVectorStored) {
      mapper.setDocumentNumber(documentNumber);
      build(indexReader, fieldName, mapper, documentNumber);
    }
  }

  /** Instance reused to save garbage collector some time */
  private List/*<String>*/ tokens;

  /** Instance reused to save garbage collector some time */
  private List/*<int[]>*/ positions;

  /** Instance reused to save garbage collector some time */
  private List/*<Integer>*/ frequencies;


  /**
   * Populates the mapper with terms available for the given field in a document
   * by resolving the inverted index.
   *
   * @param indexReader
   * @param field interned field name
   * @param mapper
   * @param documentNumber
   * @throws IOException
   */
  private void build(IndexReader indexReader, String field, TermVectorMapper mapper, int documentNumber) throws IOException {

    if (tokens == null) {
      tokens = new ArrayList/*<String>*/(500);
      positions = new ArrayList/*<int[]>*/(500);
      frequencies = new ArrayList/*<Integer>*/(500);
    } else {
      tokens.clear();
      frequencies.clear();
      positions.clear();
    }

    TermEnum termEnum = indexReader.terms();
    if (termEnum.skipTo(new Term(field, ""))) {

      while (termEnum.term().field() == field) {
        TermPositions termPositions = indexReader.termPositions(termEnum.term());
        if (termPositions.skipTo(documentNumber)) {

          frequencies.add(new Integer(termPositions.freq()));
          tokens.add(termEnum.term().text());


          if (!mapper.isIgnoringPositions()) {
            int[] positions = new int[termPositions.freq()];
            for (int i = 0; i < positions.length; i++) {
              positions[i] = termPositions.nextPosition();
            }
            this.positions.add(positions);
          } else {
            positions.add(null);
          }
        }
        termPositions.close();
        if (!termEnum.next()) {
          break;
        }
      }

      mapper.setDocumentNumber(documentNumber);
      mapper.setExpectations(field, tokens.size(), false, !mapper.isIgnoringPositions());
      for (int i = 0; i < tokens.size(); i++) {
        mapper.map((String) tokens.get(i), ((Integer) frequencies.get(i)).intValue(), (TermVectorOffsetInfo[]) null, (int[]) positions.get(i));
      }

    }
    termEnum.close();


  }


  private static class TermVectorMapperDecorator extends TermVectorMapper {

    private TermVectorMapper decorated;

    public boolean isIgnoringPositions() {
      return decorated.isIgnoringPositions();
    }

    public boolean isIgnoringOffsets() {
      return decorated.isIgnoringOffsets();
    }

    private boolean termVectorStored = false;

    public void setExpectations(String field, int numTerms, boolean storeOffsets, boolean storePositions) {
      decorated.setExpectations(field, numTerms, storeOffsets, storePositions);
      termVectorStored = true;
    }

    public void map(String term, int frequency, TermVectorOffsetInfo[] offsets, int[] positions) {
      decorated.map(term, frequency, offsets, positions);
    }

    public void setDocumentNumber(int documentNumber) {
      decorated.setDocumentNumber(documentNumber);
    }
  }

}
