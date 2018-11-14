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
package org.apache.lucene.index;


import java.io.IOException;

import org.apache.lucene.index.FilterLeafReader.FilterTerms;
import org.apache.lucene.index.FilterLeafReader.FilterTermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;


/**
 * The {@link ExitableDirectoryReader} wraps a real index {@link DirectoryReader} and
 * allows for a {@link QueryTimeout} implementation object to be checked periodically
 * to see if the thread should exit or not.  If {@link QueryTimeout#shouldExit()}
 * returns true, an {@link ExitingReaderException} is thrown.
 */
public class ExitableDirectoryReader extends FilterDirectoryReader {
  
  private QueryTimeout queryTimeout;

  /**
   * Exception that is thrown to prematurely terminate a term enumeration.
   */
  @SuppressWarnings("serial")
  public static class ExitingReaderException extends RuntimeException {

    /** Constructor **/
    ExitingReaderException(String msg) {
      super(msg);
    }
  }

  /**
   * Wrapper class for a SubReaderWrapper that is used by the ExitableDirectoryReader.
   */
  public static class ExitableSubReaderWrapper extends SubReaderWrapper {
    private QueryTimeout queryTimeout;

    /** Constructor **/
    public ExitableSubReaderWrapper(QueryTimeout queryTimeout) {
      this.queryTimeout = queryTimeout;
    }

    @Override
    public LeafReader wrap(LeafReader reader) {
      return new ExitableFilterAtomicReader(reader, queryTimeout);
    }
  }

  /**
   * Wrapper class for another FilterAtomicReader. This is used by ExitableSubReaderWrapper.
   */
  public static class ExitableFilterAtomicReader extends FilterLeafReader {

    private QueryTimeout queryTimeout;
    
    /** Constructor **/
    public ExitableFilterAtomicReader(LeafReader in, QueryTimeout queryTimeout) {
      super(in);
      this.queryTimeout = queryTimeout;
    }

    @Override
    public PointValues getPointValues(String field) throws IOException {
      final PointValues pointValues = in.getPointValues(field);
      if (pointValues == null) {
        return null;
      }
      return (queryTimeout.isTimeoutEnabled()) ? new ExitablePointValues(pointValues, queryTimeout) : pointValues;
    }

    @Override
    public Terms terms(String field) throws IOException {
      Terms terms = in.terms(field);
      if (terms == null) {
        return null;
      }
      return (queryTimeout.isTimeoutEnabled()) ? new ExitableTerms(terms, queryTimeout) : terms;
    }

    // this impl does not change deletes or data so we can delegate the
    // CacheHelpers
    @Override
    public CacheHelper getReaderCacheHelper() {
      return in.getReaderCacheHelper();
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
      return in.getCoreCacheHelper();
    }

  }

  /**
   * Wrapper class for another PointValues implementation that is used by ExitableFields.
   */
  private static class ExitablePointValues extends PointValues {

    private final PointValues in;
    private final QueryTimeout queryTimeout;

    private ExitablePointValues(PointValues in, QueryTimeout queryTimeout) {
      this.in = in;
      this.queryTimeout = queryTimeout;
      checkAndThrow();
    }

    /**
     * Throws {@link ExitingReaderException} if {@link QueryTimeout#shouldExit()} returns true,
     * or if {@link Thread#interrupted()} returns true.
     */
    private void checkAndThrow() {
      if (queryTimeout.shouldExit()) {
        throw new ExitingReaderException("The request took too long to iterate over point values. Timeout: "
            + queryTimeout.toString()
            + ", PointValues=" + in
        );
      } else if (Thread.interrupted()) {
        throw new ExitingReaderException("Interrupted while iterating over point values. PointValues=" + in);
      }
    }

    @Override
    public void intersect(IntersectVisitor visitor) throws IOException {
      checkAndThrow();
      in.intersect(new ExitableIntersectVisitor(visitor, queryTimeout));
    }

    @Override
    public long estimatePointCount(IntersectVisitor visitor) {
      checkAndThrow();
      return in.estimatePointCount(visitor);
    }

    @Override
    public byte[] getMinPackedValue() throws IOException {
      checkAndThrow();
      return in.getMinPackedValue();
    }

    @Override
    public byte[] getMaxPackedValue() throws IOException {
      checkAndThrow();
      return in.getMaxPackedValue();
    }

    @Override
    public int getNumDataDimensions() throws IOException {
      checkAndThrow();
      return in.getNumDataDimensions();
    }

    @Override
    public int getNumIndexDimensions() throws IOException {
      checkAndThrow();
      return in.getNumIndexDimensions();
    }

    @Override
    public int getBytesPerDimension() throws IOException {
      checkAndThrow();
      return in.getBytesPerDimension();
    }

    @Override
    public long size() {
      checkAndThrow();
      return in.size();
    }

    @Override
    public int getDocCount() {
      checkAndThrow();
      return in.getDocCount();
    }
  }

  private static class ExitableIntersectVisitor implements PointValues.IntersectVisitor {

    private static final int MAX_CALLS_BEFORE_QUERY_TIMEOUT_CHECK = 10;

    private final PointValues.IntersectVisitor in;
    private final QueryTimeout queryTimeout;
    private int calls;

    private ExitableIntersectVisitor(PointValues.IntersectVisitor in, QueryTimeout queryTimeout) {
      this.in = in;
      this.queryTimeout = queryTimeout;
    }

    /**
     * Throws {@link ExitingReaderException} if {@link QueryTimeout#shouldExit()} returns true,
     * or if {@link Thread#interrupted()} returns true.
     */
    private void checkAndThrowWithSampling() {
      if (calls++ % MAX_CALLS_BEFORE_QUERY_TIMEOUT_CHECK == 0) {
        checkAndThrow();
      }
    }

    private void checkAndThrow() {
      if (queryTimeout.shouldExit()) {
        throw new ExitingReaderException("The request took too long to intersect point values. Timeout: "
            + queryTimeout.toString()
            + ", PointValues=" + in
        );
      } else if (Thread.interrupted()) {
        throw new ExitingReaderException("Interrupted while intersecting point values. PointValues=" + in);
      }
    }

    @Override
    public void visit(int docID) throws IOException {
      checkAndThrowWithSampling();
      in.visit(docID);
    }

    @Override
    public void visit(int docID, byte[] packedValue) throws IOException {
      checkAndThrowWithSampling();
      in.visit(docID, packedValue);
    }

    @Override
    public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
      checkAndThrow();
      return in.compare(minPackedValue, maxPackedValue);
    }

    @Override
    public void grow(int count) {
      checkAndThrow();
      in.grow(count);
    }
  }

  /**
   * Wrapper class for another Terms implementation that is used by ExitableFields.
   */
  public static class ExitableTerms extends FilterTerms {

    private QueryTimeout queryTimeout;

    /** Constructor **/
    public ExitableTerms(Terms terms, QueryTimeout queryTimeout) {
      super(terms);
      this.queryTimeout = queryTimeout;
    }

    @Override
    public TermsEnum intersect(CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
      return new ExitableTermsEnum(in.intersect(compiled, startTerm), queryTimeout);
    }

    @Override
    public TermsEnum iterator() throws IOException {
      return new ExitableTermsEnum(in.iterator(), queryTimeout);
    }
  }

  /**
   * Wrapper class for TermsEnum that is used by ExitableTerms for implementing an
   * exitable enumeration of terms.
   */
  public static class ExitableTermsEnum extends FilterTermsEnum {

    private QueryTimeout queryTimeout;
    
    /** Constructor **/
    public ExitableTermsEnum(TermsEnum termsEnum, QueryTimeout queryTimeout) {
      super(termsEnum);
      this.queryTimeout = queryTimeout;
      checkAndThrow();
    }

    /**
     * Throws {@link ExitingReaderException} if {@link QueryTimeout#shouldExit()} returns true,
     * or if {@link Thread#interrupted()} returns true.
     */
    private void checkAndThrow() {
      if (queryTimeout.shouldExit()) {
        throw new ExitingReaderException("The request took too long to iterate over terms. Timeout: " 
            + queryTimeout.toString()
            + ", TermsEnum=" + in
        );
      } else if (Thread.interrupted()) {
        throw new ExitingReaderException("Interrupted while iterating over terms. TermsEnum=" + in);
      }
    }

    @Override
    public BytesRef next() throws IOException {
      // Before every iteration, check if the iteration should exit
      checkAndThrow();
      return in.next();
    }
  }

  /**
   * Constructor
   * @param in DirectoryReader that this ExitableDirectoryReader wraps around to make it Exitable.
   * @param queryTimeout The object to periodically check if the query should time out.
   */
  public ExitableDirectoryReader(DirectoryReader in, QueryTimeout queryTimeout) throws IOException {
    super(in, new ExitableSubReaderWrapper(queryTimeout));
    this.queryTimeout = queryTimeout;
  }

  @Override
  protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
    return new ExitableDirectoryReader(in, queryTimeout);
  }

  /**
   * Wraps a provided DirectoryReader. Note that for convenience, the returned reader
   * can be used normally (e.g. passed to {@link DirectoryReader#openIfChanged(DirectoryReader)})
   * and so on.
   */
  public static DirectoryReader wrap(DirectoryReader in, QueryTimeout queryTimeout) throws IOException {
    return new ExitableDirectoryReader(in, queryTimeout);
  }

  @Override
  public CacheHelper getReaderCacheHelper() {
    return in.getReaderCacheHelper();
  }

  @Override
  public String toString() {
    return "ExitableDirectoryReader(" + in.toString() + ")";
  }
}


