package org.apache.lucene.facet.search;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.facet.search.FacetsCollector.MatchingDocs;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

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

/** 
 * Represents {@link MatchingDocs} as {@link ScoredDocIDs}.
 * 
 * @lucene.experimental
 */
public class MatchingDocsAsScoredDocIDs implements ScoredDocIDs {

  // TODO remove this class once we get rid of ScoredDocIDs 

  final List<MatchingDocs> matchingDocs;
  final int size;
  
  public MatchingDocsAsScoredDocIDs(List<MatchingDocs> matchingDocs) {
    this.matchingDocs = matchingDocs;
    int totalSize = 0;
    for (MatchingDocs md : matchingDocs) {
      totalSize += md.totalHits;
    }
    this.size = totalSize;
  }
  
  @Override
  public ScoredDocIDsIterator iterator() throws IOException {
    return new ScoredDocIDsIterator() {
      
      final Iterator<MatchingDocs> mdIter = matchingDocs.iterator();
      
      int scoresIdx = 0;
      int doc = 0;
      MatchingDocs current;
      int currentLength;
      boolean done = false;
      
      @Override
      public boolean next() {
        if (done) {
          return false;
        }
        
        while (current == null) {
          if (!mdIter.hasNext()) {
            done = true;
            return false;
          }
          current = mdIter.next();
          currentLength = current.bits.length();
          doc = 0;
          scoresIdx = 0;
          
          if (doc >= currentLength || (doc = current.bits.nextSetBit(doc)) == -1) {
            current = null;
          } else {
            doc = -1; // we're calling nextSetBit later on
          }
        }
        
        ++doc;
        if (doc >= currentLength || (doc = current.bits.nextSetBit(doc)) == -1) {
          current = null;
          return next();
        }
        
        return true;
      }
      
      @Override
      public float getScore() {
        return current.scores == null ? ScoredDocIDsIterator.DEFAULT_SCORE : current.scores[scoresIdx++];
      }
      
      @Override
      public int getDocID() {
        return done ? DocIdSetIterator.NO_MORE_DOCS : doc + current.context.docBase;
      }
    };
  }

  @Override
  public DocIdSet getDocIDs() {
    return new DocIdSet() {
      
      final Iterator<MatchingDocs> mdIter = matchingDocs.iterator();
      int doc = 0;
      MatchingDocs current;
      int currentLength;
      boolean done = false;
      
      @Override
      public DocIdSetIterator iterator() throws IOException {
        return new DocIdSetIterator() {
          
          @Override
          public int nextDoc() throws IOException {
            if (done) {
              return DocIdSetIterator.NO_MORE_DOCS;
            }
            
            while (current == null) {
              if (!mdIter.hasNext()) {
                done = true;
                return DocIdSetIterator.NO_MORE_DOCS;
              }
              current = mdIter.next();
              currentLength = current.bits.length();
              doc = 0;
              
              if (doc >= currentLength || (doc = current.bits.nextSetBit(doc)) == -1) {
                current = null;
              } else {
                doc = -1; // we're calling nextSetBit later on
              }
            }
            
            ++doc;
            if (doc >= currentLength || (doc = current.bits.nextSetBit(doc)) == -1) {
              current = null;
              return nextDoc();
            }
            
            return doc + current.context.docBase;
          }
          
          @Override
          public int docID() {
            return doc + current.context.docBase;
          }
          
          @Override
          public long cost() {
            return size;
          }

          @Override
          public int advance(int target) throws IOException {
            throw new UnsupportedOperationException("not supported");
          }
        };
      }
    };
  }

  @Override
  public int size() {
    return size;
  }
  
}