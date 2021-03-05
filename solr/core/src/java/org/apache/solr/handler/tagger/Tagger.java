/*
 * This software was produced for the U. S. Government
 * under Contract No. W15P7T-11-C-F600, and is
 * subject to the Rights in Noncommercial Computer Software
 * and Noncommercial Computer Software Documentation
 * Clause 252.227-7014 (JUN 1995)
 *
 * Copyright 2013 The MITRE Corporation. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.handler.tagger;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tags maximum string of words in a corpus.  This is a callback-style API
 * in which you implement {@link #tagCallback(int, int, Object)}.
 *
 * This class should be independently usable outside Solr.
 */
public abstract class Tagger {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final TokenStream tokenStream;
  private final TermToBytesRefAttribute byteRefAtt;
  private final PositionIncrementAttribute posIncAtt;
  private final OffsetAttribute offsetAtt;
  private final TaggingAttribute taggingAtt;

  private final TagClusterReducer tagClusterReducer;
  private final Terms terms;
  private final Bits liveDocs;
  private final boolean skipAltTokens;
  private final boolean ignoreStopWords;

  private Map<BytesRef, IntsRef> docIdsCache;

  /** Whether the WARNING about skipped tokens was already logged. */
  private boolean loggedSkippedAltTokenWarning = false;

  public Tagger(Terms terms, Bits liveDocs, TokenStream tokenStream,
                TagClusterReducer tagClusterReducer, boolean skipAltTokens,
                boolean ignoreStopWords) throws IOException {
    this.terms = terms;
    this.liveDocs = liveDocs;
    this.tokenStream = tokenStream;
    this.skipAltTokens = skipAltTokens;
    this.ignoreStopWords = ignoreStopWords;
    byteRefAtt = tokenStream.addAttribute(TermToBytesRefAttribute.class);
    posIncAtt = tokenStream.addAttribute(PositionIncrementAttribute.class);
    offsetAtt = tokenStream.addAttribute(OffsetAttribute.class);
    taggingAtt = tokenStream.addAttribute(TaggingAttribute.class);
    tokenStream.reset();

    this.tagClusterReducer = tagClusterReducer;
  }

  public void enableDocIdsCache(int initSize) {
    if (initSize > 0)
      docIdsCache = new HashMap<>(initSize);
  }

  public void process() throws IOException {
    if (terms == null)
      return;

    //a shared pointer to the head used by this method and each Tag instance.
    final TagLL[] head = new TagLL[1];

    TermPrefixCursor cursor = null;//re-used

    //boolean switch used to log warnings in case tokens where skipped during tagging.
    boolean skippedTokens = false;

    while (tokenStream.incrementToken()) {
      if (log.isTraceEnabled()) {
        log.trace("Token: {}, posInc: {},  offset: [{},{}]",
                byteRefAtt, posIncAtt.getPositionIncrement(),
                offsetAtt.startOffset(), offsetAtt.endOffset());
      }
      //check for posInc < 1 (alternate Tokens, such as expanded Synonyms)
      if (posIncAtt.getPositionIncrement() < 1) {
        //(a) Deal with this as a configuration issue and throw an exception
        if (!skipAltTokens) {
          //TODO throw UnsupportedTokenException when PhraseBuilder is ported
          throw new IllegalStateException("Query Analyzer generates alternate "
              + "Tokens (posInc == 0). Please adapt your Analyzer configuration or "
              + "enable '" + TaggerRequestHandler.SKIP_ALT_TOKENS + "' to skip such "
              + "tokens. NOTE: enabling '" + TaggerRequestHandler.SKIP_ALT_TOKENS
              + "' might result in wrong tagging results if the index time analyzer "
              + "is not configured accordingly. For detailed information see "
              + "https://github.com/OpenSextant/SolrTextTagger/pull/11#issuecomment-24936225");
        } else {
          //(b) In case the index time analyser had indexed all variants (users
          //    need to ensure that) processing of alternate tokens can be skipped
          //    as anyways all alternatives will be contained in the FST.
          skippedTokens = true;
          log.trace("  ... ignored token");
          continue;
        }
      }
      //-- If PositionIncrement > 1 (stopwords)
      if (!ignoreStopWords && posIncAtt.getPositionIncrement() > 1) {
        log.trace("   - posInc > 1 ... mark cluster as done");
        advanceTagsAndProcessClusterIfDone(head, null);
      }

      final BytesRef term;
      //NOTE: we need to lookup tokens if
      // * the LookupAtt is true OR
      // * there are still advancing tags (to find the longest possible match)
      if(taggingAtt.isTaggable() || head[0] != null){
        //-- Lookup the term id from the next token
        term = byteRefAtt.getBytesRef();
        if (term.length == 0) {
          throw new IllegalArgumentException("term: " + term.utf8ToString() + " analyzed to a zero-length token");
        }
      } else { //no current cluster AND lookup == false ...
        term = null; //skip this token
      }

      //-- Process tag
      advanceTagsAndProcessClusterIfDone(head, term);

      //-- only create new Tags for Tokens we need to lookup
      if (taggingAtt.isTaggable() && term != null) {

        //determine if the terms index has a term starting with the provided term
        // TODO create a pool of these cursors to reuse them more?  could be trivial impl
        if (cursor == null)// (else the existing cursor will be re-used)
          cursor = new TermPrefixCursor(terms.iterator(), liveDocs, docIdsCache);
        if (cursor.advance(term)) {
          TagLL newTail = new TagLL(head, cursor, offsetAtt.startOffset(), offsetAtt.endOffset(), null);
          cursor = null;//because the new tag now "owns" this instance
          //and add it to the end
          if (head[0] == null) {
            head[0] = newTail;
          } else {
            for (TagLL t = head[0]; true; t = t.nextTag) {
              if (t.nextTag == null) {
                t.addAfterLL(newTail);
                break;
              }
            }
          }
        }
      }//if termId >= 0
    }//end while(incrementToken())

    //-- Finish all tags
    advanceTagsAndProcessClusterIfDone(head, null);
    assert head[0] == null;

    if(!loggedSkippedAltTokenWarning && skippedTokens){
      loggedSkippedAltTokenWarning = true; //only log once
      log.warn("{}{}{}{}"
          , "The Tagger skipped some alternate tokens (tokens with posInc == 0) "
          , "while processing text. This may cause problems with some Analyzer "
          , "configurations (e.g. query time synonym expansion). For details see "
          , "https://github.com/OpenSextant/SolrTextTagger/pull/11#issuecomment-24936225");
    }

    tokenStream.end();
    //tokenStream.close(); caller closes because caller acquired it
  }

  private void advanceTagsAndProcessClusterIfDone(TagLL[] head, BytesRef term) throws IOException {
    //-- Advance tags
    final int endOffset = term != null ? offsetAtt.endOffset() : -1;
    boolean anyAdvance = false;
    for (TagLL t = head[0]; t != null; t = t.nextTag) {
      anyAdvance |= t.advance(term, endOffset);
    }

    //-- Process cluster if done
    if (!anyAdvance && head[0] != null) {
      tagClusterReducer.reduce(head);
      for (TagLL t = head[0]; t != null; t = t.nextTag) {
        assert t.value != null;
        tagCallback(t.startOffset, t.endOffset, t.value);
      }
      head[0] = null;
    }
  }

  /**
   * Invoked by {@link #process()} for each tag found.  endOffset is always &gt;= the endOffset
   * given in the previous call.
   *
   * @param startOffset The character offset of the original stream where the tag starts.
   * @param endOffset One more than the character offset of the original stream where the tag ends.
   * @param docIdsKey A reference to the matching docIds that can be resolved via {@link #lookupDocIds(Object)}.
   */
  protected abstract void tagCallback(int startOffset, int endOffset, Object docIdsKey);

  /**
   * Returns a sorted array of integer docIds given the corresponding key.
   * @param docIdsKey The lookup key.
   * @return Not null
   */
  protected IntsRef lookupDocIds(Object docIdsKey) {
    return (IntsRef) docIdsKey;
  }
}

