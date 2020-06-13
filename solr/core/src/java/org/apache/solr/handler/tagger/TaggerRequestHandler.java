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

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import com.google.common.io.CharStreams;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.StopFilterFactory;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Terms;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IntsRef;
import org.apache.solr.analysis.TokenizerChain;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.BitDocSet;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.DocSlice;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SolrReturnFields;
import org.apache.solr.search.SyntaxError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scans posted text, looking for matching strings in the Solr index.
 * The public static final String members are request parameters.
 * This handler is also called the "SolrTextTagger".
 *
 * @since 7.4.0
 */
public class TaggerRequestHandler extends RequestHandlerBase {

  /** Request parameter. */
  public static final String OVERLAPS = "overlaps";
  /** Request parameter. */
  public static final String TAGS_LIMIT = "tagsLimit";
  /** Request parameter. */
  public static final String MATCH_TEXT = "matchText";
  /** Request parameter. */
  public static final String SKIP_ALT_TOKENS = "skipAltTokens";
  /** Request parameter. */
  public static final String IGNORE_STOPWORDS = "ignoreStopwords";
  /** Request parameter. */
  public static final String XML_OFFSET_ADJUST = "xmlOffsetAdjust";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public String getDescription() {
    return "Processes input text to find matching tokens stored in the index.";
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {

    //--Read params
    final String indexedField = req.getParams().get("field");
    if (indexedField == null)
      throw new RuntimeException("required param 'field'");

    final TagClusterReducer tagClusterReducer =
            chooseTagClusterReducer(req.getParams().get(OVERLAPS));
    final int rows = req.getParams().getInt(CommonParams.ROWS, 10000);
    final int tagsLimit = req.getParams().getInt(TAGS_LIMIT, 1000);
    final boolean addMatchText = req.getParams().getBool(MATCH_TEXT, false);
    final SchemaField idSchemaField = req.getSchema().getUniqueKeyField();
    if (idSchemaField == null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "The tagger requires a" +
              "uniqueKey in the schema.");//TODO this could be relaxed
    }
    final boolean skipAltTokens = req.getParams().getBool(SKIP_ALT_TOKENS, false);
    final boolean ignoreStopWords = req.getParams().getBool(IGNORE_STOPWORDS,
            fieldHasIndexedStopFilter(indexedField, req));

    //--Get posted data
    Reader inputReader = null;
    Iterable<ContentStream> streams = req.getContentStreams();
    if (streams != null) {
      Iterator<ContentStream> iter = streams.iterator();
      if (iter.hasNext()) {
        inputReader = iter.next().getReader();
      }
      if (iter.hasNext()) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            getClass().getSimpleName()+" does not support multiple ContentStreams"); //TODO support bulk tagging?
      }
    }
    if (inputReader == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          getClass().getSimpleName()+" requires text to be POSTed to it");
    }

    // We may or may not need to read the input into a string
    final InputStringLazy inputStringFuture = new InputStringLazy(inputReader);

    final OffsetCorrector offsetCorrector = getOffsetCorrector(req.getParams(), inputStringFuture);

    final String inputString;//only populated if needed
    if (addMatchText || inputStringFuture.inputString != null) {
      //Read the input fully into a String buffer that we'll need later,
      // then replace the input with a reader wrapping the buffer.
      inputString = inputStringFuture.call();
      inputReader.close();
      inputReader = new StringReader(inputString);
    } else {
      inputString = null;//not used
    }

    final SolrIndexSearcher searcher = req.getSearcher();
    final FixedBitSet matchDocIdsBS = new FixedBitSet(searcher.maxDoc());
    @SuppressWarnings({"rawtypes"})
    final List tags = new ArrayList(2000);

    try {
      Analyzer analyzer = req.getSchema().getField(indexedField).getType().getQueryAnalyzer();
      try (TokenStream tokenStream = analyzer.tokenStream("", inputReader)) {
        Terms terms = searcher.getSlowAtomicReader().terms(indexedField);
        if (terms != null) {
          Tagger tagger = new Tagger(terms, computeDocCorpus(req), tokenStream, tagClusterReducer,
              skipAltTokens, ignoreStopWords) {
            @SuppressWarnings("unchecked")
            @Override
            protected void tagCallback(int startOffset, int endOffset, Object docIdsKey) {
              if (tags.size() >= tagsLimit)
                return;
              if (offsetCorrector != null) {
                int[] offsetPair = offsetCorrector.correctPair(startOffset, endOffset);
                if (offsetPair == null) {
                  log.debug("Discarded offsets [{}, {}] because couldn't balance XML.",
                      startOffset, endOffset);
                  return;
                }
                startOffset = offsetPair[0];
                endOffset = offsetPair[1];
              }

              @SuppressWarnings({"rawtypes"})
              NamedList tag = new NamedList();
              tag.add("startOffset", startOffset);
              tag.add("endOffset", endOffset);
              if (addMatchText)
                tag.add("matchText", inputString.substring(startOffset, endOffset));
              //below caches, and also flags matchDocIdsBS
              tag.add("ids", lookupSchemaDocIds(docIdsKey));
              tags.add(tag);
            }

            @SuppressWarnings({"rawtypes"})
            Map<Object, List> docIdsListCache = new HashMap<>(2000);

            ValueSourceAccessor uniqueKeyCache = new ValueSourceAccessor(searcher,
                idSchemaField.getType().getValueSource(idSchemaField, null));

            @SuppressWarnings({"unchecked", "rawtypes"})
            private List lookupSchemaDocIds(Object docIdsKey) {
              List schemaDocIds = docIdsListCache.get(docIdsKey);
              if (schemaDocIds != null)
                return schemaDocIds;
              IntsRef docIds = lookupDocIds(docIdsKey);
              //translate lucene docIds to schema ids
              schemaDocIds = new ArrayList<>(docIds.length);
              for (int i = docIds.offset; i < docIds.offset + docIds.length; i++) {
                int docId = docIds.ints[i];
                assert i == docIds.offset || docIds.ints[i - 1] < docId : "not sorted?";
                matchDocIdsBS.set(docId);//also, flip docid in bitset
                try {
                  schemaDocIds.add(uniqueKeyCache.objectVal(docId));//translates here
                } catch (IOException e) {
                  throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
                }
              }
              assert !schemaDocIds.isEmpty();

              docIdsListCache.put(docIds, schemaDocIds);
              return schemaDocIds;
            }

          };
          tagger.enableDocIdsCache(2000);//TODO configurable
          tagger.process();
        }
      }
    } finally {
      inputReader.close();
    }
    rsp.add("tagsCount",tags.size());
    rsp.add("tags", tags);

    rsp.setReturnFields(new SolrReturnFields( req ));

    //Solr's standard name for matching docs in response
    rsp.add("response", getDocList(rows, matchDocIdsBS));
  }

  private static class InputStringLazy implements Callable<String> {
    final Reader inputReader;
    String inputString;

    InputStringLazy(Reader inputReader) {
      this.inputReader = inputReader;
    }

    @Override
    public String call() throws IOException {
      if (inputString == null) {
        inputString = CharStreams.toString(inputReader);
      }
      return inputString;
    }
  }

  protected OffsetCorrector getOffsetCorrector(SolrParams params, Callable<String> inputStringProvider) throws Exception {
    final boolean xmlOffsetAdjust = params.getBool(XML_OFFSET_ADJUST, false);
    if (!xmlOffsetAdjust) {
      return null;
    }
    try {
      return new XmlOffsetCorrector(inputStringProvider.call());
    } catch (XMLStreamException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Expecting XML but wasn't: " + e, e);
    }
  }

  private DocList getDocList(int rows, FixedBitSet matchDocIdsBS) throws IOException {
    //Now we must supply a Solr DocList and add it to the response.
    //  Typically this is gotten via a SolrIndexSearcher.search(), but in this case we
    //  know exactly what documents to return, the order doesn't matter nor does
    //  scoring.
    //  Ideally an implementation of DocList could be directly implemented off
    //  of a BitSet, but there are way too many methods to implement for a minor
    //  payoff.
    int matchDocs = matchDocIdsBS.cardinality();
    int[] docIds = new int[ Math.min(rows, matchDocs) ];
    DocIdSetIterator docIdIter = new BitSetIterator(matchDocIdsBS, 1);
    for (int i = 0; i < docIds.length; i++) {
      docIds[i] = docIdIter.nextDoc();
    }
    return new DocSlice(0, docIds.length, docIds, null, matchDocs, 1f, TotalHits.Relation.EQUAL_TO);
  }

  private TagClusterReducer chooseTagClusterReducer(String overlaps) {
    TagClusterReducer tagClusterReducer;
    if (overlaps == null || overlaps.equals("NO_SUB")) {
      tagClusterReducer = TagClusterReducer.NO_SUB;
    } else if (overlaps.equals("ALL")) {
      tagClusterReducer = TagClusterReducer.ALL;
    } else if (overlaps.equals("LONGEST_DOMINANT_RIGHT")) {
      tagClusterReducer = TagClusterReducer.LONGEST_DOMINANT_RIGHT;
    } else {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "unknown tag overlap mode: "+overlaps);
    }
    return tagClusterReducer;
  }

  /**
   * The set of documents matching the provided 'fq' (filter query). Don't include deleted docs
   * either. If null is returned, then all docs are available.
   */
  private Bits computeDocCorpus(SolrQueryRequest req) throws SyntaxError, IOException {
    final String[] corpusFilterQueries = req.getParams().getParams("fq");
    final SolrIndexSearcher searcher = req.getSearcher();
    final Bits docBits;
    if (corpusFilterQueries != null && corpusFilterQueries.length > 0) {
      List<Query> filterQueries = new ArrayList<Query>(corpusFilterQueries.length);
      for (String corpusFilterQuery : corpusFilterQueries) {
        QParser qParser = QParser.getParser(corpusFilterQuery, null, req);
        try {
          filterQueries.add(qParser.parse());
        } catch (SyntaxError e) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
        }
      }

      final DocSet docSet = searcher.getDocSet(filterQueries);//hopefully in the cache
      //note: before Solr 4.7 we could call docSet.getBits() but no longer.
      if (docSet instanceof BitDocSet) {
        docBits = ((BitDocSet)docSet).getBits();
      } else {
        docBits = new Bits() {

          @Override
          public boolean get(int index) {
            return docSet.exists(index);
          }

          @Override
          public int length() {
            return searcher.maxDoc();
          }
        };
      }
    } else {
      docBits = searcher.getSlowAtomicReader().getLiveDocs();
    }
    return docBits;
  }

  private boolean fieldHasIndexedStopFilter(String field, SolrQueryRequest req) {
    FieldType fieldType = req.getSchema().getFieldType(field);
    Analyzer analyzer = fieldType.getIndexAnalyzer();//index analyzer
    if (analyzer instanceof TokenizerChain) {
      TokenizerChain tokenizerChain = (TokenizerChain) analyzer;
      TokenFilterFactory[] tokenFilterFactories = tokenizerChain.getTokenFilterFactories();
      for (TokenFilterFactory tokenFilterFactory : tokenFilterFactories) {
        if (tokenFilterFactory instanceof StopFilterFactory)
          return true;
      }
    }
    return false;
  }

  /** See LUCENE-4541 or {@link org.apache.solr.response.transform.ValueSourceAugmenter}. */
  static class ValueSourceAccessor {
    private final List<LeafReaderContext> readerContexts;
    private final ValueSource valueSource;
    @SuppressWarnings({"rawtypes"})
    private final Map fContext;
    private final FunctionValues[] functionValuesPerSeg;
    private final int[] functionValuesDocIdPerSeg;

    ValueSourceAccessor(IndexSearcher searcher, ValueSource valueSource) {
      readerContexts = searcher.getIndexReader().leaves();
      this.valueSource = valueSource;
      fContext = ValueSource.newContext(searcher);
      functionValuesPerSeg = new FunctionValues[readerContexts.size()];
      functionValuesDocIdPerSeg = new int[readerContexts.size()];
    }

    @SuppressWarnings({"unchecked"})
    Object objectVal(int topDocId) throws IOException {
      // lookup segment level stuff:
      int segIdx = ReaderUtil.subIndex(topDocId, readerContexts);
      LeafReaderContext rcontext = readerContexts.get(segIdx);
      int segDocId = topDocId - rcontext.docBase;
      // unfortunately Lucene 7.0 requires forward only traversal (with no reset method).
      //   So we need to track our last docId (per segment) and re-fetch the FunctionValues. :-(
      FunctionValues functionValues = functionValuesPerSeg[segIdx];
      if (functionValues == null || segDocId < functionValuesDocIdPerSeg[segIdx]) {
        functionValues = functionValuesPerSeg[segIdx] = valueSource.getValues(fContext, rcontext);
      }
      functionValuesDocIdPerSeg[segIdx] = segDocId;

      // get value:
      return functionValues.objectVal(segDocId);
    }
  }
}
