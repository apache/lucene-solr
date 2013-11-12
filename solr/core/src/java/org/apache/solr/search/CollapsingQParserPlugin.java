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

package org.apache.solr.search;

import org.apache.lucene.util.BytesRef;
import org.apache.solr.schema.TrieFloatField;
import org.apache.solr.schema.TrieIntField;
import org.apache.solr.schema.TrieLongField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.handler.component.QueryElevationComponent;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.search.*;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util.Bits;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;

import com.carrotsearch.hppc.FloatArrayList;
import com.carrotsearch.hppc.IntOpenHashSet;
import com.carrotsearch.hppc.cursors.IntCursor;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.Iterator;

/**

 The <b>CollapsingQParserPlugin</b> is a PostFilter that performs field collapsing.
 This is a high performance alternative to standard Solr
 field collapsing (with ngroups) when the number of distinct groups
 in the result set is high.
 <p/>
 Sample syntax:
 <p/>
 Collapse based on the highest scoring document:
 <p/>

 fq=(!collapse field=field_name}

 <p/>
 Collapse based on the min value of a numeric field:
 <p/>
 fq={!collapse field=field_name min=field_name}
 <p/>
 Collapse based on the max value of a numeric field:
 <p/>
 fq={!collapse field=field_name max=field_name}
 <p/>
 Collapse with a null policy:
 <p/>
 fq={!collapse field=field_name nullPolicy=nullPolicy}
 <p/>
 There are three null policies: <br/>
 ignore : removes docs with a null value in the collapse field (default).<br/>
 expand : treats each doc with a null value in the collapse field as a separate group.<br/>
 collapse : collapses all docs with a null value into a single group using either highest score, or min/max.
 <p/>
 The CollapsingQParserPlugin fully supports the QueryElevationComponent


 **/

public class CollapsingQParserPlugin extends QParserPlugin {

  public static final String NAME = "collapse";
  public static final String NULL_COLLAPSE = "collapse";
  public static final String NULL_IGNORE = "ignore";
  public static final String NULL_EXPAND = "expand";


  public void init(NamedList namedList) {

  }

  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest request) {
    return new CollapsingQParser(qstr, localParams, params, request);
  }

  private class CollapsingQParser extends QParser {

    public CollapsingQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest request) {
      super(qstr, localParams, params, request);
    }

    public Query parse() throws SyntaxError {
      try {
        return new CollapsingPostFilter(localParams, params, req);
      } catch (Exception e) {
        throw new SyntaxError(e.getMessage(), e);
      }
    }
  }

  private class CollapsingPostFilter extends ExtendedQueryBase implements PostFilter {

    private Object cacheId;
    private String field;
    private int leafCount;
    private SortedDocValues docValues;
    private int maxDoc;
    private String max;
    private String min;
    private FieldType fieldType;
    private int nullPolicy;
    private SolrIndexSearcher searcher;
    private SolrParams solrParams;
    private Map context;
    private IndexSchema schema;
    public static final int NULL_POLICY_IGNORE = 0;
    public static final int NULL_POLICY_COLLAPSE = 1;
    public static final int NULL_POLICY_EXPAND = 2;

    public void setCache(boolean cache) {

    }

    public void setCacheSep(boolean cacheSep) {

    }

    public boolean getCacheSep() {
      return false;
    }

    public boolean getCache() {
      return false;
    }

    public int hashCode() {
      return this.cacheId.hashCode()*((1+Float.floatToIntBits(this.getBoost()))*31);
    }

    public boolean equals(Object o) {
      //Uses the unique id for equals to ensure that the query result cache always fails.
      if(o instanceof CollapsingPostFilter) {
        CollapsingPostFilter c = (CollapsingPostFilter)o;
        //Do object comparison to be sure only the same object will return true.
        if(this.cacheId == c.cacheId && this.getBoost()==c.getBoost()) {
          return true;
        }
      }
      return false;
    }

    public int getCost() {
      return Math.max(super.getCost(), 100);
    }

    public String toString(String s) {
      return s;
    }

    public CollapsingPostFilter(SolrParams localParams, SolrParams params, SolrQueryRequest request) throws IOException {
      this.cacheId = new Object();
      this.field = localParams.get("field");
      this.solrParams = params;
      String nPolicy = localParams.get("nullPolicy", NULL_IGNORE);
      if(nPolicy.equals(NULL_IGNORE)) {
        this.nullPolicy = NULL_POLICY_IGNORE;
      } else if (nPolicy.equals(NULL_COLLAPSE)) {
        this.nullPolicy = NULL_POLICY_COLLAPSE;
      } else if(nPolicy.equals((NULL_EXPAND))) {
        this.nullPolicy = NULL_POLICY_EXPAND;
      } else {
        throw new IOException("Invalid nullPolicy:"+nPolicy);
      }
      this.searcher = request.getSearcher();
      this.leafCount = searcher.getTopReaderContext().leaves().size();
      this.maxDoc = searcher.maxDoc();
      this.schema = searcher.getSchema();
      SchemaField schemaField = schema.getField(this.field);
      if(schemaField.hasDocValues()) {
        this.docValues = searcher.getAtomicReader().getSortedDocValues(this.field);
      } else {
        this.docValues = FieldCache.DEFAULT.getTermsIndex(searcher.getAtomicReader(), this.field);
      }

      this.max = localParams.get("max");
      if(this.max != null) {
        this.fieldType = searcher.getSchema().getField(this.max).getType();
      }

      this.min = localParams.get("min");
      if(this.min != null) {
        this.fieldType = searcher.getSchema().getField(this.min).getType();
      }

      this.context = request.getContext();
    }

    private IntOpenHashSet getBoostDocs(IndexSearcher indexSearcher, Set<String> boosted) throws IOException {
      IntOpenHashSet boostDocs = null;
      if(boosted != null) {
        SchemaField idField = this.schema.getUniqueKeyField();
        String fieldName = idField.getName();
        HashSet<BytesRef> localBoosts = new HashSet(boosted.size()*2);
        Iterator<String> boostedIt = boosted.iterator();
        while(boostedIt.hasNext()) {
          localBoosts.add(new BytesRef(boostedIt.next()));
        }

        boostDocs = new IntOpenHashSet(boosted.size()*2);

        List<AtomicReaderContext>leaves = indexSearcher.getTopReaderContext().leaves();
        TermsEnum termsEnum = null;
        DocsEnum docsEnum = null;
        for(AtomicReaderContext leaf : leaves) {
          AtomicReader reader = leaf.reader();
          int docBase = leaf.docBase;
          Bits liveDocs = reader.getLiveDocs();
          Terms terms = reader.terms(fieldName);
          termsEnum = terms.iterator(termsEnum);
          Iterator<BytesRef> it = localBoosts.iterator();
          while(it.hasNext()) {
            BytesRef ref = it.next();
            if(termsEnum.seekExact(ref)) {
              docsEnum = termsEnum.docs(liveDocs, docsEnum);
              int doc = docsEnum.nextDoc();
              if(doc != -1) {
                //Found the document.
                boostDocs.add(doc+docBase);
                it.remove();
              }
            }
          }
        }
      }

      return boostDocs;
    }

    public DelegatingCollector getFilterCollector(IndexSearcher indexSearcher) {
      try {
        IntOpenHashSet boostDocs = getBoostDocs(indexSearcher, (Set<String>) (this.context.get(QueryElevationComponent.BOOSTED)));

        if(this.min != null || this.max != null) {

          return new CollapsingFieldValueCollector(this.maxDoc,
              this.leafCount,
              this.docValues,
              this.searcher,
              this.nullPolicy,
              max != null ? this.max : this.min,
              max != null,
              needsScores(this.solrParams),
              this.fieldType,
              boostDocs);
        } else {
          return new CollapsingScoreCollector(this.maxDoc, this.leafCount, this.docValues, this.nullPolicy, boostDocs);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private boolean needsScores(SolrParams params) {

      String sortSpec = params.get("sort");
      if(sortSpec != null) {
        String[] sorts = sortSpec.split(",");
        for(String s: sorts) {
          String parts[] = s.split(" ");
          if(parts[0].equals("score")) {
            return true;
          }
        }
      } else {
        //No sort specified so it defaults to score.
        return true;
      }

      String fl = params.get("fl");
      if(fl != null) {
        String[] fls = fl.split(",");
        for(String f : fls) {
          if(f.trim().equals("score")) {
            return true;
          }
        }
      }

      if(this.context.containsKey(QueryElevationComponent.BOOSTED)) {
        return true;
      }

      return false;
    }
  }

  private class DummyScorer extends Scorer {

    public float score;

    public DummyScorer() {
      super(null);
    }

    public float score() {
      return score;
    }

    public int freq() {
      return 0;
    }

    public int advance(int i) {
      return -1;
    }

    public int nextDoc() {
      return 0;
    }

    public int docID() {
      return 0;
    }

    public long cost() {
      return 0;
    }
  }


  private class CollapsingScoreCollector extends DelegatingCollector {

    private AtomicReaderContext[] contexts;
    private OpenBitSet collapsedSet;
    private SortedDocValues values;
    private int[] ords;
    private float[] scores;
    private int docBase;
    private int maxDoc;
    private int nullPolicy;
    private float nullScore = -Float.MAX_VALUE;
    private int nullDoc;
    private FloatArrayList nullScores;
    private IntOpenHashSet boostDocs;

    public CollapsingScoreCollector(int maxDoc,
                                    int segments,
                                    SortedDocValues values,
                                    int nullPolicy,
                                    IntOpenHashSet boostDocs) {
      this.maxDoc = maxDoc;
      this.contexts = new AtomicReaderContext[segments];
      this.collapsedSet = new OpenBitSet(maxDoc);
      this.boostDocs = boostDocs;
      if(this.boostDocs != null) {
        //Set the elevated docs now.
        Iterator<IntCursor> it = this.boostDocs.iterator();
        while(it.hasNext()) {
          IntCursor cursor = it.next();
          this.collapsedSet.fastSet(cursor.value);
        }
      }
      this.values = values;
      int valueCount = values.getValueCount();
      this.ords = new int[valueCount];
      Arrays.fill(this.ords, -1);
      this.scores = new float[valueCount];
      Arrays.fill(this.scores, -Float.MAX_VALUE);
      this.nullPolicy = nullPolicy;
      if(nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
        nullScores = new FloatArrayList();
      }
    }

    public boolean acceptsDocsOutOfOrder() {
      //Documents must be sent in order to this collector.
      return false;
    }

    public void setNextReader(AtomicReaderContext context) throws IOException {
      this.contexts[context.ord] = context;
      this.docBase = context.docBase;
    }

    public void collect(int docId) throws IOException {
      int globalDoc = docId+this.docBase;
      int ord = values.getOrd(globalDoc);
      if(ord > -1) {
        float score = scorer.score();
        if(score > scores[ord]) {
          ords[ord] = globalDoc;
          scores[ord] = score;
        }
      } else if (this.collapsedSet.fastGet(globalDoc)) {
        //The doc is elevated so score does not matter
        //We just want to be sure it doesn't fall into the null policy
      } else if(nullPolicy == CollapsingPostFilter.NULL_POLICY_COLLAPSE) {
        float score = scorer.score();
        if(score > nullScore) {
          nullScore = score;
          nullDoc = globalDoc;
        }
      } else if(nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
        collapsedSet.fastSet(globalDoc);
        nullScores.add(scorer.score());
      }
    }

    public void finish() throws IOException {
      if(contexts.length == 0) {
        return;
      }

      if(nullScore > 0) {
        this.collapsedSet.fastSet(nullDoc);
      }

      for(int i=0; i<ords.length; i++) {
        int doc = ords[i];
        if(doc > -1) {
          collapsedSet.fastSet(doc);
        }
      }

      int currentContext = 0;
      int currentDocBase = 0;
      int nextDocBase = currentContext+1 < contexts.length ? contexts[currentContext+1].docBase : maxDoc;
      delegate.setNextReader(contexts[currentContext]);
      DummyScorer dummy = new DummyScorer();
      delegate.setScorer(dummy);
      DocIdSetIterator it = collapsedSet.iterator();
      int docId = -1;
      int nullScoreIndex = 0;
      while((docId = it.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {

        int ord = values.getOrd(docId);
        if(ord > -1) {
          dummy.score = scores[ord];
        } else if(this.boostDocs != null && boostDocs.contains(docId)) {
          //Elevated docs don't need a score.
          dummy.score = 0F;
        } else if (nullPolicy == CollapsingPostFilter.NULL_POLICY_COLLAPSE) {
          dummy.score = nullScore;
        } else if(nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
          dummy.score = nullScores.get(nullScoreIndex++);
        }

        while(docId >= nextDocBase) {
          currentContext++;
          currentDocBase = contexts[currentContext].docBase;
          nextDocBase = currentContext+1 < contexts.length ? contexts[currentContext+1].docBase : maxDoc;
          delegate.setNextReader(contexts[currentContext]);
          delegate.setScorer(dummy);
        }

        int contextDoc = docId-currentDocBase;
        delegate.collect(contextDoc);
      }

      if(delegate instanceof DelegatingCollector) {
        ((DelegatingCollector) delegate).finish();
      }
    }
  }

  private class CollapsingFieldValueCollector extends DelegatingCollector {
    private AtomicReaderContext[] contexts;
    private SortedDocValues values;

    private int docBase;
    private int maxDoc;
    private int nullPolicy;

    private FieldValueCollapse fieldValueCollapse;
    private boolean needsScores;
    private IntOpenHashSet boostDocs;

    public CollapsingFieldValueCollector(int maxDoc,
                                         int segments,
                                         SortedDocValues values,
                                         SolrIndexSearcher searcher,
                                         int nullPolicy,
                                         String field,
                                         boolean max,
                                         boolean needsScores,
                                         FieldType fieldType,
                                         IntOpenHashSet boostDocs) throws IOException{

      this.maxDoc = maxDoc;
      this.contexts = new AtomicReaderContext[segments];
      this.values = values;
      int valueCount = values.getValueCount();
      this.nullPolicy = nullPolicy;
      this.needsScores = needsScores;
      this.boostDocs = boostDocs;
      if(fieldType instanceof TrieIntField) {
        this.fieldValueCollapse = new IntValueCollapse(searcher, field, nullPolicy, new int[valueCount], max, this.needsScores, boostDocs);
      } else if(fieldType instanceof TrieLongField) {
        this.fieldValueCollapse =  new LongValueCollapse(searcher, field, nullPolicy, new int[valueCount], max, this.needsScores, boostDocs);
      } else if(fieldType instanceof TrieFloatField) {
        this.fieldValueCollapse =  new FloatValueCollapse(searcher, field, nullPolicy, new int[valueCount], max, this.needsScores, boostDocs);
      } else {
        throw new IOException("min/max must be either TrieInt, TrieLong or TrieFloat.");
      }
    }

    public boolean acceptsDocsOutOfOrder() {
      //Documents must be sent in order to this collector.
      return false;
    }

    public void setScorer(Scorer scorer) {
      this.fieldValueCollapse.setScorer(scorer);
    }

    public void setNextReader(AtomicReaderContext context) throws IOException {
      this.contexts[context.ord] = context;
      this.docBase = context.docBase;
      this.fieldValueCollapse.setNextReader(context);
    }

    public void collect(int docId) throws IOException {
      int globalDoc = docId+this.docBase;
      int ord = values.getOrd(globalDoc);
      fieldValueCollapse.collapse(ord, docId, globalDoc);
    }

    public void finish() throws IOException {
      if(contexts.length == 0) {
        return;
      }

      int currentContext = 0;
      int currentDocBase = 0;
      int nextDocBase = currentContext+1 < contexts.length ? contexts[currentContext+1].docBase : maxDoc;
      delegate.setNextReader(contexts[currentContext]);
      DummyScorer dummy = new DummyScorer();
      delegate.setScorer(dummy);
      DocIdSetIterator it = fieldValueCollapse.getCollapsedSet().iterator();
      int docId = -1;
      int nullScoreIndex = 0;
      float[] scores = fieldValueCollapse.getScores();
      FloatArrayList nullScores = fieldValueCollapse.getNullScores();
      float nullScore = fieldValueCollapse.getNullScore();
      while((docId = it.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {

        if(this.needsScores){
          int ord = values.getOrd(docId);
          if(ord > -1) {
            dummy.score = scores[ord];
          } else if (boostDocs != null && boostDocs.contains(docId)) {
            //Its an elevated doc so no score is needed
            dummy.score = 0F;
          } else if (nullPolicy == CollapsingPostFilter.NULL_POLICY_COLLAPSE) {
            dummy.score = nullScore;
          } else if(nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
            dummy.score = nullScores.get(nullScoreIndex++);
          }
        }

        while(docId >= nextDocBase) {
          currentContext++;
          currentDocBase = contexts[currentContext].docBase;
          nextDocBase = currentContext+1 < contexts.length ? contexts[currentContext+1].docBase : maxDoc;
          delegate.setNextReader(contexts[currentContext]);
          delegate.setScorer(dummy);
        }

        int contextDoc = docId-currentDocBase;
        delegate.collect(contextDoc);
      }

      if(delegate instanceof DelegatingCollector) {
        ((DelegatingCollector) delegate).finish();
      }
    }
  }

  private abstract class FieldValueCollapse {
    protected int nullPolicy;
    protected int[] ords;
    protected Scorer scorer;
    protected FloatArrayList nullScores;
    protected float nullScore;
    protected float[] scores;
    protected OpenBitSet collapsedSet;
    protected IntOpenHashSet boostDocs;
    protected int nullDoc = -1;
    protected boolean needsScores;
    protected boolean max;
    protected String field;

    public abstract void collapse(int ord, int contextDoc, int globalDoc) throws IOException;
    public abstract void setNextReader(AtomicReaderContext context) throws IOException;

    public FieldValueCollapse(SolrIndexSearcher searcher,
                              String field,
                              int nullPolicy,
                              boolean max,
                              boolean needsScores,
                              IntOpenHashSet boostDocs) {
      this.field = field;
      this.nullPolicy = nullPolicy;
      this.max = max;
      this.needsScores = needsScores;
      this.collapsedSet = new OpenBitSet(searcher.maxDoc());
      this.boostDocs = boostDocs;
      if(this.boostDocs != null) {
        Iterator<IntCursor> it = boostDocs.iterator();
        while(it.hasNext()) {
          IntCursor cursor = it.next();
          this.collapsedSet.fastSet(cursor.value);
        }
      }
    }

    public OpenBitSet getCollapsedSet() {
      if(nullDoc > -1) {
        this.collapsedSet.fastSet(nullDoc);
      }

      for(int i=0; i<ords.length; i++) {
        int doc = ords[i];
        if(doc > -1) {
          collapsedSet.fastSet(doc);
        }
      }

      return collapsedSet;
    }

    public void setScorer(Scorer scorer) {
      this.scorer = scorer;
    }

    public FloatArrayList getNullScores() {
      return nullScores;
    }

    public float getNullScore() {
      return this.nullScore;
    }

    public float[] getScores() {
      return scores;
    }
  }

  private class IntValueCollapse extends FieldValueCollapse {

    private FieldCache.Ints vals;
    private IntCompare comp;
    private int nullVal;
    private int[] ordVals;

    public IntValueCollapse(SolrIndexSearcher searcher,
                            String field,
                            int nullPolicy,
                            int[] ords,
                            boolean max,
                            boolean needsScores,
                            IntOpenHashSet boostDocs) throws IOException {
      super(searcher, field, nullPolicy, max, needsScores, boostDocs);
      this.ords = ords;
      this.ordVals = new int[ords.length];
      Arrays.fill(ords, -1);

      if(max) {
        comp = new MaxIntComp();
        Arrays.fill(ordVals, Integer.MIN_VALUE);
      } else {
        comp = new MinIntComp();
        Arrays.fill(ordVals, Integer.MAX_VALUE);
        this.nullVal = Integer.MAX_VALUE;
      }

      if(needsScores) {
        this.scores = new float[ords.length];
        if(nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
          nullScores = new FloatArrayList();
        }
      }
    }

    public void setNextReader(AtomicReaderContext context) throws IOException {
      this.vals = FieldCache.DEFAULT.getInts(context.reader(), this.field, false);
    }

    public void collapse(int ord, int contextDoc, int globalDoc) throws IOException {
      int val = vals.get(contextDoc);
      if(ord > -1) {
        if(comp.test(val, ordVals[ord])) {
          ords[ord] = globalDoc;
          ordVals[ord] = val;
          if(needsScores) {
            scores[ord] = scorer.score();
          }
        }
      } else if(this.collapsedSet.fastGet(globalDoc)) {
        // Elevated doc so do nothing.
      } else if(this.nullPolicy == CollapsingPostFilter.NULL_POLICY_COLLAPSE) {
        if(comp.test(val, nullVal)) {
          nullVal = val;
          nullDoc = globalDoc;
          if(needsScores) {
            nullScore = scorer.score();
          }
        }
      } else if(this.nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
        this.collapsedSet.fastSet(globalDoc);
        if(needsScores) {
          nullScores.add(scorer.score());
        }
      }
    }
  }

  private class LongValueCollapse extends FieldValueCollapse {

    private FieldCache.Longs vals;
    private LongCompare comp;
    private long nullVal;
    private long[] ordVals;

    public LongValueCollapse(SolrIndexSearcher searcher,
                             String field,
                             int nullPolicy,
                             int[] ords,
                             boolean max,
                             boolean needsScores,
                             IntOpenHashSet boostDocs) throws IOException {
      super(searcher, field, nullPolicy, max, needsScores, boostDocs);
      this.ords = ords;
      this.ordVals = new long[ords.length];
      Arrays.fill(ords, -1);

      if(max) {
        comp = new MaxLongComp();
        Arrays.fill(ordVals, Long.MIN_VALUE);
      } else {
        this.nullVal = Long.MAX_VALUE;
        comp = new MinLongComp();
        Arrays.fill(ordVals, Long.MAX_VALUE);
      }

      if(needsScores) {
        this.scores = new float[ords.length];
        if(nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
          nullScores = new FloatArrayList();
        }
      }
    }

    public void setNextReader(AtomicReaderContext context) throws IOException {
      this.vals = FieldCache.DEFAULT.getLongs(context.reader(), this.field, false);
    }

    public void collapse(int ord, int contextDoc, int globalDoc) throws IOException {
      long val = vals.get(contextDoc);
      if(ord > -1) {
        if(comp.test(val, ordVals[ord])) {
          ords[ord] = globalDoc;
          ordVals[ord] = val;
          if(needsScores) {
            scores[ord] = scorer.score();
          }
        }
      } else if (this.collapsedSet.fastGet(globalDoc)) {
        //Elevated doc so do nothing
      } else if(this.nullPolicy == CollapsingPostFilter.NULL_POLICY_COLLAPSE) {
        if(comp.test(val, nullVal)) {
          nullVal = val;
          nullDoc = globalDoc;
          if(needsScores) {
            nullScore = scorer.score();
          }
        }
      } else if(this.nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
        this.collapsedSet.fastSet(globalDoc);
        if(needsScores) {
          nullScores.add(scorer.score());
        }
      }
    }
  }

  private class FloatValueCollapse extends FieldValueCollapse {

    private FieldCache.Floats vals;
    private FloatCompare comp;
    private float nullVal;
    private float[] ordVals;

    public FloatValueCollapse(SolrIndexSearcher searcher,
                              String field,
                              int nullPolicy,
                              int[] ords,
                              boolean max,
                              boolean needsScores,
                              IntOpenHashSet boostDocs) throws IOException {
      super(searcher, field, nullPolicy, max, needsScores, boostDocs);
      this.ords = ords;
      this.ordVals = new float[ords.length];
      Arrays.fill(ords, -1);

      if(max) {
        comp = new MaxFloatComp();
        Arrays.fill(ordVals, -Float.MAX_VALUE );
      } else {
        this.nullVal = Float.MAX_VALUE;
        comp = new MinFloatComp();
        Arrays.fill(ordVals, Float.MAX_VALUE);
      }

      if(needsScores) {
        this.scores = new float[ords.length];
        if(nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
          nullScores = new FloatArrayList();
        }
      }
    }

    public void setNextReader(AtomicReaderContext context) throws IOException {
      this.vals = FieldCache.DEFAULT.getFloats(context.reader(), this.field, false);
    }

    public void collapse(int ord, int contextDoc, int globalDoc) throws IOException {
      float val = vals.get(contextDoc);
      if(ord > -1) {
        if(comp.test(val, ordVals[ord])) {
          ords[ord] = globalDoc;
          ordVals[ord] = val;
          if(needsScores) {
            scores[ord] = scorer.score();
          }
        }
      } else if (this.collapsedSet.fastGet(globalDoc)) {
        //Elevated doc so do nothing
      } else if(this.nullPolicy == CollapsingPostFilter.NULL_POLICY_COLLAPSE) {
        if(comp.test(val, nullVal)) {
          nullVal = val;
          nullDoc = globalDoc;
          if(needsScores) {
            nullScore = scorer.score();
          }
        }
      } else if(this.nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
        this.collapsedSet.fastSet(globalDoc);
        if(needsScores) {
          nullScores.add(scorer.score());
        }
      }
    }
  }

  private interface IntCompare {
    public boolean test(int i1, int i2);
  }

  private interface FloatCompare {
    public boolean test(float i1, float i2);
  }

  private interface LongCompare {
    public boolean test(long i1, long i2);
  }

  private class MaxIntComp implements IntCompare {
    public boolean test(int i1, int i2) {
      return i1 > i2;
    }
  }

  private class MinIntComp implements IntCompare {
    public boolean test(int i1, int i2) {
      return i1 < i2;
    }
  }

  private class MaxFloatComp implements FloatCompare {
    public boolean test(float i1, float i2) {
      return i1 > i2;
    }
  }

  private class MinFloatComp implements FloatCompare {
    public boolean test(float i1, float i2) {
      return i1 < i2;
    }
  }

  private class MaxLongComp implements LongCompare {
    public boolean test(long i1, long i2) {
      return i1 > i2;
    }
  }

  private class MinLongComp implements LongCompare {
    public boolean test(long i1, long i2) {
      return i1 < i2;
    }
  }
}
