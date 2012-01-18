/**
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

package org.apache.solr.handler.admin;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.*;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.ReaderUtil;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.solr.analysis.CharFilterFactory;
import org.apache.solr.analysis.TokenFilterFactory;
import org.apache.solr.analysis.TokenizerChain;
import org.apache.solr.analysis.TokenizerFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.luke.FieldFlag;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Base64;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.lucene.index.FieldInfo.IndexOptions.DOCS_AND_FREQS;
import static org.apache.lucene.index.FieldInfo.IndexOptions.DOCS_ONLY;

/**
 * This handler exposes the internal lucene index.  It is inspired by and 
 * modeled on Luke, the Lucene Index Browser by Andrzej Bialecki.
 *   http://www.getopt.org/luke/
 * <p>
 * NOTE: the response format is still likely to change.  It should be designed so
 * that it works nicely with an XSLT transformation.  Until we have a nice
 * XSLT front end for /admin, the format is still open to change.
 * </p>
 *
 * For more documentation see:
 *  http://wiki.apache.org/solr/LukeRequestHandler
 *
 *
 * @since solr 1.2
 */
public class LukeRequestHandler extends RequestHandlerBase
{
  private static Logger log = LoggerFactory.getLogger(LukeRequestHandler.class);

  public static final String NUMTERMS = "numTerms";
  public static final String DOC_ID = "docId";
  public static final String ID = "id";
  public static final int DEFAULT_COUNT = 10;
  
  static final int HIST_ARRAY_SIZE = 33;

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception
  {
    IndexSchema schema = req.getSchema();
    SolrIndexSearcher searcher = req.getSearcher();
    IndexReader reader = searcher.getIndexReader();
    SolrParams params = req.getParams();
    int numTerms = params.getInt( NUMTERMS, DEFAULT_COUNT );

    // Always show the core lucene info
    Map<String, TopTermQueue> topTerms = new TreeMap<String, TopTermQueue>();

    // If no doc is given, show all fields and top terms
    Set<String> fields = null;
    if( params.get( CommonParams.FL ) != null ) {
      fields = new TreeSet<String>(Arrays.asList(params.getParams( CommonParams.FL )));
    }
    if ( "schema".equals( params.get( "show" ))) {
      numTerms = 0; // Abort any statistics gathering.
    }
    rsp.add("index", getIndexInfo(reader, numTerms, topTerms, fields ));

    Integer docId = params.getInt( DOC_ID );
    if( docId == null && params.get( ID ) != null ) {
      // Look for something with a given solr ID
      SchemaField uniqueKey = schema.getUniqueKeyField();
      String v = uniqueKey.getType().toInternal( params.get(ID) );
      Term t = new Term( uniqueKey.getName(), v );
      docId = searcher.getFirstMatch( t );
      if( docId < 0 ) {
        throw new SolrException( SolrException.ErrorCode.NOT_FOUND, "Can't find document: "+params.get( ID ) );
      }
    }

    // Read the document from the index
    if( docId != null ) {
      Document doc = null;
      try {
        doc = reader.document( docId );
      }
      catch( Exception ex ) {}
      if( doc == null ) {
        throw new SolrException( SolrException.ErrorCode.NOT_FOUND, "Can't find document: "+docId );
      }

      SimpleOrderedMap<Object> info = getDocumentFieldsInfo( doc, docId, reader, schema );

      SimpleOrderedMap<Object> docinfo = new SimpleOrderedMap<Object>();
      docinfo.add( "docId", docId );
      docinfo.add( "lucene", info );
      docinfo.add( "solr", doc );
      rsp.add( "doc", docinfo );
    }
    else if ( "schema".equals( params.get( "show" ) ) ) {
      rsp.add( "schema", getSchemaInfo( req.getSchema() ) );
    }
    else {
      rsp.add( "fields", getIndexedFieldsInfo( searcher, fields, numTerms, topTerms) ) ;
    }

    // Add some generally helpful information
    NamedList<Object> info = new SimpleOrderedMap<Object>();
    info.add( "key", getFieldFlagsKey() );
    info.add( "NOTE", "Document Frequency (df) is not updated when a document is marked for deletion.  df values include deleted documents." );
    rsp.add( "info", info );
    rsp.setHttpCaching(false);
  }



  /**
   * @return a string representing a IndexableField's flags.  
   */
  private static String getFieldFlags( IndexableField f )
  {
    IndexOptions opts = (f == null) ? null : f.fieldType().indexOptions();

    StringBuilder flags = new StringBuilder();

    flags.append( (f != null && f.fieldType().indexed())                     ? FieldFlag.INDEXED.getAbbreviation() : '-' );
    flags.append( (f != null && f.fieldType().tokenized())                   ? FieldFlag.TOKENIZED.getAbbreviation() : '-' );
    flags.append( (f != null && f.fieldType().stored())                      ? FieldFlag.STORED.getAbbreviation() : '-' );
    flags.append( (false)                                          ? FieldFlag.MULTI_VALUED.getAbbreviation() : '-' ); // SchemaField Specific
    flags.append( (f != null && f.fieldType().storeTermVectors())            ? FieldFlag.TERM_VECTOR_STORED.getAbbreviation() : '-' );
    flags.append( (f != null && f.fieldType().storeTermVectorOffsets())   ? FieldFlag.TERM_VECTOR_OFFSET.getAbbreviation() : '-' );
    flags.append( (f != null && f.fieldType().storeTermVectorPositions()) ? FieldFlag.TERM_VECTOR_POSITION.getAbbreviation() : '-' );
    flags.append( (f != null && f.fieldType().omitNorms())                  ? FieldFlag.OMIT_NORMS.getAbbreviation() : '-' );

    flags.append( (f != null && DOCS_ONLY == opts ) ?
        FieldFlag.OMIT_TF.getAbbreviation() : '-' );

    flags.append((f != null && DOCS_AND_FREQS == opts) ?
        FieldFlag.OMIT_POSITIONS.getAbbreviation() : '-');

    flags.append( (f != null && f.getClass().getSimpleName().equals("LazyField")) ? FieldFlag.LAZY.getAbbreviation() : '-' );
    flags.append( (f != null && f.binaryValue()!=null)                      ? FieldFlag.BINARY.getAbbreviation() : '-' );
    flags.append( (false)                                          ? FieldFlag.SORT_MISSING_FIRST.getAbbreviation() : '-' ); // SchemaField Specific
    flags.append( (false)                                          ? FieldFlag.SORT_MISSING_LAST.getAbbreviation() : '-' ); // SchemaField Specific
    return flags.toString();
  }

  /**
   * @return a string representing a SchemaField's flags.  
   */
  private static String getFieldFlags( SchemaField f )
  {
    FieldType t = (f==null) ? null : f.getType();

    // see: http://www.nabble.com/schema-field-properties-tf3437753.html#a9585549
    boolean lazy = false; // "lazy" is purely a property of reading fields
    boolean binary = false; // Currently not possible

    StringBuilder flags = new StringBuilder();
    flags.append( (f != null && f.indexed())             ? FieldFlag.INDEXED.getAbbreviation() : '-' );
    flags.append( (t != null && t.isTokenized())         ? FieldFlag.TOKENIZED.getAbbreviation() : '-' );
    flags.append( (f != null && f.stored())              ? FieldFlag.STORED.getAbbreviation() : '-' );
    flags.append( (f != null && f.multiValued())         ? FieldFlag.MULTI_VALUED.getAbbreviation() : '-' );
    flags.append( (f != null && f.storeTermVector() )    ? FieldFlag.TERM_VECTOR_STORED.getAbbreviation() : '-' );
    flags.append( (f != null && f.storeTermOffsets() )   ? FieldFlag.TERM_VECTOR_OFFSET.getAbbreviation() : '-' );
    flags.append( (f != null && f.storeTermPositions() ) ? FieldFlag.TERM_VECTOR_POSITION.getAbbreviation() : '-' );
    flags.append( (f != null && f.omitNorms())           ? FieldFlag.OMIT_NORMS.getAbbreviation() : '-' );
    flags.append( (f != null &&
        f.omitTermFreqAndPositions() )        ? FieldFlag.OMIT_TF.getAbbreviation() : '-' );
    flags.append( (f != null && f.omitPositions() )      ? FieldFlag.OMIT_POSITIONS.getAbbreviation() : '-' );
    flags.append( (lazy)                                 ? FieldFlag.LAZY.getAbbreviation() : '-' );
    flags.append( (binary)                               ? FieldFlag.BINARY.getAbbreviation() : '-' );
    flags.append( (f != null && f.sortMissingFirst() )   ? FieldFlag.SORT_MISSING_FIRST.getAbbreviation() : '-' );
    flags.append( (f != null && f.sortMissingLast() )    ? FieldFlag.SORT_MISSING_LAST.getAbbreviation() : '-' );
    return flags.toString();
  }

  /**
   * @return a key to what each character means
   */
  public static SimpleOrderedMap<String> getFieldFlagsKey() {
    SimpleOrderedMap<String> key = new SimpleOrderedMap<String>();
    for (FieldFlag f : FieldFlag.values()) {
      key.add(String.valueOf(f.getAbbreviation()), f.getDisplay() );
    }
    return key;
  }

  private static SimpleOrderedMap<Object> getDocumentFieldsInfo( Document doc, int docId, IndexReader reader, IndexSchema schema ) throws IOException
  {
    final CharsRef spare = new CharsRef();
    SimpleOrderedMap<Object> finfo = new SimpleOrderedMap<Object>();
    for( Object o : doc.getFields() ) {
      Field field = (Field)o;
      SimpleOrderedMap<Object> f = new SimpleOrderedMap<Object>();

      SchemaField sfield = schema.getFieldOrNull( field.name() );
      FieldType ftype = (sfield==null)?null:sfield.getType();

      f.add( "type", (ftype==null)?null:ftype.getTypeName() );
      f.add( "schema", getFieldFlags( sfield ) );
      f.add( "flags", getFieldFlags( field ) );

      Term t = new Term(field.name(), ftype!=null ? ftype.storedToIndexed(field) : field.stringValue());

      f.add( "value", (ftype==null)?null:ftype.toExternal( field ) );

      // TODO: this really should be "stored"
      f.add( "internal", field.stringValue() );  // may be a binary number

      BytesRef bytes = field.binaryValue();
      if (bytes != null) {
        f.add( "binary", Base64.byteArrayToBase64(bytes.bytes, bytes.offset, bytes.length));
      }
      f.add( "boost", field.boost() );
      f.add( "docFreq", t.text()==null ? 0 : reader.docFreq( t ) ); // this can be 0 for non-indexed fields

      // If we have a term vector, return that
      if( field.fieldType().storeTermVectors() ) {
        try {
          Terms v = reader.getTermVector( docId, field.name() );
          if( v != null ) {
            SimpleOrderedMap<Integer> tfv = new SimpleOrderedMap<Integer>();
            final TermsEnum termsEnum = v.iterator(null);
            BytesRef text;
            while((text = termsEnum.next()) != null) {
              final int freq = (int) termsEnum.totalTermFreq();
              UnicodeUtil.UTF8toUTF16(text, spare);
              tfv.add(spare.toString(), freq);
            }
            f.add( "termVector", tfv );
          }
        }
        catch( Exception ex ) {
          log.warn( "error writing term vector", ex );
        }
      }

      finfo.add( field.name(), f );
    }
    return finfo;
  }

  @SuppressWarnings("unchecked")
  private static SimpleOrderedMap<Object> getIndexedFieldsInfo(
      final SolrIndexSearcher searcher, final Set<String> fields, final int numTerms, Map<String,TopTermQueue> ttinfo)
      throws Exception {

    IndexReader reader = searcher.getIndexReader();
    IndexSchema schema = searcher.getSchema();

    Set<String> fieldNames = new TreeSet<String>();
    for(FieldInfo fieldInfo : ReaderUtil.getMergedFieldInfos(reader)) {
      fieldNames.add(fieldInfo.name);
    }

    // Walk the term enum and keep a priority queue for each map in our set
    SimpleOrderedMap<Object> finfo = new SimpleOrderedMap<Object>();
    Fields theFields = MultiFields.getFields(reader);

    for (String fieldName : fieldNames) {
      if (fields != null && ! fields.contains(fieldName)) {
        continue; // we're not interested in this term
      }

      SimpleOrderedMap<Object> f = new SimpleOrderedMap<Object>();

      SchemaField sfield = schema.getFieldOrNull( fieldName );
      FieldType ftype = (sfield==null)?null:sfield.getType();

      f.add( "type", (ftype==null)?null:ftype.getTypeName() );
      f.add( "schema", getFieldFlags( sfield ) );
      if (sfield != null && schema.isDynamicField(sfield.getName()) && schema.getDynamicPattern(sfield.getName()) != null) {
        f.add("dynamicBase", schema.getDynamicPattern(sfield.getName()));
      }

      Terms terms = theFields.terms(fieldName);
      if (terms == null) { // Not indexed, so we need to report what we can (it made it through the fl param if specified)
        finfo.add( fieldName, f );
        continue;
      }

      TopTermQueue topTerms = ttinfo.get( fieldName );
      // If numTerms==0, the call is just asking for a quick field list
      if( ttinfo != null && sfield != null && sfield.indexed() ) {
        if (numTerms > 0) { // Read the actual field from the index and report that too.
          Document doc = null;
          if (topTerms != null && topTerms.getTopTermInfo() != null) {
            Term term = topTerms.getTopTermInfo().term;
            DocsEnum docsEnum = MultiFields.getTermDocsEnum(reader,
                MultiFields.getLiveDocs(reader),
                term.field(),
                new BytesRef(term.text()),
                false);
            if (docsEnum != null) {
              int docId;
              if ((docId = docsEnum.nextDoc()) != DocsEnum.NO_MORE_DOCS) {
                doc = reader.document(docId);
              }
            }
          }
          if( doc != null ) {
            // Found a document with this field
            try {
              IndexableField fld = doc.getField( fieldName );
              if( fld != null ) {
                f.add( "index", getFieldFlags( fld ) );
              }
              else {
                // it is a non-stored field...
                f.add( "index", "(unstored field)" );
              }
            }
            catch( Exception ex ) {
              log.warn( "error reading field: "+fieldName );
            }
          }
          f.add("docs", terms.getDocCount());
        }
        if( topTerms != null ) {
          f.add( "distinct", topTerms.distinctTerms );

          // Include top terms
          f.add( "topTerms", topTerms.toNamedList( searcher.getSchema() ) );

          // Add a histogram
          f.add( "histogram", topTerms.histogram.toNamedList() );
        }
      }
      // Add the field
      finfo.add( fieldName, f );
    }
    return finfo;
  }

  /**
   * Return info from the index
   */
  private static SimpleOrderedMap<Object> getSchemaInfo( IndexSchema schema ) {
    Map<String, List<String>> typeusemap = new TreeMap<String, List<String>>();
    Map<String, Object> fields = new TreeMap<String, Object>();
    SchemaField uniqueField = schema.getUniqueKeyField();
    for( SchemaField f : schema.getFields().values() ) {
      populateFieldInfo(schema, typeusemap, fields, uniqueField, f);
    }

    Map<String, Object> dynamicFields = new TreeMap<String, Object>();
    for (SchemaField f : schema.getDynamicFieldPrototypes()) {
      populateFieldInfo(schema, typeusemap, dynamicFields, uniqueField, f);
    }
    SimpleOrderedMap<Object> types = new SimpleOrderedMap<Object>();
    Map<String, FieldType> sortedTypes = new TreeMap<String, FieldType>(schema.getFieldTypes());
    for( FieldType ft : sortedTypes.values() ) {
      SimpleOrderedMap<Object> field = new SimpleOrderedMap<Object>();
      field.add("fields", typeusemap.get( ft.getTypeName() ) );
      field.add("tokenized", ft.isTokenized() );
      field.add("className", ft.getClass().getName());
      field.add("indexAnalyzer", getAnalyzerInfo(ft.getAnalyzer()));
      field.add("queryAnalyzer", getAnalyzerInfo(ft.getQueryAnalyzer()));
      types.add( ft.getTypeName(), field );
    }

    // Must go through this to maintain binary compatbility. Putting a TreeMap into a resp leads to casting errors
    SimpleOrderedMap<Object> finfo = new SimpleOrderedMap<Object>();

    SimpleOrderedMap<Object> fieldsSimple = new SimpleOrderedMap<Object>();
    for (Map.Entry<String, Object> ent : fields.entrySet()) {
      fieldsSimple.add(ent.getKey(), ent.getValue());
    }
    finfo.add("fields", fieldsSimple);

    SimpleOrderedMap<Object> dynamicSimple = new SimpleOrderedMap<Object>();
    for (Map.Entry<String, Object> ent : dynamicFields.entrySet()) {
      dynamicSimple.add(ent.getKey(), ent.getValue());
    }
    finfo.add("dynamicFields", dynamicSimple);

    finfo.add("uniqueKeyField",
        null == uniqueField ? null : uniqueField.getName());
    finfo.add("defaultSearchField", schema.getDefaultSearchFieldName());
    finfo.add("types", types);
    return finfo;
  }


  private static SimpleOrderedMap<Object> getAnalyzerInfo(Analyzer analyzer) {
    SimpleOrderedMap<Object> aninfo = new SimpleOrderedMap<Object>();
    aninfo.add("className", analyzer.getClass().getName());
    if (analyzer instanceof TokenizerChain) {

      TokenizerChain tchain = (TokenizerChain)analyzer;

      CharFilterFactory[] cfiltfacs = tchain.getCharFilterFactories();
      SimpleOrderedMap<Map<String, Object>> cfilters = new SimpleOrderedMap<Map<String, Object>>();
      for (CharFilterFactory cfiltfac : cfiltfacs) {
        Map<String, Object> tok = new HashMap<String, Object>();
        String className = cfiltfac.getClass().getName();
        tok.put("className", className);
        tok.put("args", cfiltfac.getArgs());
        cfilters.add(className.substring(className.lastIndexOf('.')+1), tok);
      }
      if (cfilters.size() > 0) {
        aninfo.add("charFilters", cfilters);
      }

      SimpleOrderedMap<Object> tokenizer = new SimpleOrderedMap<Object>();
      TokenizerFactory tfac = tchain.getTokenizerFactory();
      tokenizer.add("className", tfac.getClass().getName());
      tokenizer.add("args", tfac.getArgs());
      aninfo.add("tokenizer", tokenizer);

      TokenFilterFactory[] filtfacs = tchain.getTokenFilterFactories();
      SimpleOrderedMap<Map<String, Object>> filters = new SimpleOrderedMap<Map<String, Object>>();
      for (TokenFilterFactory filtfac : filtfacs) {
        Map<String, Object> tok = new HashMap<String, Object>();
        String className = filtfac.getClass().getName();
        tok.put("className", className);
        tok.put("args", filtfac.getArgs());
        filters.add(className.substring(className.lastIndexOf('.')+1), tok);
      }
      if (filters.size() > 0) {
        aninfo.add("filters", filters);
      }
    }
    return aninfo;
  }

  private static void populateFieldInfo(IndexSchema schema,
                                        Map<String, List<String>> typeusemap, Map<String, Object> fields,
                                        SchemaField uniqueField, SchemaField f) {
    FieldType ft = f.getType();
    SimpleOrderedMap<Object> field = new SimpleOrderedMap<Object>();
    field.add( "type", ft.getTypeName() );
    field.add( "flags", getFieldFlags(f) );
    if( f.isRequired() ) {
      field.add( "required", f.isRequired() );
    }
    if( f.getDefaultValue() != null ) {
      field.add( "default", f.getDefaultValue() );
    }
    if (f == uniqueField){
      field.add("uniqueKey", true);
    }
    if (ft.getAnalyzer().getPositionIncrementGap(f.getName()) != 0) {
      field.add("positionIncrementGap", ft.getAnalyzer().getPositionIncrementGap(f.getName()));
    }
    field.add("copyDests", schema.getCopyFieldsList(f.getName()));
    field.add("copySources", schema.getCopySources(f.getName()));


    fields.put( f.getName(), field );

    List<String> v = typeusemap.get( ft.getTypeName() );
    if( v == null ) {
      v = new ArrayList<String>();
    }
    v.add( f.getName() );
    typeusemap.put( ft.getTypeName(), v );
  }
  public static SimpleOrderedMap<Object> getIndexInfo(IndexReader reader, boolean countTerms) throws IOException {
    return getIndexInfo(reader, countTerms ? 1 : 0, null, null);
  }
  public static SimpleOrderedMap<Object> getIndexInfo( IndexReader reader, int numTerms,
                                                       Map<String, TopTermQueue> topTerms,
                                                       Set<String> fieldList) throws IOException {
    Directory dir = reader.directory();
    SimpleOrderedMap<Object> indexInfo = new SimpleOrderedMap<Object>();

    indexInfo.add("numDocs", reader.numDocs());
    indexInfo.add("maxDoc", reader.maxDoc());
    final CharsRef spare = new CharsRef();
    if( numTerms > 0 ) {
      Fields fields = MultiFields.getFields(reader);
      long totalTerms = 0;
      if (fields != null) {
        FieldsEnum fieldsEnum = fields.iterator();
        String field;
        while ((field = fieldsEnum.next()) != null) {
          Terms terms = fieldsEnum.terms();
          if (terms == null) {
            continue;
          }
          totalTerms += terms.getUniqueTermCount();

          if (fieldList != null && !fieldList.contains(field)) {
            continue;
          }

          TermsEnum termsEnum = terms.iterator(null);
          BytesRef text;
          int[] buckets = new int[HIST_ARRAY_SIZE];
          TopTermQueue tiq = topTerms.get(field);
          if (tiq == null) {
            tiq = new TopTermQueue(numTerms + 1);   // Allocating slots for the top N terms to collect freqs.
            topTerms.put(field, tiq);
          }
          while ((text = termsEnum.next()) != null) {
            int freq = termsEnum.docFreq();  // This calculation seems odd, but it gives the same results as it used to.
            int slot = 32 - Integer.numberOfLeadingZeros(Math.max(0, freq - 1));
            buckets[slot] = buckets[slot] + 1;
            if (freq > tiq.minFreq) {
              UnicodeUtil.UTF8toUTF16(text, spare);
              String t = spare.toString();
              tiq.distinctTerms = new Long(fieldsEnum.terms().getUniqueTermCount()).intValue();

              tiq.add(new TopTermQueue.TermInfo(new Term(field, t), termsEnum.docFreq()));
              if (tiq.size() > numTerms) { // if tiq full
                tiq.pop(); // remove lowest in tiq
                tiq.minFreq  = tiq.getTopTermInfo().docFreq;
              }
            }
          }
          tiq.histogram.add(buckets);
        }
      }
      //Clumsy, but I'm tired.
      indexInfo.add("numTerms", (new Long(totalTerms)).intValue());

    }
    indexInfo.add("version", reader.getVersion());  // TODO? Is this different then: IndexReader.getCurrentVersion( dir )?
    indexInfo.add("segmentCount", reader.getSequentialSubReaders().length);
    indexInfo.add("current", reader.isCurrent() );
    indexInfo.add("hasDeletions", reader.hasDeletions() );
    indexInfo.add("directory", dir );
    indexInfo.add("lastModified", new Date(IndexReader.lastModified(dir)) );
    return indexInfo;
  }
  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Lucene Index Browser.  Inspired and modeled after Luke: http://www.getopt.org/luke/";
  }

  @Override
  public String getVersion() {
    return "$Revision$";
  }

  @Override
  public String getSourceId() {
    return "$Id$";
  }

  @Override
  public String getSource() {
    return "$URL$";
  }

  @Override
  public URL[] getDocs() {
    try {
      return new URL[] { new URL("http://wiki.apache.org/solr/LukeRequestHandler") };
    }
    catch( MalformedURLException ex ) { return null; }
  }

  ///////////////////////////////////////////////////////////////////////////////////////

  static class TermHistogram
  {
    int _maxBucket = -1;
    int _buckets[] = new int[HIST_ARRAY_SIZE];
    public void add(int[] buckets) {
      for (int idx = 0; idx < buckets.length; ++idx) {
        if (buckets[idx] != 0) _maxBucket = idx;
      }
      for (int idx = 0; idx <= _maxBucket; ++idx) {
        _buckets[idx] = buckets[idx];
      }
    }
    // TODO? should this be a list or a map?
    public NamedList<Integer> toNamedList()
    {
      NamedList<Integer> nl = new NamedList<Integer>();
      for( int bucket = 0; bucket <= _maxBucket; bucket++ ) {
        nl.add( ""+ (1 << bucket), _buckets[bucket] );
      }
      return nl;
    }
  }
  /**
   * Private internal class that counts up frequent terms
   */
  private static class TopTermQueue extends PriorityQueue
  {
    static class TermInfo {
      TermInfo(Term t, int df) {
        term = t;
        docFreq = df;
      }
      int docFreq;
      Term term;
    }

    public int minFreq = 0;
    public int distinctTerms = 0;
    public TermHistogram histogram;


    TopTermQueue(int size) {
      super(size);
      histogram = new TermHistogram();
    }

    @Override
    protected final boolean lessThan(Object a, Object b) {
      TermInfo termInfoA = (TermInfo)a;
      TermInfo termInfoB = (TermInfo)b;
      return termInfoA.docFreq < termInfoB.docFreq;
    }

    /**
     * This is a destructive call... the queue is empty at the end
     */
    public NamedList<Integer> toNamedList( IndexSchema schema )
    {
      // reverse the list..
      List<TermInfo> aslist = new LinkedList<TermInfo>();
      while( size() > 0 ) {
        aslist.add( 0, (TermInfo)pop() );
      }

      NamedList<Integer> list = new NamedList<Integer>();
      for (TermInfo i : aslist) {
        String txt = i.term.text();
        SchemaField ft = schema.getFieldOrNull( i.term.field() );
        if( ft != null ) {
          txt = ft.getType().indexedToReadable( txt );
        }
        list.add( txt, i.docFreq );
      }
      return list;
    }
    public TermInfo getTopTermInfo() {
      return (TermInfo)top();
    }
  }
}
