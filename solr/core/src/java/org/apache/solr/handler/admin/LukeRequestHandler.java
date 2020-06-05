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
package org.apache.solr.handler.admin;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.PriorityQueue;
import org.apache.solr.analysis.TokenizerChain;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.luke.FieldFlag;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Base64;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.CopyField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.SolrIndexWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.lucene.index.IndexOptions.DOCS;
import static org.apache.lucene.index.IndexOptions.DOCS_AND_FREQS;
import static org.apache.lucene.index.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;

/**
 * This handler exposes the internal lucene index.  It is inspired by and 
 * modeled on Luke, the Lucene Index Browser by Andrzej Bialecki.
 *   http://www.getopt.org/luke/
 *
 * For more documentation see:
 *  http://wiki.apache.org/solr/LukeRequestHandler
 *
 * @since solr 1.2
 */
public class LukeRequestHandler extends RequestHandlerBase
{
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String NUMTERMS = "numTerms";
  public static final String INCLUDE_INDEX_FIELD_FLAGS = "includeIndexFieldFlags";
  public static final String DOC_ID = "docId";
  public static final String ID = CommonParams.ID;
  public static final int DEFAULT_COUNT = 10;

  static final int HIST_ARRAY_SIZE = 33;

  private static enum ShowStyle {
    ALL,
    DOC,
    SCHEMA,
    INDEX;

    public static ShowStyle get(String v) {
      if(v==null) return null;
      if("schema".equalsIgnoreCase(v)) return SCHEMA;
      if("index".equalsIgnoreCase(v))  return INDEX;
      if("doc".equalsIgnoreCase(v))    return DOC;
      if("all".equalsIgnoreCase(v))    return ALL;
      throw new SolrException(ErrorCode.BAD_REQUEST, "Unknown Show Style: "+v);
    }
  };


  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception
  {
    IndexSchema schema = req.getSchema();
    SolrIndexSearcher searcher = req.getSearcher();
    DirectoryReader reader = searcher.getIndexReader();
    SolrParams params = req.getParams();
    ShowStyle style = ShowStyle.get(params.get("show"));

    // If no doc is given, show all fields and top terms

    rsp.add("index", getIndexInfo(reader));

    if(ShowStyle.INDEX==style) {
      return; // that's all we need
    }


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
      if( style != null && style != ShowStyle.DOC ) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "missing doc param for doc style");
      }
      Document doc = null;
      try {
        doc = reader.document( docId );
      }
      catch( Exception ex ) {}
      if( doc == null ) {
        throw new SolrException( SolrException.ErrorCode.NOT_FOUND, "Can't find document: "+docId );
      }

      SimpleOrderedMap<Object> info = getDocumentFieldsInfo( doc, docId, reader, schema );

      SimpleOrderedMap<Object> docinfo = new SimpleOrderedMap<>();
      docinfo.add( "docId", docId );
      docinfo.add( "lucene", info );
      docinfo.add( "solr", doc );
      rsp.add( "doc", docinfo );
    }
    else if ( ShowStyle.SCHEMA == style ) {
      rsp.add( "schema", getSchemaInfo( req.getSchema() ) );
    }
    else {
      rsp.add( "fields", getIndexedFieldsInfo(req) ) ;
    }

    // Add some generally helpful information
    NamedList<Object> info = new SimpleOrderedMap<>();
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

    flags.append( (f != null && f.fieldType().indexOptions() != IndexOptions.NONE)                     ? FieldFlag.INDEXED.getAbbreviation() : '-' );
    flags.append( (f != null && f.fieldType().tokenized())                   ? FieldFlag.TOKENIZED.getAbbreviation() : '-' );
    flags.append( (f != null && f.fieldType().stored())                      ? FieldFlag.STORED.getAbbreviation() : '-' );
    flags.append( (f != null && f.fieldType().docValuesType() != DocValuesType.NONE)        ? FieldFlag.DOC_VALUES.getAbbreviation() : "-" );
    flags.append( (false)                                          ? FieldFlag.UNINVERTIBLE.getAbbreviation() : '-' ); // SchemaField Specific
    flags.append( (false)                                          ? FieldFlag.MULTI_VALUED.getAbbreviation() : '-' ); // SchemaField Specific
    flags.append( (f != null && f.fieldType().storeTermVectors())            ? FieldFlag.TERM_VECTOR_STORED.getAbbreviation() : '-' );
    flags.append( (f != null && f.fieldType().storeTermVectorOffsets())   ? FieldFlag.TERM_VECTOR_OFFSET.getAbbreviation() : '-' );
    flags.append( (f != null && f.fieldType().storeTermVectorPositions()) ? FieldFlag.TERM_VECTOR_POSITION.getAbbreviation() : '-' );
    flags.append( (f != null && f.fieldType().storeTermVectorPayloads())   ? FieldFlag.TERM_VECTOR_PAYLOADS.getAbbreviation() : '-' );
    flags.append( (f != null && f.fieldType().omitNorms())                  ? FieldFlag.OMIT_NORMS.getAbbreviation() : '-' );

    flags.append( (f != null && DOCS == opts ) ?
        FieldFlag.OMIT_TF.getAbbreviation() : '-' );

    flags.append((f != null && DOCS_AND_FREQS == opts) ?
        FieldFlag.OMIT_POSITIONS.getAbbreviation() : '-');
    
    flags.append((f != null && DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS == opts) ?
        FieldFlag.STORE_OFFSETS_WITH_POSITIONS.getAbbreviation() : '-');

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
    flags.append( (f != null && f.hasDocValues())        ? FieldFlag.DOC_VALUES.getAbbreviation() : "-" );
    flags.append( (f != null && f.isUninvertible())      ? FieldFlag.UNINVERTIBLE.getAbbreviation() : "-" );
    flags.append( (f != null && f.multiValued())         ? FieldFlag.MULTI_VALUED.getAbbreviation() : '-' );
    flags.append( (f != null && f.storeTermVector() )    ? FieldFlag.TERM_VECTOR_STORED.getAbbreviation() : '-' );
    flags.append( (f != null && f.storeTermOffsets() )   ? FieldFlag.TERM_VECTOR_OFFSET.getAbbreviation() : '-' );
    flags.append( (f != null && f.storeTermPositions() ) ? FieldFlag.TERM_VECTOR_POSITION.getAbbreviation() : '-' );
    flags.append( (f != null && f.storeTermPayloads() )  ? FieldFlag.TERM_VECTOR_PAYLOADS.getAbbreviation() : '-' );
    flags.append( (f != null && f.omitNorms())           ? FieldFlag.OMIT_NORMS.getAbbreviation() : '-' );
    flags.append( (f != null &&
        f.omitTermFreqAndPositions() )        ? FieldFlag.OMIT_TF.getAbbreviation() : '-' );
    flags.append( (f != null && f.omitPositions() )      ? FieldFlag.OMIT_POSITIONS.getAbbreviation() : '-' );
    flags.append( (f != null && f.storeOffsetsWithPositions() )      ? FieldFlag.STORE_OFFSETS_WITH_POSITIONS.getAbbreviation() : '-' );
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
    SimpleOrderedMap<String> key = new SimpleOrderedMap<>();
    for (FieldFlag f : FieldFlag.values()) {
      key.add(String.valueOf(f.getAbbreviation()), f.getDisplay() );
    }
    return key;
  }

  private static SimpleOrderedMap<Object> getDocumentFieldsInfo( Document doc, int docId, IndexReader reader,
                                                                 IndexSchema schema ) throws IOException
  {
    final CharsRefBuilder spare = new CharsRefBuilder();
    SimpleOrderedMap<Object> finfo = new SimpleOrderedMap<>();
    for( Object o : doc.getFields() ) {
      Field field = (Field)o;
      SimpleOrderedMap<Object> f = new SimpleOrderedMap<>();

      SchemaField sfield = schema.getFieldOrNull( field.name() );
      FieldType ftype = (sfield==null)?null:sfield.getType();

      f.add( "type", (ftype==null)?null:ftype.getTypeName() );
      f.add( "schema", getFieldFlags( sfield ) );
      f.add( "flags", getFieldFlags( field ) );

      f.add( "value", (ftype==null)?null:ftype.toExternal( field ) );

      // TODO: this really should be "stored"
      f.add( "internal", field.stringValue() );  // may be a binary number

      BytesRef bytes = field.binaryValue();
      if (bytes != null) {
        f.add( "binary", Base64.byteArrayToBase64(bytes.bytes, bytes.offset, bytes.length));
      }
      if (!ftype.isPointField()) {
        Term t = new Term(field.name(), ftype!=null ? ftype.storedToIndexed(field) : field.stringValue());
        f.add( "docFreq", t.text()==null ? 0 : reader.docFreq( t ) ); // this can be 0 for non-indexed fields
      }// TODO: Calculate docFreq for point fields

      // If we have a term vector, return that
      if( field.fieldType().storeTermVectors() ) {
        try {
          Terms v = reader.getTermVector( docId, field.name() );
          if( v != null ) {
            SimpleOrderedMap<Integer> tfv = new SimpleOrderedMap<>();
            final TermsEnum termsEnum = v.iterator();
            BytesRef text;
            while((text = termsEnum.next()) != null) {
              final int freq = (int) termsEnum.totalTermFreq();
              spare.copyUTF8Bytes(text);
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

  private static SimpleOrderedMap<Object> getIndexedFieldsInfo(SolrQueryRequest req)
      throws Exception {

    SolrIndexSearcher searcher = req.getSearcher();
    SolrParams params = req.getParams();

    Set<String> fields = null;
    String fl = params.get(CommonParams.FL);
    if (fl != null) {
      fields = new TreeSet<>(Arrays.asList(fl.split( "[,\\s]+" )));
    }

    LeafReader reader = searcher.getSlowAtomicReader();
    IndexSchema schema = searcher.getSchema();

    // Don't be tempted to put this in the loop below, the whole point here is to alphabetize the fields!
    Set<String> fieldNames = new TreeSet<>();
    for(FieldInfo fieldInfo : reader.getFieldInfos()) {
      fieldNames.add(fieldInfo.name);
    }

    // Walk the term enum and keep a priority queue for each map in our set
    SimpleOrderedMap<Object> finfo = new SimpleOrderedMap<>();

    for (String fieldName : fieldNames) {
      if (fields != null && ! fields.contains(fieldName) && ! fields.contains("*")) {
        continue; //we're not interested in this field Still an issue here
      }

      SimpleOrderedMap<Object> fieldMap = new SimpleOrderedMap<>();

      SchemaField sfield = schema.getFieldOrNull( fieldName );
      FieldType ftype = (sfield==null)?null:sfield.getType();

      fieldMap.add( "type", (ftype==null)?null:ftype.getTypeName() );
      fieldMap.add("schema", getFieldFlags(sfield));
      if (sfield != null && schema.isDynamicField(sfield.getName()) && schema.getDynamicPattern(sfield.getName()) != null) {
        fieldMap.add("dynamicBase", schema.getDynamicPattern(sfield.getName()));
      }
      Terms terms = reader.terms(fieldName);
      if (terms == null) { // Not indexed, so we need to report what we can (it made it through the fl param if specified)
        finfo.add( fieldName, fieldMap );
        continue;
      }

      if(sfield != null && sfield.indexed() ) {
        if (params.getBool(INCLUDE_INDEX_FIELD_FLAGS,true)) {
          Document doc = getFirstLiveDoc(terms, reader);

          if (doc != null) {
            // Found a document with this field
            try {
              IndexableField fld = doc.getField(fieldName);
              if (fld != null) {
                fieldMap.add("index", getFieldFlags(fld));
              } else {
                // it is a non-stored field...
                fieldMap.add("index", "(unstored field)");
              }
            } catch (Exception ex) {
              log.warn("error reading field: {}", fieldName);
            }
          }
        }
        fieldMap.add("docs", terms.getDocCount());
      }
      if (fields != null && (fields.contains(fieldName) || fields.contains("*"))) {
        getDetailedFieldInfo(req, fieldName, fieldMap);
      }
      // Add the field
      finfo.add( fieldName, fieldMap );
    }
    return finfo;
  }

  // Just get a document with the term in it, the first one will do!
  // Is there a better way to do this? Shouldn't actually be very costly
  // to do it this way.
  private static Document getFirstLiveDoc(Terms terms, LeafReader reader) throws IOException {
    PostingsEnum postingsEnum = null;
    TermsEnum termsEnum = terms.iterator();
    BytesRef text;
    // Deal with the chance that the first bunch of terms are in deleted documents. Is there a better way?
    for (int idx = 0; idx < 1000 && postingsEnum == null; ++idx) {
      text = termsEnum.next();
      if (text == null) { // Ran off the end of the terms enum without finding any live docs with that field in them.
        return null;
      }
      postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.NONE);
      final Bits liveDocs = reader.getLiveDocs();
      if (postingsEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        if (liveDocs != null && liveDocs.get(postingsEnum.docID())) {
          continue;
        }
        return reader.document(postingsEnum.docID());
      }
    }
    return null;
  }

  /**
   * Return info from the index
   */
  private static SimpleOrderedMap<Object> getSchemaInfo( IndexSchema schema ) {
    Map<String, List<String>> typeusemap = new TreeMap<>();
    Map<String, Object> fields = new TreeMap<>();
    SchemaField uniqueField = schema.getUniqueKeyField();
    for( SchemaField f : schema.getFields().values() ) {
      populateFieldInfo(schema, typeusemap, fields, uniqueField, f);
    }

    Map<String, Object> dynamicFields = new TreeMap<>();
    for (SchemaField f : schema.getDynamicFieldPrototypes()) {
      populateFieldInfo(schema, typeusemap, dynamicFields, uniqueField, f);
    }
    SimpleOrderedMap<Object> types = new SimpleOrderedMap<>();
    Map<String, FieldType> sortedTypes = new TreeMap<>(schema.getFieldTypes());
    for( FieldType ft : sortedTypes.values() ) {
      SimpleOrderedMap<Object> field = new SimpleOrderedMap<>();
      field.add("fields", typeusemap.get( ft.getTypeName() ) );
      field.add("tokenized", ft.isTokenized() );
      field.add("className", ft.getClass().getName());
      field.add("indexAnalyzer", getAnalyzerInfo(ft.getIndexAnalyzer()));
      field.add("queryAnalyzer", getAnalyzerInfo(ft.getQueryAnalyzer()));
      field.add("similarity", getSimilarityInfo(ft.getSimilarity()));
      types.add( ft.getTypeName(), field );
    }

    // Must go through this to maintain binary compatbility. Putting a TreeMap into a resp leads to casting errors
    SimpleOrderedMap<Object> finfo = new SimpleOrderedMap<>();

    SimpleOrderedMap<Object> fieldsSimple = new SimpleOrderedMap<>();
    for (Map.Entry<String, Object> ent : fields.entrySet()) {
      fieldsSimple.add(ent.getKey(), ent.getValue());
    }
    finfo.add("fields", fieldsSimple);

    SimpleOrderedMap<Object> dynamicSimple = new SimpleOrderedMap<>();
    for (Map.Entry<String, Object> ent : dynamicFields.entrySet()) {
      dynamicSimple.add(ent.getKey(), ent.getValue());
    }
    finfo.add("dynamicFields", dynamicSimple);

    finfo.add("uniqueKeyField",
        null == uniqueField ? null : uniqueField.getName());
    finfo.add("similarity", getSimilarityInfo(schema.getSimilarity()));
    finfo.add("types", types);
    return finfo;
  }

  private static SimpleOrderedMap<Object> getSimilarityInfo(Similarity similarity) {
    SimpleOrderedMap<Object> toReturn = new SimpleOrderedMap<>();
    if (similarity != null) {
      toReturn.add("className", similarity.getClass().getName());
      toReturn.add("details", similarity.toString());
    }
    return toReturn;
  }

  private static SimpleOrderedMap<Object> getAnalyzerInfo(Analyzer analyzer) {
    SimpleOrderedMap<Object> aninfo = new SimpleOrderedMap<>();
    aninfo.add("className", analyzer.getClass().getName());
    if (analyzer instanceof TokenizerChain) {

      TokenizerChain tchain = (TokenizerChain)analyzer;

      CharFilterFactory[] cfiltfacs = tchain.getCharFilterFactories();
      if (0 < cfiltfacs.length) {
        SimpleOrderedMap<Map<String, Object>> cfilters = new SimpleOrderedMap<>();
        for (CharFilterFactory cfiltfac : cfiltfacs) {
          Map<String, Object> tok = new HashMap<>();
          String className = cfiltfac.getClass().getName();
          tok.put("className", className);
          tok.put("args", cfiltfac.getOriginalArgs());
          cfilters.add(className.substring(className.lastIndexOf('.')+1), tok);
        }
        aninfo.add("charFilters", cfilters);
      }

      SimpleOrderedMap<Object> tokenizer = new SimpleOrderedMap<>();
      TokenizerFactory tfac = tchain.getTokenizerFactory();
      tokenizer.add("className", tfac.getClass().getName());
      tokenizer.add("args", tfac.getOriginalArgs());
      aninfo.add("tokenizer", tokenizer);

      TokenFilterFactory[] filtfacs = tchain.getTokenFilterFactories();
      if (0 < filtfacs.length) {
        SimpleOrderedMap<Map<String, Object>> filters = new SimpleOrderedMap<>();
        for (TokenFilterFactory filtfac : filtfacs) {
          Map<String, Object> tok = new HashMap<>();
          String className = filtfac.getClass().getName();
          tok.put("className", className);
          tok.put("args", filtfac.getOriginalArgs());
          filters.add(className.substring(className.lastIndexOf('.')+1), tok);
        }
        aninfo.add("filters", filters);
      }
    }
    return aninfo;
  }

  private static void populateFieldInfo(IndexSchema schema,
                                        Map<String, List<String>> typeusemap, Map<String, Object> fields,
                                        SchemaField uniqueField, SchemaField f) {
    FieldType ft = f.getType();
    SimpleOrderedMap<Object> field = new SimpleOrderedMap<>();
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
    if (ft.getIndexAnalyzer().getPositionIncrementGap(f.getName()) != 0) {
      field.add("positionIncrementGap", ft.getIndexAnalyzer().getPositionIncrementGap(f.getName()));
    }
    field.add("copyDests", toListOfStringDests(schema.getCopyFieldsList(f.getName())));
    field.add("copySources", schema.getCopySources(f.getName()));


    fields.put( f.getName(), field );

    List<String> v = typeusemap.get( ft.getTypeName() );
    if( v == null ) {
      v = new ArrayList<>();
    }
    v.add( f.getName() );
    typeusemap.put( ft.getTypeName(), v );
  }

  // This method just gets the top-most level of information. This was conflated with getting detailed info
  // for *all* the fields, called from CoreAdminHandler etc.

  public static SimpleOrderedMap<Object> getIndexInfo(DirectoryReader reader) throws IOException {
    Directory dir = reader.directory();
    SimpleOrderedMap<Object> indexInfo = new SimpleOrderedMap<>();

    indexInfo.add("numDocs", reader.numDocs());
    indexInfo.add("maxDoc", reader.maxDoc());
    indexInfo.add("deletedDocs", reader.maxDoc() - reader.numDocs());
    indexInfo.add("indexHeapUsageBytes", getIndexHeapUsed(reader));

    indexInfo.add("version", reader.getVersion());  // TODO? Is this different then: IndexReader.getCurrentVersion( dir )?
    indexInfo.add("segmentCount", reader.leaves().size());
    indexInfo.add("current", closeSafe( reader::isCurrent));
    indexInfo.add("hasDeletions", reader.hasDeletions() );
    indexInfo.add("directory", dir );
    IndexCommit indexCommit = reader.getIndexCommit();
    String segmentsFileName = indexCommit.getSegmentsFileName();
    indexInfo.add("segmentsFile", segmentsFileName);
    indexInfo.add("segmentsFileSizeInBytes", getSegmentsFileLength(indexCommit));
    Map<String,String> userData = indexCommit.getUserData();
    indexInfo.add("userData", userData);
    String s = userData.get(SolrIndexWriter.COMMIT_TIME_MSEC_KEY);
    if (s != null) {
      indexInfo.add("lastModified", new Date(Long.parseLong(s)));
    }
    return indexInfo;
  }

  @FunctionalInterface
  interface IOSupplier {
    boolean get() throws IOException;
  }
  
  private static Object closeSafe(IOSupplier isCurrent) {
    try {
      return isCurrent.get();
    }catch(AlreadyClosedException | IOException exception) {
    }
    return false;
  }


  /**
   * <p>A helper method that attempts to determine the file length of the the segments file for the 
   * specified IndexCommit from it's Directory.
   * </p>
   * <p>
   * If any sort of {@link IOException} occurs, this method will return "-1" and swallow the exception since 
   * this may be normal if the IndexCommit is no longer "on disk".  The specific type of the Exception will 
   * affect how severely it is logged: {@link NoSuchFileException} is considered more "acceptible" then other 
   * types of IOException which may indicate an actual problem with the Directory.
   */
  private static long getSegmentsFileLength(IndexCommit commit) {
    try {
      return commit.getDirectory().fileLength(commit.getSegmentsFileName());
    } catch (NoSuchFileException okException) {
      log.debug("Unable to determine the (optional) fileSize for the current IndexReader's segments file because it is "
          + "no longer in the Directory, this can happen if there are new commits since the Reader was opened"
          , okException);
    } catch (IOException strangeException) {
      log.warn("Ignoring IOException wile attempting to determine the (optional) fileSize stat for the current IndexReader's segments file",
               strangeException);
    }
    return -1;
  }

  /** Returns the sum of RAM bytes used by each segment */
  private static long getIndexHeapUsed(DirectoryReader reader) {
    return reader.leaves().stream()
        .map(LeafReaderContext::reader)
        .map(FilterLeafReader::unwrap)
        .map(leafReader -> {
          if (leafReader instanceof Accountable) {
            return ((Accountable) leafReader).ramBytesUsed();
          } else {
            return -1L; // unsupported
          }
        })
        .mapToLong(Long::longValue)
        .reduce(0, (left, right) -> left == -1 || right == -1 ? -1 : left + right);
    // if any leaves are unsupported (-1), we ultimately return -1.
  }

  // Get terribly detailed information about a particular field. This is a very expensive call, use it with caution
  // especially on large indexes!
  @SuppressWarnings("unchecked")
  private static void getDetailedFieldInfo(SolrQueryRequest req, String field, SimpleOrderedMap<Object> fieldMap)
      throws IOException {

    SolrParams params = req.getParams();
    final int numTerms = params.getInt( NUMTERMS, DEFAULT_COUNT );

    TopTermQueue tiq = new TopTermQueue(numTerms + 1);  // Something to collect the top N terms in.

    final CharsRefBuilder spare = new CharsRefBuilder();

    Terms terms = MultiTerms.getTerms(req.getSearcher().getIndexReader(), field);
    if (terms == null) {  // field does not exist
      return;
    }
    TermsEnum termsEnum = terms.iterator();
    BytesRef text;
    int[] buckets = new int[HIST_ARRAY_SIZE];
    while ((text = termsEnum.next()) != null) {
      ++tiq.distinctTerms;
      int freq = termsEnum.docFreq();  // This calculation seems odd, but it gives the same results as it used to.
      int slot = 32 - Integer.numberOfLeadingZeros(Math.max(0, freq - 1));
      buckets[slot] = buckets[slot] + 1;
      if (numTerms > 0 && freq > tiq.minFreq) {
        spare.copyUTF8Bytes(text);
        String t = spare.toString();

        tiq.add(new TopTermQueue.TermInfo(new Term(field, t), termsEnum.docFreq()));
        if (tiq.size() > numTerms) { // if tiq full
          tiq.pop(); // remove lowest in tiq
          tiq.minFreq = tiq.getTopTermInfo().docFreq;
        }
      }
    }
    tiq.histogram.add(buckets);
    fieldMap.add("distinct", tiq.distinctTerms);

    // Include top terms
    fieldMap.add("topTerms", tiq.toNamedList(req.getSearcher().getSchema()));

    // Add a histogram
    fieldMap.add("histogram", tiq.histogram.toNamedList());
  }

  private static List<String> toListOfStrings(SchemaField[] raw) {
    List<String> result = new ArrayList<>(raw.length);
    for (SchemaField f : raw) {
      result.add(f.getName());
    }
    return result;
  }
  private static List<String> toListOfStringDests(List<CopyField> raw) {
    List<String> result = new ArrayList<>(raw.size());
    for (CopyField f : raw) {
      result.add(f.getDestination().getName());
    }
    return result;
  }

  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Lucene Index Browser.  Inspired and modeled after Luke: http://www.getopt.org/luke/";
  }

  @Override
  public Category getCategory() {
    return Category.ADMIN;
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
      NamedList<Integer> nl = new NamedList<>();
      for( int bucket = 0; bucket <= _maxBucket; bucket++ ) {
        nl.add( ""+ (1 << bucket), _buckets[bucket] );
      }
      return nl;
    }
  }
  /**
   * Private internal class that counts up frequent terms
   */
  @SuppressWarnings("rawtypes")
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
      List<TermInfo> aslist = new LinkedList<>();
      while( size() > 0 ) {
        aslist.add( 0, (TermInfo)pop() );
      }

      NamedList<Integer> list = new NamedList<>();
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
