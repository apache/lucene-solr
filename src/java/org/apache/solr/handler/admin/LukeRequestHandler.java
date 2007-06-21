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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.PriorityQueue;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.RequestHandlerUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocList;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SolrQueryParser;

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
 * @author ryan
 * @version $Id$
 * @since solr 1.2
 */
public class LukeRequestHandler extends RequestHandlerBase 
{
  private static Logger log = Logger.getLogger(LukeRequestHandler.class.getName());
  
  public static final String NUMTERMS = "numTerms";
  public static final String DOC_ID = "docId";
  public static final String ID = "id";
  public static final int DEFAULT_COUNT = 10;
  
  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception
  {
    RequestHandlerUtils.addExperimentalFormatWarning( rsp );
    
    IndexSchema schema = req.getSchema();
    SolrIndexSearcher searcher = req.getSearcher();
    IndexReader reader = searcher.getReader();
    SolrParams params = req.getParams();
    int numTerms = params.getInt( NUMTERMS, DEFAULT_COUNT );
        
    // Always show the core lucene info
    rsp.add("index", getIndexInfo(reader, numTerms>0 ) );

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
      // If no doc is given, show all fields and top terms
      Set<String> fields = null;
      if( params.get( SolrParams.FL ) != null ) {
        fields = new HashSet<String>();
        for( String f : params.getParams( SolrParams.FL ) ) {
          fields.add( f );
        }
      }
      rsp.add( "fields", getIndexedFieldsInfo( searcher, fields, numTerms ) ) ;
    }

    // Add some generally helpful information
    NamedList<Object> info = new SimpleOrderedMap<Object>();
    info.add( "key", getFieldFlagsKey() );
    info.add( "NOTE", "Document Frequency (df) is not updated when a document is marked for deletion.  df values include deleted documents." ); 
    rsp.add( "info", info );
  }
  
  /**
   * @return a string representing a Fieldable's flags.  
   */
  private static String getFieldFlags( Fieldable f )
  {
    StringBuilder flags = new StringBuilder();
    flags.append( (f != null && f.isIndexed())                     ? 'I' : '-' );
    flags.append( (f != null && f.isTokenized())                   ? 'T' : '-' );
    flags.append( (f != null && f.isStored())                      ? 'S' : '-' );
    flags.append( (false)                                          ? 'M' : '-' ); // SchemaField Specific
    flags.append( (f != null && f.isTermVectorStored())            ? 'V' : '-' );
    flags.append( (f != null && f.isStoreOffsetWithTermVector())   ? 'o' : '-' );
    flags.append( (f != null && f.isStorePositionWithTermVector()) ? 'p' : '-' );
    flags.append( (f != null && f.getOmitNorms())                  ? 'O' : '-' );
    flags.append( (f != null && f.isLazy())                        ? 'L' : '-' );
    flags.append( (f != null && f.isBinary())                      ? 'B' : '-' );
    flags.append( (f != null && f.isCompressed())                  ? 'C' : '-' );
    flags.append( (false)                                          ? 'f' : '-' ); // SchemaField Specific
    flags.append( (false)                                          ? 'l' : '-' ); // SchemaField Specific
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
    flags.append( (f != null && f.indexed())             ? 'I' : '-' );
    flags.append( (t != null && t.isTokenized())         ? 'T' : '-' );
    flags.append( (f != null && f.stored())              ? 'S' : '-' );
    flags.append( (f != null && f.multiValued())         ? 'M' : '-' );
    flags.append( (f != null && f.storeTermVector() )    ? 'V' : '-' );
    flags.append( (f != null && f.storeTermOffsets() )   ? 'o' : '-' );
    flags.append( (f != null && f.storeTermPositions() ) ? 'p' : '-' );
    flags.append( (f != null && f.omitNorms())           ? 'O' : '-' );
    flags.append( (lazy)                                 ? 'L' : '-' );
    flags.append( (binary)                               ? 'B' : '-' );
    flags.append( (f != null && f.isCompressed())        ? 'C' : '-' );
    flags.append( (f != null && f.sortMissingFirst() )   ? 'f' : '-' );
    flags.append( (f != null && f.sortMissingLast() )    ? 'l' : '-' );
    return flags.toString();
  }
  
  /**
   * @return a key to what each character means
   */
  private static SimpleOrderedMap<String> getFieldFlagsKey()
  {
    SimpleOrderedMap<String> key = new SimpleOrderedMap<String>();
    key.add( "I", "Indexed" );                     
    key.add( "T", "Tokenized" );                   
    key.add( "S", "Stored" );                   
    key.add( "M", "Multivalued" );                     
    key.add( "V", "TermVector Stored" );            
    key.add( "o", "Store Offset With TermVector" );   
    key.add( "p", "Store Position With TermVector" ); 
    key.add( "O", "Omit Norms" );                  
    key.add( "L", "Lazy" );                        
    key.add( "B", "Binary" );                      
    key.add( "C", "Compressed" );                  
    key.add( "f", "Sort Missing First" );                  
    key.add( "l", "Sort Missing Last" );                  
    return key;
  }
  
  private static SimpleOrderedMap<Object> getDocumentFieldsInfo( Document doc, int docId, IndexReader reader, IndexSchema schema ) throws IOException
  { 
    SimpleOrderedMap<Object> finfo = new SimpleOrderedMap<Object>();
    for( Object o : doc.getFields() ) {
      Fieldable fieldable = (Fieldable)o;
      SimpleOrderedMap<Object> f = new SimpleOrderedMap<Object>();
      
      SchemaField sfield = schema.getFieldOrNull( fieldable.name() );
      FieldType ftype = (sfield==null)?null:sfield.getType();

      f.add( "type", (ftype==null)?null:ftype.getTypeName() );
      f.add( "schema", getFieldFlags( sfield ) );
      f.add( "flags", getFieldFlags( fieldable ) );
      
      Term t = new Term( fieldable.name(), fieldable.stringValue() );
      f.add( "value", (ftype==null)?null:ftype.toExternal( fieldable ) );
      f.add( "internal", fieldable.stringValue() );  // may be a binary number
      f.add( "boost", fieldable.getBoost() );
      f.add( "docFreq", reader.docFreq( t ) ); // this can be 0 for non-indexed fields
            
      // If we have a term vector, return that
      if( fieldable.isTermVectorStored() ) {
        try {
          TermFreqVector v = reader.getTermFreqVector( docId, fieldable.name() );
          if( v != null ) {
            SimpleOrderedMap<Integer> tfv = new SimpleOrderedMap<Integer>();
            for( int i=0; i<v.size(); i++ ) {
              tfv.add( v.getTerms()[i], v.getTermFrequencies()[i] );
            }
            f.add( "termVector", tfv );
          }
        }
        catch( Exception ex ) {
          log.log( Level.WARNING, "error writing term vector", ex );
        }
      }
      
      finfo.add( fieldable.name(), f );
    }
    return finfo;
  }

  @SuppressWarnings("unchecked")
  private static SimpleOrderedMap<Object> getIndexedFieldsInfo( 
    final SolrIndexSearcher searcher, final Set<String> fields, final int numTerms ) 
    throws Exception
  { 
    Query matchAllDocs = new MatchAllDocsQuery();
    SolrQueryParser qp = searcher.getSchema().getSolrQueryParser(null);

    IndexReader reader = searcher.getReader();
    IndexSchema schema = searcher.getSchema();
    
    // Walk the term enum and keep a priority queue for each map in our set
    Map<String,TopTermQueue> ttinfo = null;
    if( numTerms > 0 ) {
      ttinfo = getTopTerms(reader, fields, numTerms, null );
    }
    SimpleOrderedMap<Object> finfo = new SimpleOrderedMap<Object>();
    Collection<String> fieldNames = reader.getFieldNames(IndexReader.FieldOption.ALL);
    for (String fieldName : fieldNames) {
      if( fields != null && !fields.contains( fieldName ) ) {
        continue; // if a field is specified, only them
      }
      
      SimpleOrderedMap<Object> f = new SimpleOrderedMap<Object>();
      
      SchemaField sfield = schema.getFieldOrNull( fieldName );
      FieldType ftype = (sfield==null)?null:sfield.getType();

      f.add( "type", (ftype==null)?null:ftype.getTypeName() );
      f.add( "schema", getFieldFlags( sfield ) );

      // If numTerms==0, the call is just asking for a quick field list
      if( ttinfo != null && sfield != null && sfield.indexed() ) {
        Query q = qp.parse( fieldName+":[* TO *]" ); 
        int docCount = searcher.numDocs( q, matchAllDocs );
        if( docCount > 0 ) {
          // Find a document with this field
          DocList ds = searcher.getDocList( q, (Query)null, (Sort)null, 0, 1 );
          try {
            Document doc = searcher.doc( ds.iterator().next() );
            Fieldable fld = doc.getFieldable( fieldName );
            if( fld != null ) {
              f.add( "index", getFieldFlags( fld ) );
            }
            else {
              // it is a non-stored field...
              f.add( "index", "(unstored field)" );
            }
          }
          catch( Exception ex ) {
            log.warning( "error reading field: "+fieldName );
          }
          // Find one document so we can get the fieldable
        }
        f.add( "docs", docCount );
        
        TopTermQueue topTerms = ttinfo.get( fieldName );
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
  private static SimpleOrderedMap<Object> getSchemaInfo( IndexSchema schema ) 
  { 
    Map<String, List<String>> typeusemap = new HashMap<String, List<String>>();
    SimpleOrderedMap<Object> fields = new SimpleOrderedMap<Object>();
    for( SchemaField f : schema.getFields().values() ) {
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
      fields.add( f.getName(), field );
      
      List<String> v = typeusemap.get( ft.getTypeName() );
      if( v == null ) {
        v = new ArrayList<String>();
      }
      v.add( f.getName() );
      typeusemap.put( ft.getTypeName(), v );
    }

    SimpleOrderedMap<Object> types = new SimpleOrderedMap<Object>();
    for( FieldType ft : schema.getFieldTypes().values() ) {
      SimpleOrderedMap<Object> field = new SimpleOrderedMap<Object>();
      field.add( "fields", typeusemap.get( ft.getTypeName() ) );
      field.add( "tokenized", ft.isTokenized() );
      field.add( "analyzer", ft.getAnalyzer()+"" );
      types.add( ft.getTypeName(), field );
    }

    SimpleOrderedMap<Object> finfo = new SimpleOrderedMap<Object>();
    finfo.add("fields", fields);
    finfo.add("types", types);
    return finfo;
  }
  
  private static SimpleOrderedMap<Object> getIndexInfo( IndexReader reader, boolean countTerms ) throws IOException
  {
    Directory dir = reader.directory();
    SimpleOrderedMap<Object> indexInfo = new SimpleOrderedMap<Object>();
    indexInfo.add("numDocs", reader.numDocs());
    indexInfo.add("maxDoc", reader.maxDoc());
    
    if( countTerms ) {
      TermEnum te = reader.terms();
      int numTerms = 0;
      while (te.next()) {
        numTerms++;
      }
      indexInfo.add("numTerms", numTerms );
    }

    indexInfo.add("version", reader.getVersion());  // TODO? Is this different then: IndexReader.getCurrentVersion( dir )?
    indexInfo.add("optimized", reader.isOptimized() );
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
  
  private static class TermHistogram 
  {
    int maxBucket = -1;
    public Map<Integer,Integer> hist = new HashMap<Integer, Integer>();
    
    private static final double LOG2 = Math.log( 2 );
    public static int getPowerOfTwoBucket( int num )
    {
      int exp = (int)Math.ceil( (Math.log( num ) / LOG2 ) );
      return (int) Math.pow( 2, exp );
    }
    
    public void add( int df )
    {
      Integer bucket = getPowerOfTwoBucket( df );
      if( bucket > maxBucket ) {
        maxBucket = bucket;
      }
      Integer old = hist.get( bucket );
      if( old == null ) {
        hist.put( bucket, 1 );
      }
      else {
        hist.put( bucket, old+1 );
      }
    }
    
    // TODO? should this be a list or a map?
    public NamedList<Integer> toNamedList()
    {
      NamedList<Integer> nl = new NamedList<Integer>();
      for( int bucket = 2; bucket <= maxBucket; bucket *= 2 ) {
        Integer val = hist.get( bucket );
        if( val == null ) {
          val = 0;
        }
        nl.add( ""+bucket, val );
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
      initialize(size);
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
  }

  private static Map<String,TopTermQueue> getTopTerms( IndexReader reader, Set<String> fields, int numTerms, Set<String> junkWords ) throws Exception 
  {
    Map<String,TopTermQueue> info = new HashMap<String, TopTermQueue>();
    TermEnum terms = reader.terms();
    
    while (terms.next()) {
      String field = terms.term().field();
      String t = terms.term().text();

      // Compute distinct terms for every field
      TopTermQueue tiq = info.get( field );
      if( tiq == null ) {
        tiq = new TopTermQueue( numTerms );
        info.put( field, tiq );
      }
      tiq.distinctTerms++;
      tiq.histogram.add( terms.docFreq() );  // add the term to the histogram
      
      // Only save the distinct terms for fields we worry about
      if (fields != null && fields.size() > 0) {
        if( !fields.contains( field ) ) {
          continue;
        }
      }
      if( junkWords != null && junkWords.contains( t ) ) {
        continue;
      }
      
      if( terms.docFreq() > tiq.minFreq ) {
        tiq.put(new TopTermQueue.TermInfo(terms.term(), terms.docFreq()));
        if (tiq.size() >= numTerms) { // if tiq full
          tiq.pop(); // remove lowest in tiq
          tiq.minFreq = ((TopTermQueue.TermInfo)tiq.top()).docFreq; // reset minFreq
        }
      }
    }
    return info;
  }
}







