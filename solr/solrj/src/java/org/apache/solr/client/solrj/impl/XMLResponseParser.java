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
package org.apache.solr.client.solrj.impl;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.InputStream;
import java.io.Reader;
import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.common.EmptyEntityResolver;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.XMLErrorLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 *
 * @since solr 1.3
 */
public class XMLResponseParser extends ResponseParser
{
  public static final String XML_CONTENT_TYPE = "application/xml; charset=UTF-8";
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final XMLErrorLogger xmllog = new XMLErrorLogger(log);

  // reuse the factory among all parser instances so things like string caches
  // won't be duplicated
  static final XMLInputFactory factory;
  static {
    factory = XMLInputFactory.newInstance();
    EmptyEntityResolver.configureXMLInputFactory(factory);

    try {
      // The java 1.6 bundled stax parser (sjsxp) does not currently have a thread-safe
      // XMLInputFactory, as that implementation tries to cache and reuse the
      // XMLStreamReader.  Setting the parser-specific "reuse-instance" property to false
      // prevents this.
      // All other known open-source stax parsers (and the bea ref impl)
      // have thread-safe factories.
      factory.setProperty("reuse-instance", Boolean.FALSE);
    }
    catch( IllegalArgumentException ex ) {
      // Other implementations will likely throw this exception since "reuse-instance"
      // isimplementation specific.
      log.debug( "Unable to set the 'reuse-instance' property for the input factory: {}", factory );
    }
    factory.setXMLReporter(xmllog);
  }

  public XMLResponseParser() {}

  @Override
  public String getWriterType()
  {
    return "xml";
  }

  @Override
  public String getContentType() {
    return XML_CONTENT_TYPE;
  }

  @Override
  public NamedList<Object> processResponse(Reader in) {
    XMLStreamReader parser = null;
    try {
      parser = factory.createXMLStreamReader(in);
    } catch (XMLStreamException e) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, "parsing error", e);
    }

    return processResponse(parser);
  }

  @Override
  public NamedList<Object> processResponse(InputStream in, String encoding)
  {
     XMLStreamReader parser = null;
    try {
      parser = factory.createXMLStreamReader(in, encoding);
    } catch (XMLStreamException e) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, "parsing error", e);
    }

    return processResponse(parser);
  }

  /**
   * parse the text into a named list...
   */
  private NamedList<Object> processResponse(XMLStreamReader parser)
  {
    try {
      NamedList<Object> response = null;
      for (int event = parser.next();
       event != XMLStreamConstants.END_DOCUMENT;
       event = parser.next())
      {
        switch (event) {
          case XMLStreamConstants.START_ELEMENT:

            if( response != null ) {
              throw new Exception( "already read the response!" );
            }

            // only top-level element is "response
            String name = parser.getLocalName();
            if( name.equals( "response" ) || name.equals( "result" ) ) {
              response = readNamedList( parser );
            }
            else if( name.equals( "solr" ) ) {
              return new SimpleOrderedMap<>();
            }
            else {
              throw new Exception( "really needs to be response or result.  " +
                  "not:"+parser.getLocalName() );
            }
            break;
        }
      }
      return response;
    }
    catch( Exception ex ) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, "parsing error", ex );
    }
    finally {
      try {
        parser.close();
      }
      catch( Exception ex ){}
    }
  }


  protected enum KnownType {
    STR    (true)  { @Override public String  read( String txt ) { return txt;                  } },
    INT    (true)  { @Override public Integer read( String txt ) { return Integer.valueOf(txt); } },
    FLOAT  (true)  { @Override public Float   read( String txt ) { return Float.valueOf(txt);   } },
    DOUBLE (true)  { @Override public Double  read( String txt ) { return Double.valueOf(txt);  } },
    LONG   (true)  { @Override public Long    read( String txt ) { return Long.valueOf(txt);    } },
    BOOL   (true)  { @Override public Boolean read( String txt ) { return Boolean.valueOf(txt); } },
    NULL   (true)  { @Override public Object  read( String txt ) { return null;                 } },
    DATE   (true)  {
      @Override
      public Date read( String txt ) {
        try {
          return new Date(Instant.parse(txt).toEpochMilli());
        }
        catch( Exception ex ) {
          if (log.isInfoEnabled()) {
            log.info("Error reading date: {}", txt, ex);
          }
        }
        return null;
      }
    },

    ARR    (false) { @Override public Object read( String txt ) { return null; } },
    LST    (false) { @Override public Object read( String txt ) { return null; } },
    RESULT (false) { @Override public Object read( String txt ) { return null; } },
    DOC    (false) { @Override public Object read( String txt ) { return null; } };

    final boolean isLeaf;

    KnownType( boolean isLeaf )
    {
      this.isLeaf = isLeaf;
    }

    public abstract Object read( String txt );

    public static KnownType get( String v )
    {
      if( v != null ) {
        try {
          return KnownType.valueOf( v.toUpperCase(Locale.ROOT) );
        }
        catch( Exception ex ) {}
      }
      return null;
    }
  };

  protected NamedList<Object> readNamedList( XMLStreamReader parser ) throws XMLStreamException
  {
    if( XMLStreamConstants.START_ELEMENT != parser.getEventType() ) {
      throw new RuntimeException( "must be start element, not: "+parser.getEventType() );
    }

    StringBuilder builder = new StringBuilder();
    NamedList<Object> nl = new SimpleOrderedMap<>();
    KnownType type = null;
    String name = null;

    // just eat up the events...
    int depth = 0;
    while( true )
    {
      switch (parser.next()) {
      case XMLStreamConstants.START_ELEMENT:
        depth++;
        builder.setLength( 0 ); // reset the text
        type = KnownType.get( parser.getLocalName() );
        if( type == null ) {
          throw new RuntimeException( "this must be known type! not: "+parser.getLocalName() );
        }

        name = null;
        int cnt = parser.getAttributeCount();
        for( int i=0; i<cnt; i++ ) {
          if( "name".equals( parser.getAttributeLocalName( i ) ) ) {
            name = parser.getAttributeValue( i );
            break;
          }
        }

        /** The name in a NamedList can actually be null
        if( name == null ) {
          throw new XMLStreamException( "requires 'name' attribute: "+parser.getLocalName(), parser.getLocation() );
        }
        **/

        if( !type.isLeaf ) {
          switch( type ) {
          case LST:    nl.add( name, readNamedList( parser ) ); depth--; continue;
          case ARR:    nl.add( name, readArray(     parser ) ); depth--; continue;
          case RESULT: nl.add( name, readDocuments( parser ) ); depth--; continue;
          case DOC:    nl.add( name, readDocument(  parser ) ); depth--; continue;
          case BOOL:
          case DATE:
          case DOUBLE:
          case FLOAT:
          case INT:
          case LONG:
          case NULL:
          case STR:
            break;
          }
          throw new XMLStreamException( "branch element not handled!", parser.getLocation() );
        }
        break;

      case XMLStreamConstants.END_ELEMENT:
        if( --depth < 0 ) {
          return nl;
        }
        //System.out.println( "NL:ELEM:"+type+"::"+name+"::"+builder );
        nl.add( name, type.read( builder.toString().trim() ) );
        break;

      case XMLStreamConstants.SPACE: // TODO?  should this be trimmed? make sure it only gets one/two space?
      case XMLStreamConstants.CDATA:
      case XMLStreamConstants.CHARACTERS:
        builder.append( parser.getText() );
        break;
      }
    }
  }

  protected List<Object> readArray( XMLStreamReader parser ) throws XMLStreamException
  {
    if( XMLStreamConstants.START_ELEMENT != parser.getEventType() ) {
      throw new RuntimeException( "must be start element, not: "+parser.getEventType() );
    }
    if( !"arr".equals( parser.getLocalName().toLowerCase(Locale.ROOT) ) ) {
      throw new RuntimeException( "must be 'arr', not: "+parser.getLocalName() );
    }

    StringBuilder builder = new StringBuilder();
    KnownType type = null;

    List<Object> vals = new ArrayList<>();

    int depth = 0;
    while( true )
    {
      switch (parser.next()) {
      case XMLStreamConstants.START_ELEMENT:
        depth++;
        KnownType t = KnownType.get( parser.getLocalName() );
        if( t == null ) {
          throw new RuntimeException( "this must be known type! not: "+parser.getLocalName() );
        }
        if( type == null ) {
          type = t;
        }
        /*** actually, there is no rule that arrays need the same type
        else if( type != t && !(t == KnownType.NULL || type == KnownType.NULL)) {
          throw new RuntimeException( "arrays must have the same type! ("+type+"!="+t+") "+parser.getLocalName() );
        }
        ***/
        type = t;

        builder.setLength( 0 ); // reset the text

        if( !type.isLeaf ) {
          switch( type ) {
          case LST:    vals.add( readNamedList( parser ) ); depth--; continue;
          case ARR:    vals.add( readArray( parser ) ); depth--; continue;
          case RESULT: vals.add( readDocuments( parser ) ); depth--; continue;
          case DOC:    vals.add( readDocument( parser ) ); depth--; continue;
          case BOOL:
          case DATE:
          case DOUBLE:
          case FLOAT:
          case INT:
          case LONG:
          case NULL:
          case STR:
            break;
          }
          throw new XMLStreamException( "branch element not handled!", parser.getLocation() );
        }
        break;

      case XMLStreamConstants.END_ELEMENT:
        if( --depth < 0 ) {
          return vals; // the last element is itself
        }
        //System.out.println( "ARR:"+type+"::"+builder );
        Object val = type.read( builder.toString().trim() );
        if( val == null && type != KnownType.NULL) {
          throw new XMLStreamException( "error reading value:"+type, parser.getLocation() );
        }
        vals.add( val );
        break;

      case XMLStreamConstants.SPACE: // TODO?  should this be trimmed? make sure it only gets one/two space?
      case XMLStreamConstants.CDATA:
      case XMLStreamConstants.CHARACTERS:
        builder.append( parser.getText() );
        break;
    }
    }
  }

  protected SolrDocumentList readDocuments( XMLStreamReader parser ) throws XMLStreamException
  {
    SolrDocumentList docs = new SolrDocumentList();

    // Parse the attributes
    for( int i=0; i<parser.getAttributeCount(); i++ ) {
      String n = parser.getAttributeLocalName( i );
      String v = parser.getAttributeValue( i );
      if( "numFound".equals( n ) ) {
        docs.setNumFound( Long.parseLong( v ) );
      }
      else if( "start".equals( n ) ) {
        docs.setStart( Long.parseLong( v ) );
      }
      else if( "maxScore".equals( n ) ) {
        docs.setMaxScore( Float.parseFloat( v ) );
      }
    }

    // Read through each document
    int event;
    while( true ) {
      event = parser.next();
      if( XMLStreamConstants.START_ELEMENT == event ) {
        if( !"doc".equals( parser.getLocalName() ) ) {
          throw new RuntimeException( "should be doc! "+parser.getLocalName() + " :: " + parser.getLocation() );
        }
        docs.add( readDocument( parser ) );
      }
      else if ( XMLStreamConstants.END_ELEMENT == event ) {
        return docs;  // only happens once
      }
    }
  }

  protected SolrDocument readDocument( XMLStreamReader parser ) throws XMLStreamException
  {
    if( XMLStreamConstants.START_ELEMENT != parser.getEventType() ) {
      throw new RuntimeException( "must be start element, not: "+parser.getEventType() );
    }
    if( !"doc".equals( parser.getLocalName().toLowerCase(Locale.ROOT) ) ) {
      throw new RuntimeException( "must be 'lst', not: "+parser.getLocalName() );
    }

    SolrDocument doc = new SolrDocument();
    StringBuilder builder = new StringBuilder();
    KnownType type = null;
    String name = null;

    // just eat up the events...
    int depth = 0;
    while( true )
    {
      switch (parser.next()) {
      case XMLStreamConstants.START_ELEMENT:
        depth++;
        builder.setLength( 0 ); // reset the text
        type = KnownType.get( parser.getLocalName() );
        if( type == null ) {
          throw new RuntimeException( "this must be known type! not: "+parser.getLocalName() );
        }

        if ( type == KnownType.DOC) {
          doc.addChildDocument(readDocument(parser));
          depth--; // (nested) readDocument clears out the (nested) 'endElement'
          continue; // may be more child docs, or other fields
        }

        // other then nested documents, all other possible nested elements require a name...

        name = null;
        int cnt = parser.getAttributeCount();
        for( int i=0; i<cnt; i++ ) {
          if( "name".equals( parser.getAttributeLocalName( i ) ) ) {
            name = parser.getAttributeValue( i );
            break;
          }
        }

        if( name == null ) {
          throw new XMLStreamException( "requires 'name' attribute: "+parser.getLocalName(), parser.getLocation() );
        }

        // Handle multi-valued fields
        if( type == KnownType.ARR ) {
          for( Object val : readArray( parser ) ) {
            doc.addField( name, val );
          }
          depth--; // the array reading clears out the 'endElement'
        } else if( type == KnownType.LST ) {
            doc.addField( name, readNamedList( parser ) );
          depth--;
        } else if( !type.isLeaf ) {
          System.out.println("nbot leaf!:" + type);

          throw new XMLStreamException( "must be value or array", parser.getLocation() );
        }
        break;

      case XMLStreamConstants.END_ELEMENT:
        if( --depth < 0 ) {
          return doc;
        }
        //System.out.println( "FIELD:"+type+"::"+name+"::"+builder );
        Object val = type.read( builder.toString().trim() );
        if( val == null ) {
          throw new XMLStreamException( "error reading value:"+type, parser.getLocation() );
        }
        doc.addField( name, val );
        break;

      case XMLStreamConstants.SPACE: // TODO?  should this be trimmed? make sure it only gets one/two space?
      case XMLStreamConstants.CDATA:
      case XMLStreamConstants.CHARACTERS:
        builder.append( parser.getText() );
        break;
      }
    }
  }


}
