/**
 * Copyright 2006 The Apache Software Foundation
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

package org.apache.solr.schema;

import org.apache.lucene.document.Field;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.search.SortField;
import org.apache.solr.search.function.ValueSource;
import org.apache.solr.search.function.OrdFieldSource;
import org.apache.solr.search.Sorting;
import org.apache.solr.request.XMLWriter;
import org.apache.solr.analysis.SolrAnalyzer;

import java.util.logging.Logger;
import java.util.Map;
import java.util.HashMap;
import java.io.Reader;
import java.io.IOException;

/**
 * Base class for all field types used by an index schema.
 *
 * @author yonik
 * @version $Id$
 */
public abstract class FieldType extends FieldProperties {
  public static final Logger log = Logger.getLogger(FieldType.class.getName());

  protected String typeName;  // the name of the type, not the name of the field
  protected Map<String,String> args;  // additional arguments
  protected int trueProperties;   // properties explicitly set to true
  protected int falseProperties;  // properties explicitly set to false
  int properties;

  protected boolean isTokenized() {
    return (properties & TOKENIZED) != 0;
  }

  /** subclasses should initialize themselves with the args provided
   * and remove valid arguments.  leftover arguments will cause an exception.
   * Common boolean properties have already been handled.
   *
   */
  protected void init(IndexSchema schema, Map<String,String> args) {
  }

  // Handle additional arguments...
  void setArgs(IndexSchema schema, Map<String,String> args) {
    // default to STORED and INDEXED, and MULTIVALUED depending on schema version
    properties = (STORED | INDEXED);
    if (schema.getVersion()< 1.1f) properties |= MULTIVALUED;

    this.args=args;
    Map<String,String> initArgs = new HashMap<String,String>(args);

    trueProperties = FieldProperties.parseProperties(initArgs,true);
    falseProperties = FieldProperties.parseProperties(initArgs,false);

    properties &= ~falseProperties;
    properties |= trueProperties;

    for (String prop : FieldProperties.propertyNames) initArgs.remove(prop);

    init(schema, initArgs);

    String positionInc = initArgs.get("positionIncrementGap");
    if (positionInc != null) {
      Analyzer analyzer = getAnalyzer();
      if (analyzer instanceof SolrAnalyzer) {
        ((SolrAnalyzer)analyzer).setPositionIncrementGap(Integer.parseInt(positionInc));
      } else {
        throw new RuntimeException("Can't set positionIncrementGap on custom analyzer " + analyzer.getClass());
      }
      analyzer = getQueryAnalyzer();
      if (analyzer instanceof SolrAnalyzer) {
        ((SolrAnalyzer)analyzer).setPositionIncrementGap(Integer.parseInt(positionInc));
      } else {
        throw new RuntimeException("Can't set positionIncrementGap on custom analyzer " + analyzer.getClass());
      }
      initArgs.remove("positionIncrementGap");
    }

    if (initArgs.size() > 0) {
      throw new RuntimeException("schema fieldtype " + typeName
              + "("+ this.getClass().getName() + ")"
              + " invalid arguments:" + initArgs);
    }
  }

  protected void restrictProps(int props) {
    if ((properties & props) != 0) {
      throw new RuntimeException("schema fieldtype " + typeName
              + "("+ this.getClass().getName() + ")"
              + " invalid properties:" + propertiesToString(properties & props));
    }
  }


  public String getTypeName() {
    return typeName;
  }

  void setTypeName(String typeName) {
    this.typeName = typeName;
  }

  public String toString() {
    return typeName + "{class=" + this.getClass().getName()
//            + propertiesToString(properties)
            + (analyzer != null ? ",analyzer=" + analyzer.getClass().getName() : "")
            + ",args=" + args
            +"}";
  }


  // used for adding a document when a field needs to be created from a type and a string
  // by default, the indexed value is the same as the stored value (taken from toInternal())
  // Having a different representation for external, internal, and indexed would present quite
  // a few problems given the current Lucene architecture.  An analyzer for adding docs would
  // need to translate internal->indexed while an analyzer for querying would need to
  // translate external->indexed.
  //
  // The only other alternative to having internal==indexed would be to have
  // internal==external.
  // In this case, toInternal should convert to the indexed representation,
  // toExternal() should do nothing, and createField() should *not* call toInternal,
  // but use the external value and set tokenized=true to get Lucene to convert
  // to the internal(indexed) form.
  public Field createField(SchemaField field, String externalVal, float boost) {
    String val = toInternal(externalVal);
    if (val==null) return null;
    Field f =  new Field(field.getName(),val,
            field.stored() ? Field.Store.YES : Field.Store.NO ,
            field.indexed() ? (isTokenized() ? Field.Index.TOKENIZED : Field.Index.UN_TOKENIZED)
                    : Field.Index.NO);
    f.setOmitNorms(field.omitNorms());
    f.setBoost(boost);
    return f;
  }


  // Convert an external value (from XML update command or from query string)
  // into the internal format.
  // - used in delete when a Term needs to be created.
  // - used by the default getTokenizer() and createField()
  public String toInternal(String val) {
    return val;
  }

  // Convert the stored-field format to an external (string, human readable) value
  // currently used in writing XML of the search result (but perhaps
  // a more efficient toXML(Field f, Writer w) should be used
  // in the future.
  public String toExternal(Field f) {
    return f.stringValue();
  }


  public String indexedToReadable(String indexedForm) {
    return indexedForm;
  }


  /*********
  // default analyzer for non-text fields.
  // Only reads 80 bytes, but that should be plenty for a single value.
  public Analyzer getAnalyzer() {
    if (analyzer != null) return analyzer;

    // the default analyzer...
    return new Analyzer() {
      public TokenStream tokenStream(String fieldName, Reader reader) {
        return new Tokenizer(reader) {
          final char[] cbuf = new char[80];
          public Token next() throws IOException {
            int n = input.read(cbuf,0,80);
            if (n<=0) return null;
            String s = toInternal(new String(cbuf,0,n));
            return new Token(s,0,n);
          };
        };
      }
    };
  }
  **********/


  //
  // Default analyzer for types that only produce 1 verbatim token...
  // A maximum size of chars to be read must be specified
  //
  protected final class DefaultAnalyzer extends SolrAnalyzer {
    final int maxChars;

    DefaultAnalyzer(int maxChars) {
      this.maxChars=maxChars;
    }

    public TokenStream tokenStream(String fieldName, Reader reader) {
      return new Tokenizer(reader) {
        char[] cbuf = new char[maxChars];
        public Token next() throws IOException {
          int n = input.read(cbuf,0,maxChars);
          if (n<=0) return null;
          String s = toInternal(new String(cbuf,0,n));  // virtual func on parent
          return new Token(s,0,n);
        };
      };
    }
  }


  //analyzer set by schema for text types.
  //subclasses can set analyzer themselves or override getAnalyzer()
  protected Analyzer analyzer=new DefaultAnalyzer(256);
  protected Analyzer queryAnalyzer=analyzer;

  // get analyzer should be fast to call... since the addition of dynamic fields,
  // this can be called all the time instead of just once at startup.
  // The analyzer will only be used in the following scenarios:
  // - during a document add for any field that has "tokenized" set (typically
  //   only Text fields)
  // - during query parsing

  public Analyzer getAnalyzer() {
    return analyzer;
  }

  public Analyzer getQueryAnalyzer() {
    return queryAnalyzer;
  }

  // This is called by the schema parser if a custom analyzer is defined
  public void setAnalyzer(Analyzer analyzer) {
    this.analyzer = analyzer;
    log.finest("FieldType: " + typeName + ".setAnalyzer(" + analyzer.getClass().getName() + ")" );
  }

   // This is called by the schema parser if a custom analyzer is defined
  public void setQueryAnalyzer(Analyzer analyzer) {
    this.queryAnalyzer = analyzer;
    log.finest("FieldType: " + typeName + ".setQueryAnalyzer(" + analyzer.getClass().getName() + ")" );
  }


  public abstract void write(XMLWriter xmlWriter, String name, Field f) throws IOException;


  public abstract SortField getSortField(SchemaField field, boolean top);

  protected SortField getStringSort(SchemaField field, boolean reverse) {
    return Sorting.getStringSortField(field.name, reverse, field.sortMissingLast(),field.sortMissingFirst());
  }

  /** called to get the default value source (normally, from the
   *  Lucene FieldCache.)
   */
  public ValueSource getValueSource(SchemaField field) {
    return new OrdFieldSource(field.name);
  }
}
