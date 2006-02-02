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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.search.Similarity;
import org.apache.solr.core.SolrException;
import org.apache.solr.core.Config;
import org.apache.solr.analysis.TokenFilterFactory;
import org.apache.solr.analysis.TokenizerChain;
import org.apache.solr.analysis.TokenizerFactory;
import org.apache.solr.util.DOMUtil;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.InputStream;
import java.io.Reader;
import java.util.*;
import java.util.logging.Logger;

/**
 * <code>IndexSchema</code> contains information about the valid fields in an index
 * and the types of those fields.
 *
 * @author yonik
 * @version $Id: IndexSchema.java,v 1.21 2005/12/20 16:05:46 yonik Exp $
 */

public final class IndexSchema {
  final static Logger log = Logger.getLogger(IndexSchema.class.getName());

  private final String schemaFile;
  private String name;
  private float version;

  public IndexSchema(String schemaFile) {
    this.schemaFile=schemaFile;
    readConfig();
  }

  public InputStream getInputStream() {
    return Config.openResource(schemaFile);
  }


  float getVersion() {
    return version;
  }

  public String getName() { return name; }

  private final HashMap<String, SchemaField> fields = new HashMap<String,SchemaField>();
  private final HashMap<String, FieldType> fieldTypes = new HashMap<String,FieldType>();

  public Map<String,SchemaField> getFields() { return fields; }
  public Map<String,FieldType> getFieldTypes() { return fieldTypes; }


  private Similarity similarity;
  public Similarity getSimilarity() { return similarity; }

  private Analyzer analyzer;
  public Analyzer getAnalyzer() { return analyzer; }

  private Analyzer queryAnalyzer;
  public Analyzer getQueryAnalyzer() { return queryAnalyzer; }

  private String defaultSearchFieldName=null;
  public String getDefaultSearchFieldName() {
    return defaultSearchFieldName;
  }

  private SchemaField uniqueKeyField;
  public SchemaField getUniqueKeyField() { return uniqueKeyField; }

  private String uniqueKeyFieldName;
  private FieldType uniqueKeyFieldType;

  public Field getUniqueKeyField(org.apache.lucene.document.Document doc) {
    return doc.getField(uniqueKeyFieldName);  // this should return null if name is null
  }

  public String printableUniqueKey(org.apache.lucene.document.Document doc) {
     Field f = doc.getField(uniqueKeyFieldName);
     return f==null ? null : uniqueKeyFieldType.toExternal(f);
  }

  private SchemaField getIndexedField(String fname) {
    SchemaField f = getFields().get(fname);
    if (f==null) {
      throw new RuntimeException("unknown field '" + fname + "'");
    }
    if (!f.indexed()) {
      throw new RuntimeException("'"+fname+"' is not an indexed field:" + f);
    }
    return f;
  }



  private class SolrIndexAnalyzer extends Analyzer {
    protected final HashMap<String,Analyzer> analyzers;

    SolrIndexAnalyzer() {
      analyzers = analyzerCache();
    }

    protected HashMap<String,Analyzer> analyzerCache() {
      HashMap<String,Analyzer> cache = new HashMap<String,Analyzer>();
       for (SchemaField f : getFields().values()) {
        Analyzer analyzer = f.getType().getAnalyzer();
        cache.put(f.getName(), analyzer);
      }
      return cache;
    }

    protected Analyzer getAnalyzer(String fieldName)
    {
      Analyzer analyzer = analyzers.get(fieldName);
      return analyzer!=null ? analyzer : getDynamicFieldType(fieldName).getAnalyzer();
    }

    public TokenStream tokenStream(String fieldName, Reader reader)
    {
      return getAnalyzer(fieldName).tokenStream(fieldName,reader);
    }

    public int getPositionIncrementGap(String fieldName) {
      return getAnalyzer(fieldName).getPositionIncrementGap(fieldName);
    }
  }


  private class SolrQueryAnalyzer extends SolrIndexAnalyzer {
    protected HashMap<String,Analyzer> analyzerCache() {
      HashMap<String,Analyzer> cache = new HashMap<String,Analyzer>();
       for (SchemaField f : getFields().values()) {
        Analyzer analyzer = f.getType().getQueryAnalyzer();
        cache.put(f.getName(), analyzer);
      }
      return cache;
    }

    protected Analyzer getAnalyzer(String fieldName)
    {
      Analyzer analyzer = analyzers.get(fieldName);
      return analyzer!=null ? analyzer : getDynamicFieldType(fieldName).getQueryAnalyzer();
    }
  }


  private void readConfig() {
    log.info("Reading Solr Schema");

    try {
      /***
      DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
      Document document = builder.parse(getInputStream());
      ***/

      Config config = new Config("schema", getInputStream(), "/schema/");
      Document document = config.getDocument();
      XPath xpath = config.getXPath();

      Node nd = (Node) xpath.evaluate("/schema/@name", document, XPathConstants.NODE);
      if (nd==null) {
        log.warning("schema has no name!");
      } else {
        name = nd.getNodeValue();
        log.info("Schema name=" + name);
      }

      version = config.getFloat("/schema/@version", 1.0f);

      String expression = "/schema/types/fieldtype";
      NodeList nodes = (NodeList) xpath.evaluate(expression, document, XPathConstants.NODESET);


      for (int i=0; i<nodes.getLength(); i++) {
        Node node = nodes.item(i);
        NamedNodeMap attrs = node.getAttributes();

        String name = DOMUtil.getAttr(attrs,"name","fieldtype error");
        log.finest("reading fieldtype "+name);
        String clsName = DOMUtil.getAttr(attrs,"class", "fieldtype error");
        FieldType ft = (FieldType)Config.newInstance(clsName);
        ft.setTypeName(name);

        expression = "./analyzer[@type='query']";
        Node anode = (Node)xpath.evaluate(expression, node, XPathConstants.NODE);
        Analyzer queryAnalyzer = readAnalyzer(anode);

        // An analyzer without a type specified, or with type="index"
        expression = "./analyzer[not(@type)] | ./analyzer[@type='index']";
        anode = (Node)xpath.evaluate(expression, node, XPathConstants.NODE);
        Analyzer analyzer = readAnalyzer(anode);

        if (queryAnalyzer==null) queryAnalyzer=analyzer;
        if (analyzer==null) analyzer=queryAnalyzer;
        if (analyzer!=null) {
          ft.setAnalyzer(analyzer);
          ft.setQueryAnalyzer(queryAnalyzer);
        }


        ft.setArgs(this, DOMUtil.toMapExcept(attrs,"name","class"));
        fieldTypes.put(ft.typeName,ft);
        log.finest("fieldtype defined: " + ft);
      }


      ArrayList<DynamicField> dFields = new ArrayList<DynamicField>();
      expression = "/schema/fields/field | /schema/fields/dynamicField";
      nodes = (NodeList) xpath.evaluate(expression, document, XPathConstants.NODESET);

      for (int i=0; i<nodes.getLength(); i++) {
        Node node = nodes.item(i);

        NamedNodeMap attrs = node.getAttributes();

        String name = DOMUtil.getAttr(attrs,"name","field definition");
        log.finest("reading field def "+name);
        String type = DOMUtil.getAttr(attrs,"type","field " + name);
        String val;

        FieldType ft = fieldTypes.get(type);
        if (ft==null) {
          throw new SolrException(400,"Unknown fieldtype '" + type + "'",false);
        }

        Map<String,String> args = DOMUtil.toMapExcept(attrs, "name", "type");

        SchemaField f = SchemaField.create(name,ft,args);

        if (node.getNodeName().equals("field")) {
          fields.put(f.getName(),f);
          log.fine("field defined: " + f);
        } else if (node.getNodeName().equals("dynamicField")) {
          dFields.add(new DynamicField(f));
          log.fine("dynamic field defined: " + f);
        } else {
          // we should never get here
          throw new RuntimeException("Unknown field type");
        }
      }

    // OK, now sort the dynamic fields largest to smallest size so we don't get
    // any false matches.  We want to act like a compiler tool and try and match
    // the largest string possible.
    Collections.sort(dFields, new Comparator<DynamicField>() {
        public int compare(DynamicField a, DynamicField b) {
           // swap natural ordering to get biggest first.
           // The sort is stable, so elements of the same size should
           // be
           if (a.regex.length() < b.regex.length()) return 1;
           else if (a.regex.length() > b.regex.length()) return -1;
           return 0;
        }
      }
    );

    log.finest("Dynamic Field Ordering:" + dFields);

    // stuff it in a normal array for faster access
    dynamicFields = (DynamicField[])dFields.toArray(new DynamicField[dFields.size()]);


    Node node = (Node) xpath.evaluate("/schema/similarity/@class", document, XPathConstants.NODE);
    if (node==null) {
      similarity = new DefaultSimilarity();
      log.fine("using default similarity");
    } else {
      similarity = (Similarity)Config.newInstance(node.getNodeValue().trim());
      log.fine("using similarity " + similarity.getClass().getName());
    }

    node = (Node) xpath.evaluate("/schema/defaultSearchField/text()", document, XPathConstants.NODE);
    if (node==null) {
      log.warning("no default search field specified in schema.");
    } else {
      String defName=node.getNodeValue().trim();
      defaultSearchFieldName = getIndexedField(defName)!=null ? defName : null;
      log.info("default search field is "+defName);
    }

    node = (Node) xpath.evaluate("/schema/uniqueKey/text()", document, XPathConstants.NODE);
    if (node==null) {
      log.warning("no uniqueKey specified in schema.");
    } else {
      uniqueKeyField=getIndexedField(node.getNodeValue().trim());
      uniqueKeyFieldName=uniqueKeyField.getName();
      uniqueKeyFieldType=uniqueKeyField.getType();
      log.info("unique key field: "+uniqueKeyFieldName);
    }

    /////////////// parse out copyField commands ///////////////
    // Map<String,ArrayList<SchemaField>> cfields = new HashMap<String,ArrayList<SchemaField>>();
    // expression = "/schema/copyField";
    expression = "//copyField";
    nodes = (NodeList) xpath.evaluate(expression, document, XPathConstants.NODESET);

      for (int i=0; i<nodes.getLength(); i++) {
        node = nodes.item(i);
        NamedNodeMap attrs = node.getAttributes();

        String source = DOMUtil.getAttr(attrs,"source","copyField definition");
        String dest = DOMUtil.getAttr(attrs,"dest","copyField definition");
        log.fine("copyField source='"+source+"' dest='"+dest+"'");
        SchemaField f = getField(source);
        SchemaField d = getField(dest);
        SchemaField[] destArr = copyFields.get(source);
        if (destArr==null) {
          destArr=new SchemaField[]{d};
        } else {
          destArr = (SchemaField[])append(destArr,d);
        }
        copyFields.put(source,destArr);
      }


    } catch (SolrException e) {
      throw e;
    } catch(Exception e) {
      // unexpected exception...
      throw new SolrException(1,"Schema Parsing Failed",e,false);
    }

     analyzer = new SolrIndexAnalyzer();
     queryAnalyzer = new SolrQueryAnalyzer();
  }

  private static Object[] append(Object[] orig, Object item) {
    Object[] newArr = (Object[])java.lang.reflect.Array.newInstance(orig.getClass().getComponentType(), orig.length+1);
	  System.arraycopy(orig, 0, newArr, 0, orig.length);
    newArr[orig.length] = item;
    return newArr;
  }

  //
  // <analyzer><tokenizer class="...."/><tokenizer class="...." arg="....">
  //
  //
  private Analyzer readAnalyzer(Node node) throws XPathExpressionException {
    // parent node used to be passed in as "fieldtype"
    // if (!fieldtype.hasChildNodes()) return null;
    // Node node = DOMUtil.getChild(fieldtype,"analyzer");

    if (node == null) return null;
    NamedNodeMap attrs = node.getAttributes();
    String analyzerName = DOMUtil.getAttr(attrs,"class");
    if (analyzerName != null) {
      return (Analyzer)Config.newInstance(analyzerName);
    }

    XPath xpath = XPathFactory.newInstance().newXPath();
    Node tokNode = (Node)xpath.evaluate("./tokenizer", node, XPathConstants.NODE);
    NodeList nList = (NodeList)xpath.evaluate("./filter", node, XPathConstants.NODESET);

    if (tokNode==null){
      throw new SolrException(1,"analyzer without class or tokenizer & filter list");
    }
    TokenizerFactory tfac = readTokenizerFactory(tokNode);

    /******
    // oops, getChildNodes() includes text (newlines, etc) in addition
    // to the actual child elements
    NodeList nList = node.getChildNodes();
    TokenizerFactory tfac = readTokenizerFactory(nList.item(0));
     if (tfac==null) {
       throw new SolrException(1,"TokenizerFactory must be specified first in analyzer");
     }
    ******/

    ArrayList<TokenFilterFactory> filters = new ArrayList<TokenFilterFactory>();
    for (int i=0; i<nList.getLength(); i++) {
      TokenFilterFactory filt = readTokenFilterFactory(nList.item(i));
      if (filt != null) filters.add(filt);
    }

    return new TokenizerChain(tfac, filters.toArray(new TokenFilterFactory[filters.size()]));
  };

  // <tokenizer class="solr.StandardFilterFactory"/>
  private TokenizerFactory readTokenizerFactory(Node node) {
    // if (node.getNodeName() != "tokenizer") return null;
    NamedNodeMap attrs = node.getAttributes();
    String className = DOMUtil.getAttr(attrs,"class","tokenizer");
    TokenizerFactory tfac = (TokenizerFactory)Config.newInstance(className);
    tfac.init(DOMUtil.toMapExcept(attrs,"class"));
    return tfac;
  }

  // <tokenizer class="solr.StandardFilterFactory"/>
  private TokenFilterFactory readTokenFilterFactory(Node node) {
    // if (node.getNodeName() != "filter") return null;
    NamedNodeMap attrs = node.getAttributes();
    String className = DOMUtil.getAttr(attrs,"class","token filter");
    TokenFilterFactory tfac = (TokenFilterFactory)Config.newInstance(className);
    tfac.init(DOMUtil.toMapExcept(attrs,"class"));
    return tfac;
  }


  //
  // Instead of storing a type, this could be implemented as a hierarchy
  // with a virtual matches().
  // Given how often a search will be done, however, speed is the overriding
  // concern and I'm not sure which is faster.
  //
  final static class DynamicField {
    final static int STARTS_WITH=1;
    final static int ENDS_WITH=2;

    final String regex;
    final int type;
    final SchemaField prototype;

    final String str;

    DynamicField(SchemaField prototype) {
      this.regex=prototype.name;
      if (regex.startsWith("*")) {
        type=ENDS_WITH;
        str=regex.substring(1);
      }
      else if (regex.endsWith("*")) {
        type=STARTS_WITH;
        str=regex.substring(0,regex.length()-1);
      }
      else {
        throw new RuntimeException("dynamic field name must start or end with *");
      }
      this.prototype=prototype;
    }

    boolean matches(String name) {
      if (type==STARTS_WITH && name.startsWith(str)) return true;
      else if (type==ENDS_WITH && name.endsWith(str)) return true;
      else return false;
    }

    SchemaField makeSchemaField(String name) {
      // could have a cache instead of returning a new one each time, but it might
      // not be worth it.
      // Actually, a higher level cache could be worth it to avoid too many
      // .startsWith() and .endsWith() comparisons.  it depends on how many
      // dynamic fields there are.
      return new SchemaField(prototype, name);
    }

    public String toString() {
      return prototype.toString();
    }
  }



  private DynamicField[] dynamicFields;


  // get a field, and if not statically defined, check dynamic fields.
  public SchemaField getField(String fieldName) {
     SchemaField f = fields.get(fieldName);
    if (f != null) return f;

    for (DynamicField df : dynamicFields) {
      if (df.matches(fieldName)) return df.makeSchemaField(fieldName);
    }

    // Hmmm, default field could also be implemented with a dynamic field of "*".
    // It would have to be special-cased and only used if nothing else matched.
    /***  REMOVED -YCS
    if (defaultFieldType != null) return new SchemaField(fieldName,defaultFieldType);
    ***/
    throw new SolrException(1,"undefined field "+fieldName);
  }

  // This method exists because it can be more efficient for dynamic fields
  // if a full SchemaField isn't needed.
  public FieldType getFieldType(String fieldName) {
    SchemaField f = fields.get(fieldName);
    if (f != null) return f.getType();

    return getDynamicFieldType(fieldName);
  }

  /**
   * return null instead of throwing an exception if
   * the field is undefined.
   */
  public FieldType getFieldTypeNoEx(String fieldName) {
    SchemaField f = fields.get(fieldName);
    if (f != null) return f.getType();
    return dynFieldType(fieldName);
  }


  public FieldType getDynamicFieldType(String fieldName) {
     for (DynamicField df : dynamicFields) {
      if (df.matches(fieldName)) return df.prototype.getType();
    }
    throw new SolrException(400,"undefined field "+fieldName);
  }

  private FieldType dynFieldType(String fieldName) {
     for (DynamicField df : dynamicFields) {
      if (df.matches(fieldName)) return df.prototype.getType();
    }
    return null;
  };


  private final Map<String, SchemaField[]> copyFields = new HashMap<String,SchemaField[]>();
  public SchemaField[] getCopyFields(String sourceField) {
    return copyFields.get(sourceField);
  }

}

