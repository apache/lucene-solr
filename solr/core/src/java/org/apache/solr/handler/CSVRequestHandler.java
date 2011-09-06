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

package org.apache.solr.handler;

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.*;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.commons.csv.CSVStrategy;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.io.IOUtils;

import java.util.regex.Pattern;
import java.util.List;
import java.util.HashMap;
import java.util.Iterator;
import java.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */

public class CSVRequestHandler extends ContentStreamHandlerBase {
	public static Logger log = LoggerFactory.getLogger(CSVRequestHandler.class);

  @Override
  protected ContentStreamLoader newLoader(SolrQueryRequest req, UpdateRequestProcessor processor) {
    return new SingleThreadedCSVLoader(req, processor);
  }

  //////////////////////// SolrInfoMBeans methods //////////////////////
  @Override
  public String getDescription() {
    return "Add/Update multiple documents with CSV formatted rows";
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
}


abstract class CSVLoader extends ContentStreamLoader {
  public static final String SEPARATOR="separator";
  public static final String FIELDNAMES="fieldnames";
  public static final String HEADER="header";
  public static final String SKIP="skip";
  public static final String SKIPLINES="skipLines";
  public static final String MAP="map";
  public static final String TRIM="trim";
  public static final String EMPTY="keepEmpty";
  public static final String SPLIT="split";
  public static final String ENCAPSULATOR="encapsulator";
  public static final String ESCAPE="escape";
  public static final String OVERWRITE="overwrite";
  public static final String LITERALS_PREFIX = "literal.";

  private static Pattern colonSplit = Pattern.compile(":");
  private static Pattern commaSplit = Pattern.compile(",");
  
  public static Logger log = LoggerFactory.getLogger(CSVRequestHandler.class);

  final IndexSchema schema;
  final SolrParams params;
  final CSVStrategy strategy;
  final UpdateRequestProcessor processor;

  // hashmap to save any literal fields and their values
  HashMap <SchemaField, String> literals;
  String[] fieldnames;
  SchemaField[] fields;
  CSVLoader.FieldAdder[] adders;

  int skipLines;    // number of lines to skip at start of file

  final AddUpdateCommand templateAdd;



  /** Add a field to a document unless it's zero length.
   * The FieldAdder hierarchy handles all the complexity of
   * further transforming or splitting field values to keep the
   * main logic loop clean.  All implementations of add() must be
   * MT-safe!
   */
  private class FieldAdder {
    void add(SolrInputDocument doc, int line, int column, String val) {
      if (val.length() > 0) {
        doc.addField(fields[column].getName(),val,1.0f);
      }
    }
  }

  /** add zero length fields */
  private class FieldAdderEmpty extends CSVLoader.FieldAdder {
    @Override
    void add(SolrInputDocument doc, int line, int column, String val) {
      doc.addField(fields[column].getName(),val,1.0f);
    }
  }

  /** trim fields */
  private class FieldTrimmer extends CSVLoader.FieldAdder {
    private final CSVLoader.FieldAdder base;
    FieldTrimmer(CSVLoader.FieldAdder base) { this.base=base; }
    @Override
    void add(SolrInputDocument doc, int line, int column, String val) {
      base.add(doc, line, column, val.trim());
    }
  }

  /** map a single value.
   * for just a couple of mappings, this is probably faster than
   * using a HashMap.
   */
 private class FieldMapperSingle extends CSVLoader.FieldAdder {
   private final String from;
   private final String to;
   private final CSVLoader.FieldAdder base;
   FieldMapperSingle(String from, String to, CSVLoader.FieldAdder base) {
     this.from=from;
     this.to=to;
     this.base=base;
   }
    @Override
    void add(SolrInputDocument doc, int line, int column, String val) {
      if (from.equals(val)) val=to;
      base.add(doc,line,column,val);
    }
 }

  /** Split a single value into multiple values based on
   * a CSVStrategy.
   */
  private class FieldSplitter extends CSVLoader.FieldAdder {
    private final CSVStrategy strategy;
    private final CSVLoader.FieldAdder base;
    FieldSplitter(CSVStrategy strategy, CSVLoader.FieldAdder base) {
      this.strategy = strategy;
      this.base = base;
    }

    @Override
    void add(SolrInputDocument doc, int line, int column, String val) {
      CSVParser parser = new CSVParser(new StringReader(val), strategy);
      try {
        String[] vals = parser.getLine();
        if (vals!=null) {
          for (String v: vals) base.add(doc,line,column,v);
        } else {
          base.add(doc,line,column,val);
        }
      } catch (IOException e) {
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,e);
      }
    }
  }


  String errHeader="CSVLoader:";

  CSVLoader(SolrQueryRequest req, UpdateRequestProcessor processor) {
    this.processor = processor;
    this.params = req.getParams();
    schema = req.getSchema();
    this.literals = new HashMap<SchemaField, String>();

    templateAdd = new AddUpdateCommand(req);
    templateAdd.overwrite=params.getBool(OVERWRITE,true);
    templateAdd.commitWithin = params.getInt(UpdateParams.COMMIT_WITHIN, -1);
    
    strategy = new CSVStrategy(',', '"', CSVStrategy.COMMENTS_DISABLED, CSVStrategy.ESCAPE_DISABLED, false, false, false, true);
    String sep = params.get(SEPARATOR);
    if (sep!=null) {
      if (sep.length()!=1) throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"Invalid separator:'"+sep+"'");
      strategy.setDelimiter(sep.charAt(0));
    }

    String encapsulator = params.get(ENCAPSULATOR);
    if (encapsulator!=null) {
      if (encapsulator.length()!=1) throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"Invalid encapsulator:'"+encapsulator+"'");
    }

    String escape = params.get(ESCAPE);
    if (escape!=null) {
      if (escape.length()!=1) throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"Invalid escape:'"+escape+"'");
    }

    // if only encapsulator or escape is set, disable the other escaping mechanism
    if (encapsulator == null && escape != null) {
      strategy.setEncapsulator( CSVStrategy.ENCAPSULATOR_DISABLED);     
      strategy.setEscape(escape.charAt(0));
    } else {
      if (encapsulator != null) {
        strategy.setEncapsulator(encapsulator.charAt(0));
      }
      if (escape != null) {
        char ch = escape.charAt(0);
        strategy.setEscape(ch);
        if (ch == '\\') {
          // If the escape is the standard backslash, then also enable
          // unicode escapes (it's harmless since 'u' would not otherwise
          // be escaped.                    
          strategy.setUnicodeEscapeInterpretation(true);
        }
      }
    }

    String fn = params.get(FIELDNAMES);
    fieldnames = fn != null ? commaSplit.split(fn,-1) : null;

    Boolean hasHeader = params.getBool(HEADER);

    skipLines = params.getInt(SKIPLINES,0);

    if (fieldnames==null) {
      if (null == hasHeader) {
        // assume the file has the headers if they aren't supplied in the args
        hasHeader=true;
      } else if (!hasHeader) {
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"CSVLoader: must specify fieldnames=<fields>* or header=true");
      }
    } else {
      // if the fieldnames were supplied and the file has a header, we need to
      // skip over that header.
      if (hasHeader!=null && hasHeader) skipLines++;

      prepareFields();
    }
  }

  /** create the FieldAdders that control how each field  is indexed */
  void prepareFields() {
    // Possible future optimization: for really rapid incremental indexing
    // from a POST, one could cache all of this setup info based on the params.
    // The link from FieldAdder to this would need to be severed for that to happen.

    fields = new SchemaField[fieldnames.length];
    adders = new CSVLoader.FieldAdder[fieldnames.length];
    String skipStr = params.get(SKIP);
    List<String> skipFields = skipStr==null ? null : StrUtils.splitSmart(skipStr,',');

    CSVLoader.FieldAdder adder = new CSVLoader.FieldAdder();
    CSVLoader.FieldAdder adderKeepEmpty = new CSVLoader.FieldAdderEmpty();

    for (int i=0; i<fields.length; i++) {
      String fname = fieldnames[i];
      // to skip a field, leave the entries in fields and addrs null
      if (fname.length()==0 || (skipFields!=null && skipFields.contains(fname))) continue;

      fields[i] = schema.getField(fname);
      boolean keepEmpty = params.getFieldBool(fname,EMPTY,false);
      adders[i] = keepEmpty ? adderKeepEmpty : adder;

      // Order that operations are applied: split -> trim -> map -> add
      // so create in reverse order.
      // Creation of FieldAdders could be optimized and shared among fields

      String[] fmap = params.getFieldParams(fname,MAP);
      if (fmap!=null) {
        for (String mapRule : fmap) {
          String[] mapArgs = colonSplit.split(mapRule,-1);
          if (mapArgs.length!=2)
            throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "Map rules must be of the form 'from:to' ,got '"+mapRule+"'");
          adders[i] = new CSVLoader.FieldMapperSingle(mapArgs[0], mapArgs[1], adders[i]);
        }
      }
 
      if (params.getFieldBool(fname,TRIM,false)) {
        adders[i] = new CSVLoader.FieldTrimmer(adders[i]);
      }

      if (params.getFieldBool(fname,SPLIT,false)) {
        String sepStr = params.getFieldParam(fname,SEPARATOR);
        char fsep = sepStr==null || sepStr.length()==0 ? ',' : sepStr.charAt(0);
        String encStr = params.getFieldParam(fname,ENCAPSULATOR);
        char fenc = encStr==null || encStr.length()==0 ? (char)-2 : encStr.charAt(0);
        String escStr = params.getFieldParam(fname,ESCAPE);
        char fesc = escStr==null || escStr.length()==0 ? CSVStrategy.ESCAPE_DISABLED : escStr.charAt(0);

        CSVStrategy fstrat = new CSVStrategy(fsep,fenc,CSVStrategy.COMMENTS_DISABLED,fesc, false, false, false, false);
        adders[i] = new CSVLoader.FieldSplitter(fstrat, adders[i]);
      }
    }
    // look for any literal fields - literal.foo=xyzzy
    Iterator<String> paramNames = params.getParameterNamesIterator();
    while (paramNames.hasNext()) {
      String pname = paramNames.next();
      if (!pname.startsWith(LITERALS_PREFIX)) continue;

      String name = pname.substring(LITERALS_PREFIX.length());
      SchemaField sf = schema.getFieldOrNull(name);
      if(sf == null)
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"Invalid field name for literal:'"+ name +"'");
      literals.put(sf, params.get(pname));
    }
  }

  private void input_err(String msg, String[] line, int lineno) {
    StringBuilder sb = new StringBuilder();
    sb.append(errHeader).append(", line=").append(lineno).append(",").append(msg).append("\n\tvalues={");
    for (String val: line) {
      sb.append("'").append(val).append("',"); }
    sb.append('}');
    throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,sb.toString());
  }

  private void input_err(String msg, String[] lines, int lineNo, Throwable e) {
    StringBuilder sb = new StringBuilder();
    sb.append(errHeader).append(", line=").append(lineNo).append(",").append(msg).append("\n\tvalues={");
    if (lines != null) {
      for (String val : lines) {
        sb.append("'").append(val).append("',");
      }
    } else {
      sb.append("NO LINES AVAILABLE");
    }
    sb.append('}');
    throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,sb.toString(), e);
  }

  /** load the CSV input */
  @Override
  public void load(SolrQueryRequest req, SolrQueryResponse rsp, ContentStream stream) throws IOException {
    errHeader = "CSVLoader: input=" + stream.getSourceInfo();
    Reader reader = null;
    try {
      reader = stream.getReader();
      if (skipLines>0) {
        if (!(reader instanceof BufferedReader)) {
          reader = new BufferedReader(reader);
        }
        BufferedReader r = (BufferedReader)reader;
        for (int i=0; i<skipLines; i++) {
          r.readLine();
        }
      }

      CSVParser parser = new CSVParser(reader, strategy);

      // parse the fieldnames from the header of the file
      if (fieldnames==null) {
        fieldnames = parser.getLine();
        if (fieldnames==null) {
          throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"Expected fieldnames in CSV input");
        }
        prepareFields();
      }

      // read the rest of the CSV file
      for(;;) {
        int line = parser.getLineNumber();  // for error reporting in MT mode
        String[] vals = null;
        try {
          vals = parser.getLine();
        } catch (IOException e) {
          //Catch the exception and rethrow it with more line information
         input_err("can't read line: " + line, null, line, e);
        }
        if (vals==null) break;

        if (vals.length != fields.length) {
          input_err("expected "+fields.length+" values but got "+vals.length, vals, line);
        }

        addDoc(line,vals);
      }
    } finally{
      if (reader != null) {
        IOUtils.closeQuietly(reader);
      }
    }
  }

  /** called for each line of values (document) */
  abstract void addDoc(int line, String[] vals) throws IOException;

  /** this must be MT safe... may be called concurrently from multiple threads. */
  void doAdd(int line, String[] vals, SolrInputDocument doc, AddUpdateCommand template) throws IOException {
    // the line number is passed simply for error reporting in MT mode.
    // first, create the lucene document
    for (int i=0; i<vals.length; i++) {
      if (fields[i]==null) continue;  // ignore this field
      String val = vals[i];
      adders[i].add(doc, line, i, val);
    }

    // add any literals
    for (SchemaField sf : literals.keySet()) {
    	String fn = sf.getName();
    	String val = literals.get(sf);
    	doc.addField(fn, val);
    }
   
    template.solrDoc = doc;
    processor.processAdd(template);
  }

}


class SingleThreadedCSVLoader extends CSVLoader {
  SingleThreadedCSVLoader(SolrQueryRequest req, UpdateRequestProcessor processor) {
    super(req, processor);
  }

  @Override
  void addDoc(int line, String[] vals) throws IOException {
    templateAdd.clear();
    SolrInputDocument doc = new SolrInputDocument();
    doAdd(line, vals, doc, templateAdd);
  }
}


