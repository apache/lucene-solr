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
package org.apache.solr.handler.loader;

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.update.*;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.internal.csv.CSVStrategy;
import org.apache.solr.internal.csv.CSVParser;
import org.apache.commons.io.IOUtils;

import java.util.Map;
import java.util.regex.Pattern;
import java.util.List;
import java.util.HashMap;
import java.util.Iterator;
import java.io.*;

public abstract class CSVLoaderBase extends ContentStreamLoader {
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
  public static final String ROW_ID = "rowid";
  public static final String ROW_ID_OFFSET = "rowidOffset";

  private static Pattern colonSplit = Pattern.compile(":");
  private static Pattern commaSplit = Pattern.compile(",");
  
  final SolrParams params;
  final CSVStrategy strategy;
  protected final UpdateRequestProcessor processor;
  // hashmap to save any literal fields and their values
  HashMap <String, String> literals;

  String[] fieldnames;
  CSVLoaderBase.FieldAdder[] adders;

  String rowId = null;// if not null, add a special field by the name given with the line number/row id as the value
  int rowIdOffset = 0; //add to line/rowid before creating the field

  int skipLines;    // number of lines to skip at start of file

  protected final AddUpdateCommand templateAdd;



  /** Add a field to a document unless it's zero length.
   * The FieldAdder hierarchy handles all the complexity of
   * further transforming or splitting field values to keep the
   * main logic loop clean.  All implementations of add() must be
   * MT-safe!
   */
  private class FieldAdder {
    void add(SolrInputDocument doc, int line, int column, String val) {
      if (val.length() > 0) {
        doc.addField(fieldnames[column],val);
      }
    }
  }

  /** add zero length fields */
  private class FieldAdderEmpty extends CSVLoaderBase.FieldAdder {
    @Override
    void add(SolrInputDocument doc, int line, int column, String val) {
      doc.addField(fieldnames[column],val);
    }
  }

  /** trim fields */
  private class FieldTrimmer extends CSVLoaderBase.FieldAdder {
    private final CSVLoaderBase.FieldAdder base;
    FieldTrimmer(CSVLoaderBase.FieldAdder base) { this.base=base; }
    @Override
    void add(SolrInputDocument doc, int line, int column, String val) {
      base.add(doc, line, column, val.trim());
    }
  }

  /** map a single value.
   * for just a couple of mappings, this is probably faster than
   * using a HashMap.
   */
 private class FieldMapperSingle extends CSVLoaderBase.FieldAdder {
   private final String from;
   private final String to;
   private final CSVLoaderBase.FieldAdder base;
   FieldMapperSingle(String from, String to, CSVLoaderBase.FieldAdder base) {
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
  private class FieldSplitter extends CSVLoaderBase.FieldAdder {
    private final CSVStrategy strategy;
    private final CSVLoaderBase.FieldAdder base;
    FieldSplitter(CSVStrategy strategy, CSVLoaderBase.FieldAdder base) {
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

  protected CSVLoaderBase(SolrQueryRequest req, UpdateRequestProcessor processor) {
    this.processor = processor;
    this.params = req.getParams();
    this.literals = new HashMap<>();

    templateAdd = new AddUpdateCommand(req);
    templateAdd.overwrite=params.getBool(OVERWRITE,true);
    templateAdd.commitWithin = params.getInt(UpdateParams.COMMIT_WITHIN, -1);
    
    strategy = new CSVStrategy(',', '"', CSVStrategy.COMMENTS_DISABLED, CSVStrategy.ESCAPE_DISABLED, false, false, false, true, "\n");
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
    rowId = params.get(ROW_ID);
    rowIdOffset = params.getInt(ROW_ID_OFFSET, 0);

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

    adders = new CSVLoaderBase.FieldAdder[fieldnames.length];
    String skipStr = params.get(SKIP);
    List<String> skipFields = skipStr==null ? null : StrUtils.splitSmart(skipStr,',');

    CSVLoaderBase.FieldAdder adder = new CSVLoaderBase.FieldAdder();
    CSVLoaderBase.FieldAdder adderKeepEmpty = new CSVLoaderBase.FieldAdderEmpty();

    for (int i=0; i<fieldnames.length; i++) {
      String fname = fieldnames[i];
      // to skip a field, leave the entries in fields and addrs null
      if (fname.length()==0 || (skipFields!=null && skipFields.contains(fname))) continue;

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
          adders[i] = new CSVLoaderBase.FieldMapperSingle(mapArgs[0], mapArgs[1], adders[i]);
        }
      }
 
      if (params.getFieldBool(fname,TRIM,false)) {
        adders[i] = new CSVLoaderBase.FieldTrimmer(adders[i]);
      }

      if (params.getFieldBool(fname,SPLIT,false)) {
        String sepStr = params.getFieldParam(fname,SEPARATOR);
        char fsep = sepStr==null || sepStr.length()==0 ? ',' : sepStr.charAt(0);
        String encStr = params.getFieldParam(fname,ENCAPSULATOR);
        char fenc = encStr==null || encStr.length()==0 ? (char)-2 : encStr.charAt(0);
        String escStr = params.getFieldParam(fname,ESCAPE);
        char fesc = escStr==null || escStr.length()==0 ? CSVStrategy.ESCAPE_DISABLED : escStr.charAt(0);

        CSVStrategy fstrat = new CSVStrategy
            (fsep, fenc, CSVStrategy.COMMENTS_DISABLED, fesc, false, false, false, false, "\n");
        adders[i] = new CSVLoaderBase.FieldSplitter(fstrat, adders[i]);
      }
    }
    // look for any literal fields - literal.foo=xyzzy
    Iterator<String> paramNames = params.getParameterNamesIterator();
    while (paramNames.hasNext()) {
      String pname = paramNames.next();
      if (!pname.startsWith(LITERALS_PREFIX)) continue;

      String name = pname.substring(LITERALS_PREFIX.length());
      literals.put(name, params.get(pname));
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
  public void load(SolrQueryRequest req, SolrQueryResponse rsp, ContentStream stream, UpdateRequestProcessor processor) throws IOException {
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

        if (vals.length != fieldnames.length) {
          input_err("expected "+fieldnames.length+" values but got "+vals.length, vals, line);
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
  public abstract void addDoc(int line, String[] vals) throws IOException;

  /** this must be MT safe... may be called concurrently from multiple threads. */
  protected void doAdd(int line, String[] vals, SolrInputDocument doc, AddUpdateCommand template) throws IOException {
    // the line number is passed for error reporting in MT mode as well as for optional rowId.
    // first, create the lucene document
    for (int i=0; i<vals.length; i++) {
      if (adders[i]==null) continue;  // skip this field
      String val = vals[i];
      adders[i].add(doc, line, i, val);
    }

    // add any literals
    for (Map.Entry<String, String> entry : literals.entrySet()) {
      doc.addField(entry.getKey(), entry.getValue());
    }
    if (rowId != null){
      doc.addField(rowId, line + rowIdOffset);
    }
    template.solrDoc = doc;
    processor.processAdd(template);
  }
}
