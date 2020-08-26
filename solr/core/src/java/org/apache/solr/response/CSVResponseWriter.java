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
package org.apache.solr.response;

import java.io.CharArrayWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.IndexableField;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.FastWriter;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.internal.csv.CSVPrinter;
import org.apache.solr.internal.csv.CSVStrategy;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrField;
import org.apache.solr.search.ReturnFields;

/**
 * Response writer for csv data
 */

public class CSVResponseWriter implements QueryResponseWriter {

  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList n) {
  }

  @Override
  public void write(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {
    CSVWriter w = new CSVWriter(writer, req, rsp);
    try {
      w.writeResponse();
    } finally {
      w.close();
    }
  }

  @Override
  public String getContentType(SolrQueryRequest request, SolrQueryResponse response) {
    // using the text/plain allows this to be viewed in the browser easily
    return CONTENT_TYPE_TEXT_UTF8;
  }
}


class CSVWriter extends TabularResponseWriter {
  static String SEPARATOR = "separator";
  static String ENCAPSULATOR = "encapsulator";
  static String ESCAPE = "escape";

  static String CSV = "csv.";
  static String CSV_SEPARATOR = CSV + SEPARATOR;
  static String CSV_ENCAPSULATOR = CSV + ENCAPSULATOR;
  static String CSV_ESCAPE = CSV + ESCAPE;

  static String MV = CSV+"mv.";
  static String MV_SEPARATOR = MV + SEPARATOR;
  static String MV_ENCAPSULATOR = MV + ENCAPSULATOR;
  static String MV_ESCAPE = MV + ESCAPE;

  static String CSV_NULL = CSV + "null";
  static String CSV_HEADER = CSV + "header";
  static String CSV_NEWLINE = CSV + "newline";

  char[] sharedCSVBuf = new char[8192];

  // prevent each instance from creating its own buffer
  class CSVSharedBufPrinter extends CSVPrinter {
    public CSVSharedBufPrinter(Writer out, CSVStrategy strategy) {
      super(out, strategy);
      super.buf = sharedCSVBuf;
    }

    public void reset() {
      super.newLine = true;
      // update our shared buf in case a new bigger one was allocated
      sharedCSVBuf = super.buf;
    }
  }

  // allows access to internal buf w/o copying it
  static class OpenCharArrayWriter extends CharArrayWriter {
    public char[]  getInternalBuf() { return buf; }
  }

  // Writes all data to a char array,
  // allows access to internal buffer, and allows fast resetting.
  static class ResettableFastWriter extends FastWriter {
    OpenCharArrayWriter cw = new OpenCharArrayWriter();
    char[] result;
    int resultLen;

    public ResettableFastWriter() {
      super(new OpenCharArrayWriter());
      cw = (OpenCharArrayWriter)sink;
    }

    public void reset() {
      cw.reset();
      pos=0;
    }

    public void freeze() throws IOException {
      if (cw.size() > 0) {
        flush();
        result = cw.getInternalBuf();
        resultLen = cw.size();
      } else {
        result = buf;
        resultLen = pos;
      }
    }

    public int getFrozenSize() { return resultLen; }
    public char[] getFrozenBuf() { return result; }
  }


  static class CSVField {
    String name;
    SchemaField sf;
    CSVSharedBufPrinter mvPrinter;  // printer used to encode multiple values in a single CSV value

    // used to collect values
    List<IndexableField> values = new ArrayList<>(1);  // low starting amount in case there are many fields
    int tmp;
  }

  int pass;
  Map<String,CSVField> csvFields = new LinkedHashMap<>();

  Calendar cal;  // for formatting date objects

  CSVStrategy strategy;  // strategy for encoding the fields of documents
  CSVPrinter printer;
  ResettableFastWriter mvWriter = new ResettableFastWriter();  // writer used for multi-valued fields

  String NullValue;


  public CSVWriter(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp) {
    super(writer, req, rsp);
  }

  public void writeResponse() throws IOException {
    SolrParams params = req.getParams();

    strategy = new CSVStrategy
        (',', '"', CSVStrategy.COMMENTS_DISABLED, CSVStrategy.ESCAPE_DISABLED, false, false, false, true, "\n");
    CSVStrategy strat = strategy;

    String sep = params.get(CSV_SEPARATOR);
    if (sep!=null) {
      if (sep.length()!=1) throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"Invalid separator:'"+sep+"'");
      strat.setDelimiter(sep.charAt(0));
    }

    String nl = params.get(CSV_NEWLINE);
    if (nl!=null) {
      if (nl.length()==0) throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"Invalid newline:'"+nl+"'");
      strat.setPrinterNewline(nl);
    }

    String encapsulator = params.get(CSV_ENCAPSULATOR);
    String escape = params.get(CSV_ESCAPE);
    if (encapsulator!=null) {
      if (encapsulator.length()!=1) throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"Invalid encapsulator:'"+encapsulator+"'");
      strat.setEncapsulator(encapsulator.charAt(0));
    }

    if (escape!=null) {
      if (escape.length()!=1) throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"Invalid escape:'"+escape+"'");
      strat.setEscape(escape.charAt(0));
      if (encapsulator == null) {
        strat.setEncapsulator( CSVStrategy.ENCAPSULATOR_DISABLED);
      }
    }

    if (strat.getEscape() == '\\') {
      // If the escape is the standard backslash, then also enable
      // unicode escapes (it's harmless since 'u' would not otherwise
      // be escaped.
      strat.setUnicodeEscapeInterpretation(true);
    }
    printer = new CSVPrinter(writer, strategy);
    

    CSVStrategy mvStrategy = new CSVStrategy(strategy.getDelimiter(), CSVStrategy.ENCAPSULATOR_DISABLED, 
        CSVStrategy.COMMENTS_DISABLED, '\\', false, false, false, false, "\n");
    strat = mvStrategy;

    sep = params.get(MV_SEPARATOR);
    if (sep!=null) {
      if (sep.length()!=1) throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"Invalid mv separator:'"+sep+"'");
      strat.setDelimiter(sep.charAt(0));
    }

    encapsulator = params.get(MV_ENCAPSULATOR);
    escape = params.get(MV_ESCAPE);

    if (encapsulator!=null) {
      if (encapsulator.length()!=1) throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"Invalid mv encapsulator:'"+encapsulator+"'");
      strat.setEncapsulator(encapsulator.charAt(0));
      if (escape == null) {
        strat.setEscape(CSVStrategy.ESCAPE_DISABLED);
      }
    }

    escape = params.get(MV_ESCAPE);
    if (escape!=null) {
      if (escape.length()!=1) throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"Invalid mv escape:'"+escape+"'");
      strat.setEscape(escape.charAt(0));
      // encapsulator will already be disabled if it wasn't specified
    }

    Collection<String> fields = getFields();
    CSVSharedBufPrinter csvPrinterMV = new CSVSharedBufPrinter(mvWriter, mvStrategy);

    for (String field : fields) {
       if (!returnFields.wantsField(field)) {
         continue;
       }
      if (field.equals("score")) {
        CSVField csvField = new CSVField();
        csvField.name = "score";
        csvFields.put("score", csvField);
        continue;
      }

      if (shouldSkipField(field)) {
        continue;
      }

      SchemaField sf = schema.getFieldOrNull(field);
      if (sf == null) {
        FieldType ft = new StrField();
        sf = new SchemaField(field, ft);
      }

      // check for per-field overrides
      sep = params.get("f." + field + '.' + CSV_SEPARATOR);
      encapsulator = params.get("f." + field + '.' + CSV_ENCAPSULATOR);
      escape = params.get("f." + field + '.' + CSV_ESCAPE);
     
      // if polyfield and no escape is provided, add "\\" escape by default
      if (sf.isPolyField()) {
        escape = (escape==null)?"\\":escape;
      }

      CSVSharedBufPrinter csvPrinter = csvPrinterMV;
      if (sep != null || encapsulator != null || escape != null) {
        // create a new strategy + printer if there were any per-field overrides
        strat = (CSVStrategy)mvStrategy.clone();
        if (sep!=null) {
          if (sep.length()!=1) throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"Invalid mv separator:'"+sep+"'");
          strat.setDelimiter(sep.charAt(0));
        }
        if (encapsulator!=null) {
          if (encapsulator.length()!=1) throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"Invalid mv encapsulator:'"+encapsulator+"'");
          strat.setEncapsulator(encapsulator.charAt(0));
          if (escape == null) {
            strat.setEscape(CSVStrategy.ESCAPE_DISABLED);
          }
        }
        if (escape!=null) {
          if (escape.length()!=1) throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"Invalid mv escape:'"+escape+"'");
          strat.setEscape(escape.charAt(0));
          if (encapsulator == null) {
            strat.setEncapsulator(CSVStrategy.ENCAPSULATOR_DISABLED);
          }
        }        
        csvPrinter = new CSVSharedBufPrinter(mvWriter, strat);
      }

      CSVField csvField = new CSVField();
      csvField.name = field;
      csvField.sf = sf;
      csvField.mvPrinter = csvPrinter;
      csvFields.put(field, csvField);
    }

    NullValue = params.get(CSV_NULL, "");

    if (params.getBool(CSV_HEADER, true)) {
      for (CSVField csvField : csvFields.values()) {
        printer.print(csvField.name);
      }
      printer.println();
    }

    writeResponse(rsp.getResponse());
  }

  @Override
  public void close() throws IOException {
    if (printer != null) printer.flush();
    super.close();
  }

  //NOTE: a document cannot currently contain another document
  @SuppressWarnings({"rawtypes"})
  List tmpList;
  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void writeSolrDocument(String name, SolrDocument doc, ReturnFields returnFields, int idx ) throws IOException {
    if (tmpList == null) {
      tmpList = new ArrayList(1);
      tmpList.add(null);
    }

    for (CSVField csvField : csvFields.values()) {
      Object val = doc.getFieldValue(csvField.name);
      int nVals = val instanceof Collection ? ((Collection)val).size() : (val==null ? 0 : 1);
      if (nVals == 0) {
        writeNull(csvField.name);
        continue;
      }

      if ((csvField.sf != null && csvField.sf.multiValued()) || nVals > 1) {
        Collection values;
        // normalize to a collection
        if (val instanceof Collection) {
          values = (Collection)val;
        } else {
          tmpList.set(0, val);
          values = tmpList;
        }

        mvWriter.reset();
        csvField.mvPrinter.reset();
        // switch the printer to use the multi-valued one
        CSVPrinter tmp = printer;
        printer = csvField.mvPrinter;
        for (Object fval : values) {
          writeVal(csvField.name, fval);
        }
        printer = tmp;  // restore the original printer

        mvWriter.freeze();
        printer.print(mvWriter.getFrozenBuf(), 0, mvWriter.getFrozenSize(), true);

      } else {
        // normalize to first value
        if (val instanceof Collection) {
          Collection values = (Collection)val;
          val = values.iterator().next();
        }
        // if field is polyfield, use the multi-valued printer to apply appropriate escaping
        if (csvField.sf != null && csvField.sf.isPolyField()) {
          mvWriter.reset();
          csvField.mvPrinter.reset();
          CSVPrinter tmp = printer;
          printer = csvField.mvPrinter;
          writeVal(csvField.name, val);
          printer = tmp;
          mvWriter.freeze();
          printer.print(mvWriter.getFrozenBuf(), 0, mvWriter.getFrozenSize(), true);
        } else {
          writeVal(csvField.name, val);
        }
      }
    }

    printer.println();
  }

  @Override
  public void writeStr(String name, String val, boolean needsEscaping) throws IOException {
    printer.print(val, needsEscaping);
  }

  @Override
  public void writeNull(String name) throws IOException {
    printer.print(NullValue);
  }

  @Override
  public void writeInt(String name, String val) throws IOException {
    printer.print(val, false);
  }

  @Override
  public void writeLong(String name, String val) throws IOException {
    printer.print(val, false);
  }

  @Override
  public void writeBool(String name, String val) throws IOException {
    printer.print(val, false);
  }

  @Override
  public void writeFloat(String name, String val) throws IOException {
    printer.print(val, false);
  }

  @Override
  public void writeDouble(String name, String val) throws IOException {
    printer.print(val, false);
  }

  @Override
  public void writeDate(String name, String val) throws IOException {
    printer.print(val, false);
  }
}
