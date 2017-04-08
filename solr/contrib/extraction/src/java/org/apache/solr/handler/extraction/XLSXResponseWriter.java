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
package org.apache.solr.handler.extraction;

import java.io.CharArrayWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.lucene.index.IndexableField;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.BasicResultContext;
import org.apache.solr.response.RawResponseWriter;
import org.apache.solr.response.ResultContext;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrField;
import org.apache.solr.search.DocList;
import org.apache.solr.search.ReturnFields;

public class XLSXResponseWriter extends RawResponseWriter {

  @Override
  public void write(OutputStream out, SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {
    // throw away arraywriter just to satisfy super requirements; we're grabbing
    // all writes before they go to it anyway
    XLSXWriter w = new XLSXWriter(new CharArrayWriter(), req, rsp);

    LinkedHashMap<String,String> reqNamesMap = new LinkedHashMap<>();
    LinkedHashMap<String,Integer> reqWidthsMap = new LinkedHashMap<>();

    Iterator<String> paramNamesIter = req.getParams().getParameterNamesIterator();
    while (paramNamesIter.hasNext()) {
      String nextParam = paramNamesIter.next();
      if (nextParam.startsWith("colname.")) {
        String field = nextParam.substring("colname.".length());
        reqNamesMap.put(field, req.getParams().get(nextParam));
      } else if (nextParam.startsWith("colwidth.")) {
        String field = nextParam.substring("colwidth.".length());
        reqWidthsMap.put(field, req.getParams().getInt(nextParam));
      }
    }

    try {
      w.writeResponse(out, reqNamesMap, reqWidthsMap);
    } finally {
      w.close();
    }
  }

  @Override
  public String getContentType(SolrQueryRequest request, SolrQueryResponse response) {
    return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
  }
}

class XLSXWriter extends TextResponseWriter {

  SolrQueryRequest req;
  SolrQueryResponse rsp;

  static class SerialWriteWorkbook {
    SXSSFWorkbook swb;
    Sheet sh;

    XSSFCellStyle headerStyle;
    int rowIndex;
    Row curRow;
    int cellIndex;

    SerialWriteWorkbook() {
      this.swb = new SXSSFWorkbook(100);
      this.sh = this.swb.createSheet();

      this.rowIndex = 0;

      this.headerStyle = (XSSFCellStyle)swb.createCellStyle();
      this.headerStyle.setFillBackgroundColor(IndexedColors.BLACK.getIndex());
      //solid fill
      this.headerStyle.setFillPattern((short)1);
      Font headerFont = swb.createFont();
      headerFont.setFontHeightInPoints((short)14);
      headerFont.setBoldweight(Font.BOLDWEIGHT_BOLD);
      headerFont.setColor(IndexedColors.WHITE.getIndex());
      this.headerStyle.setFont(headerFont);
    }

    void addRow() {
      curRow = sh.createRow(rowIndex++);
      cellIndex = 0;
    }

    void setHeaderRow() {
      curRow.setHeightInPoints((short)21);
    }

    //sets last created cell to have header style
    void setHeaderCell() {
      curRow.getCell(cellIndex - 1).setCellStyle(this.headerStyle);
    }

    //set the width of the most recently created column
    void setColWidth(int charWidth) {
      //width in poi is units of 1/256th of a character width for some reason
      this.sh.setColumnWidth(cellIndex - 1, 256*charWidth);
    }

    void writeCell(String value) {
      Cell cell = curRow.createCell(cellIndex++);
      cell.setCellValue(value);
    }

    void flush(OutputStream out) {
      try {
        swb.write(out);
      } catch (IOException e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        String stacktrace = sw.toString();
      }finally {
        swb.dispose();
      }
    }
  }

  private SerialWriteWorkbook wb = new SerialWriteWorkbook();

  static class XLField {
    String name;
    SchemaField sf;
  }

  private Map<String,XLField> xlFields = new LinkedHashMap<String,XLField>();

  public XLSXWriter(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp){
    super(writer, req, rsp);
    this.req = req;
    this.rsp = rsp;
  }

  public void writeResponse(OutputStream out, LinkedHashMap<String, String> colNamesMap,
                            LinkedHashMap<String, Integer> colWidthsMap) throws IOException {
    SolrParams params = req.getParams();

    Collection<String> fields = returnFields.getRequestedFieldNames();
    Object responseObj = rsp.getValues().get("response");
    boolean returnOnlyStored = false;
    if (fields==null||returnFields.hasPatternMatching()) {
      if (responseObj instanceof SolrDocumentList) {
        // get the list of fields from the SolrDocumentList
        if(fields==null) {
          fields = new LinkedHashSet<String>();
        }
        for (SolrDocument sdoc: (SolrDocumentList)responseObj) {
          fields.addAll(sdoc.getFieldNames());
        }
      } else {
        // get the list of fields from the index
        Iterable<String> all = req.getSearcher().getFieldNames();
        if (fields == null) {
          fields = Sets.newHashSet(all);
        } else {
          Iterables.addAll(fields, all);
        }
      }
      if (returnFields.wantsScore()) {
        fields.add("score");
      } else {
        fields.remove("score");
      }
      returnOnlyStored = true;
    }

    for (String field : fields) {
      if (!returnFields.wantsField(field)) {
        continue;
      }
      if (field.equals("score")) {
        XLField xlField = new XLField();
        xlField.name = "score";
        xlFields.put("score", xlField);
        continue;
      }

      SchemaField sf = schema.getFieldOrNull(field);
      if (sf == null) {
        FieldType ft = new StrField();
        sf = new SchemaField(field, ft);
      }

      // Return only stored fields, unless an explicit field list is specified
      if (returnOnlyStored && sf != null && !sf.stored()) {
        continue;
      }

      XLField xlField = new XLField();
      xlField.name = field;
      xlField.sf = sf;
      xlFields.put(field, xlField);
    }



    wb.addRow();
    //write header
    for (XLField xlField : xlFields.values()) {
      String printName = xlField.name;
      int colWidth = 14;

      String niceName = colNamesMap.get(xlField.name);
      if (niceName != null) {
        printName = niceName;
      }

      Integer niceWidth = colWidthsMap.get(xlField.name);
      if (niceWidth != null) {
        colWidth = niceWidth.intValue();
      }

      writeStr(xlField.name, printName, false);
      wb.setColWidth(colWidth);
      wb.setHeaderCell();
    }
    wb.setHeaderRow();
    wb.addRow();

    if (responseObj instanceof ResultContext) {
      writeDocuments(null, (ResultContext)responseObj );
    }
    else if (responseObj instanceof DocList) {
      ResultContext ctx = new BasicResultContext((DocList)responseObj, returnFields, null, null, req);
      writeDocuments(null, ctx );
    } else if (responseObj instanceof SolrDocumentList) {
      writeSolrDocumentList(null, (SolrDocumentList)responseObj, returnFields );
    }

    wb.flush(out);
    wb = null;
  }

  @Override
  public void close() throws IOException {
    super.close();
  }

  @Override
  public void writeNamedList(String name, NamedList val) throws IOException {
  }

  @Override
  public void writeStartDocumentList(String name,
                                     long start, int size, long numFound, Float maxScore) throws IOException
  {
    // nothing
  }

  @Override
  public void writeEndDocumentList() throws IOException
  {
    // nothing
  }

  //NOTE: a document cannot currently contain another document
  List tmpList;
  @Override
  public void writeSolrDocument(String name, SolrDocument doc, ReturnFields returnFields, int idx ) throws IOException {
    if (tmpList == null) {
      tmpList = new ArrayList(1);
      tmpList.add(null);
    }

    for (XLField xlField : xlFields.values()) {
      Object val = doc.getFieldValue(xlField.name);
      int nVals = val instanceof Collection ? ((Collection)val).size() : (val==null ? 0 : 1);
      if (nVals == 0) {
        writeNull(xlField.name);
        continue;
      }

      if ((xlField.sf != null && xlField.sf.multiValued()) || nVals > 1) {
        Collection values;
        // normalize to a collection
        if (val instanceof Collection) {
          values = (Collection)val;
        } else {
          tmpList.set(0, val);
          values = tmpList;
        }

        writeArray(xlField.name, values.iterator());

      } else {
        // normalize to first value
        if (val instanceof Collection) {
          Collection values = (Collection)val;
          val = values.iterator().next();
        }
        writeVal(xlField.name, val);
      }
    }
    wb.addRow();
  }

  @Override
  public void writeStr(String name, String val, boolean needsEscaping) throws IOException {
    wb.writeCell(val);
  }

  @Override
  public void writeMap(String name, Map val, boolean excludeOuter, boolean isFirstVal) throws IOException {
  }

  @Override
  public void writeArray(String name, Iterator val) throws IOException {
    StringBuffer output = new StringBuffer();
    while (val.hasNext()) {
      Object v = val.next();
      if (v instanceof IndexableField) {
        IndexableField f = (IndexableField)v;
        if (v instanceof Date) {
          output.append(((Date) val).toInstant().toString() + "; ");
        } else {
          output.append(f.stringValue() + "; ");
        }
      } else {
        output.append(v.toString() + "; ");
      }
    }
    if (output.length() > 0) {
      output.deleteCharAt(output.length()-1);
      output.deleteCharAt(output.length()-1);
    }
    writeStr(name, output.toString(), false);
  }

  @Override
  public void writeNull(String name) throws IOException {
    wb.writeCell("");
  }

  @Override
  public void writeInt(String name, String val) throws IOException {
    wb.writeCell(val);
  }

  @Override
  public void writeLong(String name, String val) throws IOException {
    wb.writeCell(val);
  }

  @Override
  public void writeBool(String name, String val) throws IOException {
    wb.writeCell(val);
  }

  @Override
  public void writeFloat(String name, String val) throws IOException {
    wb.writeCell(val);
  }

  @Override
  public void writeDouble(String name, String val) throws IOException {
    wb.writeCell(val);
  }

  @Override
  public void writeDate(String name, Date val) throws IOException {
    writeDate(name, val.toInstant().toString());
  }

  @Override
  public void writeDate(String name, String val) throws IOException {
    wb.writeCell(val);
  }
}