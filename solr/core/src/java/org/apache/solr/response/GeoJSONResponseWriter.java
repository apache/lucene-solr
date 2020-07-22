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

import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.index.IndexableField;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.transform.WriteableGeoJSON;
import org.apache.solr.schema.AbstractSpatialFieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.ReturnFields;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.io.ShapeWriter;
import org.locationtech.spatial4j.io.SupportedFormats;
import org.locationtech.spatial4j.shape.Shape;

/**
 * Extend the standard JSONResponseWriter to support GeoJSON.  This writes
 * a {@link SolrDocumentList} with a 'FeatureCollection', following the
 * specification in <a href="http://geojson.org/">geojson.org</a>
 */
public class GeoJSONResponseWriter extends JSONResponseWriter {
  
  public static final String FIELD = "geojson.field";
  
  @Override
  public void write(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {
    
    String geofield = req.getParams().get(FIELD, null);
    if(geofield==null || geofield.length()==0) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "GeoJSON.  Missing parameter: '"+FIELD+"'");
    }
    
    SchemaField sf = req.getSchema().getFieldOrNull(geofield);
    if(sf==null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "GeoJSON.  Unknown field: '"+FIELD+"'="+geofield);
    }
    
    SupportedFormats formats = null;
    if(sf.getType() instanceof AbstractSpatialFieldType) {
      SpatialContext ctx = ((AbstractSpatialFieldType)sf.getType()).getSpatialContext();
      formats = ctx.getFormats();
    }

    JSONWriter w = new GeoJSONWriter(writer, req, rsp, 
        geofield,
        formats); 
    
    try {
      w.writeResponse();
    } finally {
      w.close();
    }
  }
}

class GeoJSONWriter extends JSONWriter {
  
  final SupportedFormats formats;
  final ShapeWriter geowriter;
  final String geofield;
  
  public GeoJSONWriter(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp, 
      String geofield, SupportedFormats formats) {
    super(writer, req, rsp);
    this.geofield = geofield;
    this.formats = formats;
    if(formats==null) {
      this.geowriter = null;
    }
    else {
      this.geowriter = formats.getGeoJsonWriter();
    }
  }

  @Override
  public void writeResponse() throws IOException {
    if(req.getParams().getBool(CommonParams.OMIT_HEADER, false)) {
      if(wrapperFunction!=null) {
          writer.write(wrapperFunction + "(");
      }
      rsp.removeResponseHeader();

      @SuppressWarnings({"unchecked"})
      NamedList<Object> vals = rsp.getValues();
      Object response = vals.remove("response");
      if(vals.size()==0) {
        writeVal(null, response);
      }
      else {
        throw new SolrException(ErrorCode.BAD_REQUEST, 
            "GeoJSON with "+CommonParams.OMIT_HEADER +
            " can not return more than a result set");
      }
      
      if(wrapperFunction!=null) {
        writer.write(')');
      }
      writer.write('\n');  // ending with a newline looks much better from the command line
    }
    else {
      super.writeResponse();
    }
  }
  
  @Override
  public void writeSolrDocument(String name, SolrDocument doc, ReturnFields returnFields, int idx) throws IOException {
    if( idx > 0 ) {
      writeArraySeparator();
    }

    indent();
    writeMapOpener(-1); 
    incLevel();

    writeKey("type", false);
    writeVal(null, "Feature");
    
    Object val = doc.getFieldValue(geofield);
    if(val != null) {  
      writeFeatureGeometry(val);
    }
    
    boolean first=true;
    for (String fname : doc.getFieldNames()) {
      if (fname.equals(geofield) || ((returnFields!= null && !returnFields.wantsField(fname)))) {
        continue;
      }
      writeMapSeparator();
      if (first) {
        indent();
        writeKey("properties", false);
        writeMapOpener(-1); 
        incLevel();
        
        first=false;
      }

      indent();
      writeKey(fname, true);
      val = doc.getFieldValue(fname);
      writeVal(fname, val);
    }

    // GeoJSON does not really support nested FeatureCollections
    if(doc.hasChildDocuments()) {
      if(first == false) {
        writeMapSeparator();
        indent();
      }
      writeKey("_childDocuments_", true);
      writeArrayOpener(doc.getChildDocumentCount());
      List<SolrDocument> childDocs = doc.getChildDocuments();
      for(int i=0; i<childDocs.size(); i++) {
        writeSolrDocument(null, childDocs.get(i), null, i);
      }
      writeArrayCloser();
    }

    // check that we added any properties
    if(!first) {
      decLevel();
      writeMapCloser();
    }
    
    decLevel();
    writeMapCloser();
  }

  protected void writeFeatureGeometry(Object geo) throws IOException 
  {
    // Support multi-valued geometries
    if(geo instanceof Iterable) {
      @SuppressWarnings({"rawtypes"})
      Iterator iter = ((Iterable)geo).iterator();
      if(!iter.hasNext()) {
        return; // empty list
      }
      else {
        geo = iter.next();
        
        // More than value
        if(iter.hasNext()) {
          writeMapSeparator();
          indent();
          writeKey("geometry", false);
          incLevel();

          // TODO: in the future, we can be smart and try to make this the appropriate MULTI* value
          // if all the values are the same
          // { "type": "GeometryCollection",
          //    "geometries": [
          writeMapOpener(-1); 
          writeKey("type",false);
          writeStr(null, "GeometryCollection", false);
          writeMapSeparator();
          writeKey("geometries", false);
          writeArrayOpener(-1); // no trivial way to determine array size
          incLevel();
          
          // The first one
          indent();
          writeGeo(geo);
          while(iter.hasNext()) {
            // Each element in the array
            writeArraySeparator();
            indent();
            writeGeo(iter.next());
          }
          
          decLevel();
          writeArrayCloser();
          writeMapCloser();
          
          decLevel();
          return;
        }
      }
    }
    
    // Single Value
    if(geo!=null) {
      writeMapSeparator();
      indent();
      writeKey("geometry", false);
      writeGeo(geo);
    }
  }

  protected void writeGeo(Object geo) throws IOException {
    Shape shape = null;
    String str = null;
    if(geo instanceof Shape) {
      shape = (Shape)geo;
    }
    else if(geo instanceof IndexableField) {
      str = ((IndexableField)geo).stringValue();
    }
    else if(geo instanceof WriteableGeoJSON) {
      shape = ((WriteableGeoJSON)geo).shape;
    }
    else {
      str = geo.toString();
    }
    
    if(str !=null) {
      // Assume it is well formed JSON
      if(str.startsWith("{") && str.endsWith("}")) {
        writer.write(str);
        return;
      }
      
      if(formats==null) {
        // The check is here and not in the constructor because we do not know if the
        // *stored* values for the field look like JSON until we actually try to read them
        throw new SolrException(ErrorCode.BAD_REQUEST, 
            "GeoJSON unable to write field: '&"+ GeoJSONResponseWriter.FIELD +"="+geofield+"' ("+str+")");
      }
      shape = formats.read(str); 
    }
    
    if(geowriter==null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, 
          "GeoJSON unable to write field: '&"+ GeoJSONResponseWriter.FIELD +"="+geofield+"'");
    }
    
    if(shape!=null) {
      geowriter.write(writer, shape);
    }
  }

  @Deprecated
  @Override
  public void writeStartDocumentList(String name, 
      long start, int size, long numFound, Float maxScore) throws IOException {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public void writeStartDocumentList(String name, 
      long start, int size, long numFound, Float maxScore, Boolean numFoundExact) throws IOException
  {
    writeMapOpener(headerSize(maxScore, numFoundExact));
    incLevel();
    writeKey("type",false);
    writeStr(null, "FeatureCollection", false);
    writeMapSeparator();
    writeKey("numFound",false);
    writeLong(null,numFound);
    writeMapSeparator();
    writeKey("start",false);
    writeLong(null,start);

    if (maxScore!=null) {
      writeMapSeparator();
      writeKey("maxScore",false);
      writeFloat(null,maxScore);
    }
    
    if (numFoundExact != null) {
      writeMapSeparator();
      writeKey("numFoundExact",false);
      writeBool(null, numFoundExact);
    }
    
    writeMapSeparator();
    
    // if can we get bbox of all results, we should write it here
    
    // indent();
    writeKey("features",false);
    writeArrayOpener(size);

    incLevel();
  }

  @Override
  public void writeEndDocumentList() throws IOException
  {
    decLevel();
    writeArrayCloser();

    decLevel();
    indent();
    writeMapCloser();
  }
}