package org.apache.solr.schema;

import org.apache.lucene.document.Fieldable;
import org.apache.lucene.search.SortField;

import org.apache.solr.request.TextResponseWriter;
import org.apache.solr.request.XMLWriter;
import org.apache.solr.search.function.ValueSource;
import org.apache.solr.search.function.ShortFieldSource;

import java.io.IOException;
import java.util.Map;


/**
 *
 *
 **/
public class ShortField extends FieldType {
  protected void init(IndexSchema schema, Map<String, String> args) {
    restrictProps(SORT_MISSING_FIRST | SORT_MISSING_LAST);
  }

  /////////////////////////////////////////////////////////////

  public SortField getSortField(SchemaField field, boolean reverse) {

    return new SortField(field.name, SortField.SHORT, reverse);
  }

  public ValueSource getValueSource(SchemaField field) {

    return new ShortFieldSource(field.name);
  }


  public void write(XMLWriter xmlWriter, String name, Fieldable f) throws IOException {
    xmlWriter.writeShort(name, f.stringValue());
  }

  @Override
  public void write(TextResponseWriter writer, String name, Fieldable f) throws IOException {
    writer.writeShort(name, f.stringValue());
  }

  @Override
  public Short toObject(Fieldable f) {
    return Short.valueOf(toExternal(f));
  }

}
