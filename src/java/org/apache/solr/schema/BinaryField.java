package org.apache.solr.schema;

import org.apache.solr.request.XMLWriter;
import org.apache.solr.request.TextResponseWriter;
import org.apache.solr.common.util.Base64;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.SortField;

import java.io.IOException;
import java.nio.ByteBuffer;


public class BinaryField extends FieldType  {

  public void write(XMLWriter xmlWriter, String name, Fieldable f) throws IOException {
    xmlWriter.writeStr( name, toBase64String(toObject(f)) );
  }

  private String  toBase64String(ByteBuffer buf) {
    return Base64.byteArrayToBase64(buf.array(), buf.position(), buf.limit()-buf.position());
  }

  public void write(TextResponseWriter writer, String name, Fieldable f) throws IOException {
    writer.writeStr(name, toBase64String(toObject(f)), false);
  }

  public SortField getSortField(SchemaField field, boolean top) {
    throw new RuntimeException("Cannot sort on a Binary field");
  }


  public String toExternal(Fieldable f) {
    return toBase64String(toObject(f));
  }
  
  public ByteBuffer toObject(Fieldable f) {
    return  ByteBuffer.wrap(f.getBinaryValue(), f.getBinaryOffset(), f.getBinaryLength() ) ;
  }

  public Field createField(SchemaField field, Object val, float boost) {
    if (val == null) return null;
    if (!field.stored()) {
      log.trace("Ignoring unstored binary field: " + field);
      return null;
    }
    byte[] buf = null;
    int offset = 0, len = 0;
    if (val instanceof byte[]) {
      buf = (byte[]) val;
      len = buf.length;
    } else if (val instanceof ByteBuffer && ((ByteBuffer)val).hasArray()) {
      ByteBuffer byteBuf = (ByteBuffer) val;
      buf = byteBuf.array();
      offset = byteBuf.position();
      len = byteBuf.limit() - byteBuf.position();
    } else {
      String strVal = val.toString();
      //the string has to be a base64 encoded string
      buf = Base64.base64ToByteArray(strVal);
      offset = 0;
      len = buf.length;
    }

    Field f = new Field(field.getName(), buf, offset, len,
            getFieldStore(field, null));
    f.setBoost(boost);
    return f;
  }
}
