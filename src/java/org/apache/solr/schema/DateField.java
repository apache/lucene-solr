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

import org.apache.solr.core.SolrException;
import org.apache.solr.request.XMLWriter;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.function.ValueSource;
import org.apache.lucene.search.function.OrdFieldSource;

import java.util.Map;
import java.io.IOException;

/***
Date Format for the XML, incoming and outgoing:

A date field shall be of the form 1995-12-31T23:59:59Z
The trailing "Z" designates UTC time and is mandatory.
Optional fractional seconds are allowed: 1995-12-31T23:59:59.999Z
All other parts are mandatory.

This format was derived to be standards compliant (ISO 8601) and is a more
restricted form of the canonical representation of dateTime from XML schema part 2.
http://www.w3.org/TR/xmlschema-2/#dateTime

"In 1970 the Coordinated Universal Time system was devised by an international
advisory group of technical experts within the International Telecommunication
Union (ITU).  The ITU felt it was best to designate a single abbreviation for
use in all languages in order to minimize confusion.  Since unanimous agreement
could not be achieved on using either the English word order, CUT, or the
French word order, TUC, the acronym UTC was chosen as a compromise."
***/

// The XML (external) date format will sort correctly, except if
// fractions of seconds are present (because '.' is lower than 'Z').
// The easiest fix is to simply remove the 'Z' for the internal
// format.

// TODO: make a FlexibleDateField that can accept dates in multiple
// formats, better for human entered dates.

// TODO: make a DayField that only stores the day?

/**
 * @author yonik
 * @version $Id$
 */
public class DateField extends FieldType {
  protected void init(IndexSchema schema, Map<String,String> args) {
  }

  public String toInternal(String val) {
    int len=val.length();
    if (val.charAt(len-1)=='Z') {
      return val.substring(0,len-1);
    }
    throw new SolrException(1,"Invalid Date String:'" +val+'\'');
  }

  public String indexedToReadable(String indexedForm) {
    return indexedForm + 'Z';
  }

  public String toExternal(Field f) {
    return indexedToReadable(f.stringValue());
  }

  public SortField getSortField(SchemaField field,boolean reverse) {
    return getStringSort(field,reverse);
  }

  public ValueSource getValueSource(SchemaField field) {
    return new OrdFieldSource(field.name);
  }

  public void write(XMLWriter xmlWriter, String name, Field f) throws IOException {
    xmlWriter.writeDate(name, toExternal(f));
  }
}
