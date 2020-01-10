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
package org.apache.solr.schema;

import java.util.Date;

import org.apache.lucene.index.IndexableField;
import org.apache.solr.update.processor.TimestampUpdateProcessorFactory;
import org.apache.solr.util.DateMathParser;

/**
 * FieldType that can represent any Date/Time with millisecond precision.
 * <p>
 * Date Format for the XML, incoming and outgoing:
 * </p>
 * <blockquote>
 * A date field shall be of the form 1995-12-31T23:59:59Z
 * The trailing "Z" designates UTC time and is mandatory
 * (See below for an explanation of UTC).
 * Optional fractional seconds are allowed, as long as they do not end
 * in a trailing 0 (but any precision beyond milliseconds will be ignored).
 * All other parts are mandatory.
 * </blockquote>
 * <p>
 * This format was derived to be standards compliant (ISO 8601) and is a more
 * restricted form of the
 * <a href="http://www.w3.org/TR/xmlschema-2/#dateTime-canonical-representation">canonical
 * representation of dateTime</a> from XML schema part 2.  Examples...
 * </p>
 * <ul>
 *   <li>1995-12-31T23:59:59Z</li>
 *   <li>1995-12-31T23:59:59.9Z</li>
 *   <li>1995-12-31T23:59:59.99Z</li>
 *   <li>1995-12-31T23:59:59.999Z</li>
 * </ul>
 * <p>
 * Note that TrieDateField is lenient with regards to parsing fractional
 * seconds that end in trailing zeros and will ensure that those values
 * are indexed in the correct canonical format.
 * </p>
 * <p>
 * This FieldType also supports incoming "Date Math" strings for computing
 * values by adding/rounding internals of time relative either an explicit
 * datetime (in the format specified above) or the literal string "NOW",
 * ie: "NOW+1YEAR", "NOW/DAY", "1995-12-31T23:59:59.999Z+5MINUTES", etc...
 * -- see {@link DateMathParser} for more examples.
 * </p>
 * <p>
 * <b>NOTE:</b> Although it is possible to configure a <code>TrieDateField</code>
 * instance with a default value of "<code>NOW</code>" to compute a timestamp
 * of when the document was indexed, this is not advisable when using SolrCloud
 * since each replica of the document may compute a slightly different value.
 * {@link TimestampUpdateProcessorFactory} is recommended instead.
 * </p>
 *
 * <p>
 * Explanation of "UTC"...
 * </p>
 * <blockquote>
 * "In 1970 the Coordinated Universal Time system was devised by an
 * international advisory group of technical experts within the International
 * Telecommunication Union (ITU).  The ITU felt it was best to designate a
 * single abbreviation for use in all languages in order to minimize
 * confusion.  Since unanimous agreement could not be achieved on using
 * either the English word order, CUT, or the French word order, TUC, the
 * acronym UTC was chosen as a compromise."
 * </blockquote>
 *
 * @see TrieField
 * @deprecated Trie fields are deprecated as of Solr 7.0
 */
@Deprecated
public class TrieDateField extends TrieField implements DateValueFieldType {
  {
    this.type = NumberType.DATE;
  }
  
  @Override
  public Date toObject(IndexableField f) {
    return (Date)super.toObject(f);
  }

  @Override
  public Object toNativeType(Object val) {
    if (val instanceof CharSequence) {
      return DateMathParser.parseMath(null, val.toString());
    }
    return super.toNativeType(val);
  }
}
