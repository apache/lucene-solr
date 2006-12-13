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
package org.apache.lucene.gdata.gom;

import java.util.Date;

/**
 * 
 * <P>
 * GOMDateConstruct is a base interface for several concrete DateConstruct
 * implementations like {@link org.apache.lucene.gdata.gom.GOMUpdated} or
 * {@link org.apache.lucene.gdata.gom.GOMPublished}
 * </p>
 * <p>
 * A Date construct is an element whose content MUST conform to the "date-time"
 * production in [RFC3339]. In addition, an uppercase "T" character MUST be used
 * to separate date and time, and an uppercase "Z" character MUST be present in
 * the absence of a numeric time zone offset.
 * 
 * <pre>
 *  atomDateConstruct = atomCommonAttributes, xsd:dateTime
 * </pre>
 * 
 * Such date values happen to be compatible with the following specifications:
 * [ISO.8601.1988], [W3C.NOTE-datetime-19980827], and
 * [W3C.REC-xmlschema-2-20041028].
 * </p>
 * <p>
 * Example Date constructs:
 * 
 * <pre>
 *    &lt;updated&gt;2003-12-13T18:30:02Z&lt;/updated&gt;
 *   
 *    &lt;updated&gt;2003-12-13T18:30:02.25Z&lt;/updated&gt;
 *    &lt;updated&gt;2003-12-13T18:30:02+01:00&lt;/updated&gt;
 *    &lt;updated&gt;2003-12-13T18:30:02.25+01:00&lt;/updated&gt;
 * </pre>
 * 
 * Date values SHOULD be as accurate as possible. For example, it would be
 * generally inappropriate for a publishing system to apply the same timestamp
 * to several entries that were published during the course of a single day.
 * </p>
 * 
 * @author Simon Willnauer
 * 
 * @see org.apache.lucene.gdata.gom.GOMUpdated
 * @see org.apache.lucene.gdata.gom.GOMPublished
 */
public abstract interface GOMDateConstruct extends GOMElement {
	/**
	 * 
	 * @param date -
	 *            the date to set
	 */
	public abstract void setDate(Date date);

	/**
	 * 
	 * @return - the date object, if no date has been set this method will
	 *         return a <code>new Date(0)</code> date object
	 */
	public abstract Date getDate();
}
