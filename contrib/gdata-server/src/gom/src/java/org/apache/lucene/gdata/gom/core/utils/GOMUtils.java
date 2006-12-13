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
package org.apache.lucene.gdata.gom.core.utils;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.gdata.gom.ContentType;
import org.apache.lucene.gdata.gom.GOMAttribute;
import org.apache.lucene.gdata.gom.GOMLink;
import org.apache.lucene.gdata.gom.GOMNamespace;
import org.apache.lucene.gdata.gom.core.GDataParseException;
import org.apache.lucene.gdata.gom.core.GOMAttributeImpl;

/**
 * @author Simon Willnauer
 * 
 */
public class GOMUtils {

	/*
	 * Possible values 2003-12-13T18:30:02Z 2003-12-13T18:30:02.25Z
	 * 2003-12-13T18:30:02+01:00 2003-12-13T18:30:02.25+01:00
	 */
	private static final Pattern RFC3339_DATE_PATTERN = Pattern.compile(
			"(\\d\\d\\d\\d)" + // #YEAR
					"\\-(\\d\\d)" + // #MONTH
					"\\-(\\d\\d)[Tt]" + // #DAY
					"(\\d\\d)" + // #HOURS
					":(\\d\\d)" + // #MINUTES
					":(\\d\\d)" + // #SECONDS
					"(\\.(\\d+))?" + // #MILLISorless
					"([Zz]|((\\+|\\-)(\\d\\d):(\\d\\d)))?"// #TIMEZONESHIFT
			, Pattern.COMMENTS);

	static final String ATTRIBUTE_TYPE = "type";

	static final GOMAttribute TEXT_TYPE;

	static final GOMAttribute HTML_TYPE;

	static final GOMAttribute XHTML_TYPE;

	static final GOMAttribute TEXT_TYPE_DEFAULT_NS;

	static final GOMAttribute HTML_TYPE_DEFAULT_NS;

	static final GOMAttribute XHTML_TYPE_DEFAULT_NS;

	static {
		TEXT_TYPE = new GOMAttributeImpl(GOMNamespace.ATOM_NS_URI,
				GOMNamespace.ATOM_NS_PREFIX, ATTRIBUTE_TYPE, ContentType.TEXT
						.name().toLowerCase());
		HTML_TYPE = new GOMAttributeImpl(GOMNamespace.ATOM_NS_URI,
				GOMNamespace.ATOM_NS_PREFIX, ATTRIBUTE_TYPE, ContentType.HTML
						.name().toLowerCase());
		XHTML_TYPE = new GOMAttributeImpl(GOMNamespace.ATOM_NS_URI,
				GOMNamespace.ATOM_NS_PREFIX, ATTRIBUTE_TYPE, ContentType.XHTML
						.name().toLowerCase());

		TEXT_TYPE_DEFAULT_NS = new GOMAttributeImpl(ATTRIBUTE_TYPE,
				ContentType.TEXT.name().toLowerCase());
		HTML_TYPE_DEFAULT_NS = new GOMAttributeImpl(ATTRIBUTE_TYPE,
				ContentType.HTML.name().toLowerCase());
		XHTML_TYPE_DEFAULT_NS = new GOMAttributeImpl(ATTRIBUTE_TYPE,
				ContentType.XHTML.name().toLowerCase());

	}

	/**
	 * @param type
	 * @return
	 */
	public static GOMAttribute getAttributeByContentType(ContentType type) {
		switch (type) {
		case HTML:
			return HTML_TYPE;
		case XHTML:
			return XHTML_TYPE;

		default:
			return TEXT_TYPE;
		}

	}

	/**
	 * @param type
	 * @return
	 */
	public static GOMAttribute getAttributeByContentTypeDefaultNs(
			ContentType type) {
		if (type == null)
			return TEXT_TYPE_DEFAULT_NS;
		switch (type) {
		case HTML:
			return HTML_TYPE_DEFAULT_NS;
		case XHTML:
			return XHTML_TYPE_DEFAULT_NS;

		default:
			return TEXT_TYPE_DEFAULT_NS;
		}

	}

	/**
	 * Builds a atom namespace attribute
	 * 
	 * @param aValue
	 *            attribute value
	 * @param aName
	 *            attribute name
	 * @return a GOMAttribute
	 */
	public static GOMAttribute buildAtomAttribute(String aValue, String aName) {
		return new GOMAttributeImpl(GOMNamespace.ATOM_NS_URI,
				GOMNamespace.ATOM_NS_PREFIX, aName, aValue);
	}

	/**
	 * @param aValue
	 * @param aName
	 * @return
	 */
	public static GOMAttribute buildDefaultNamespaceAttribute(String aValue,
			String aName) {
		return new GOMAttributeImpl(aName, aValue);
	}

	/**
	 * @param aValue
	 * @param aName
	 * @return
	 */
	public static GOMAttribute buildXMLNamespaceAttribute(String aValue,
			String aName) {
		return new GOMAttributeImpl(GOMNamespace.XML_NS_URI,
				GOMNamespace.XML_NS_PREFIX, aName, aValue);
	}

	/**
	 * @param aString
	 * @return
	 */
	public static boolean isRfc3339DateFormat(String aString) {
		Matcher aMatcher = RFC3339_DATE_PATTERN.matcher(aString);
		return aMatcher.matches();
	}

	/**
	 * @param aString
	 * @return
	 */
	public static long parseRfc3339DateFormat(String aString) {
		if (aString == null)
			throw new IllegalArgumentException(
					"Date-Time String must not be null");
		Matcher aMatcher = RFC3339_DATE_PATTERN.matcher(aString);

		if (!aMatcher.matches()) {
			throw new GDataParseException(
					"Invalid RFC3339 date / time pattern -- " + aString);
		}
		int grCount = aMatcher.groupCount();
		if (grCount > 13)
			throw new GDataParseException(
					"Invalid RFC3339 date / time pattern -- " + aString);

		Integer timeZoneShift = null;
		Calendar dateTime = null;
		try {

			if (aMatcher.group(9) == null) {
				// skip time zone
			} else if (aMatcher.group(9).equalsIgnoreCase("Z")) {
				timeZoneShift = new Integer(0);
			} else {
				timeZoneShift = new Integer((Integer
						.valueOf(aMatcher.group(12)) * 60 + Integer
						.valueOf(aMatcher.group(13))));
				if (aMatcher.group(11).equals("-")) {
					timeZoneShift = new Integer(-timeZoneShift.intValue());
				}
			}

			dateTime = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
			;
			dateTime.clear();
			dateTime.set(Integer.valueOf(aMatcher.group(1)), Integer
					.valueOf(aMatcher.group(2)) - 1, Integer.valueOf(aMatcher
					.group(3)), Integer.valueOf(aMatcher.group(4)), Integer
					.valueOf(aMatcher.group(5)), Integer.valueOf(aMatcher
					.group(6)));
			// seconds with milliseconds
			if (aMatcher.group(8) != null && aMatcher.group(8).length() > 0) {

				dateTime.set(Calendar.MILLISECOND, new BigDecimal("0."/*
																		 * use
																		 * big
																		 * dec
																		 * this
																		 * could
																		 * be
																		 * big!!
																		 */
						+ aMatcher.group(8)).movePointRight(3).intValue());
			}
		} catch (NumberFormatException e) {
			throw new GDataParseException(
					"Invalid RFC3339 date / time pattern -- " + aString, e);
		}

		long retVal = dateTime.getTimeInMillis();
		if (timeZoneShift != null) {
			retVal -= timeZoneShift.intValue() * 60000;
		}

		return retVal;
	}

	/**
	 * @param aMillisecondLong
	 * @return
	 */
	public static String buildRfc3339DateFormat(long aMillisecondLong) {
		Calendar instance = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		instance.setTimeInMillis(aMillisecondLong);

		StringBuilder builder = new StringBuilder();
		// 2003-12-13T18:30:02.25+01:00
		int time = 0;
		time = instance.get(Calendar.YEAR);
		if (time < 1000)
			builder.append("0");
		if (time < 100)
			builder.append("0");
		if (time < 10)
			builder.append("0");
		builder.append(time);
		builder.append('-');
		time = instance.get(Calendar.MONTH);
		if (time < 10)
			builder.append(0);
		builder.append(time);
		builder.append('-');
		time = instance.get(Calendar.DAY_OF_MONTH);
		if (time < 10)
			builder.append(0);
		builder.append(time);
		builder.append('T');
		time = instance.get(Calendar.HOUR_OF_DAY);
		if (time < 10)
			builder.append(0);
		builder.append(time);
		builder.append(':');
		time = instance.get(Calendar.MINUTE);
		if (time < 10)
			builder.append(0);
		builder.append(time);
		builder.append(':');
		time = instance.get(Calendar.SECOND);
		if (time < 10)
			builder.append(0);
		builder.append(time);
		builder.append('.');
		builder.append(instance.get(Calendar.MILLISECOND));
		// this is always GMT offset -> 0
		builder.append('Z');

		return builder.toString();
	}

	/**
	 * @param aMillisecondLong
	 * @return
	 */
	public static String buildRfc822Date(long aMillisecondLong) {
		/*
		 * Rather implement it for a special case as use SDF. SDF is very
		 * expensive to create and not thread safe so it should be synchronized
		 * of pooled
		 */
		Calendar instance = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		instance.setTimeInMillis(aMillisecondLong);

		StringBuilder builder = new StringBuilder();
		// Sun, 06 Aug 2006 00:53:49 +0000
		// EEE, dd MMM yyyy HH:mm:ss Z

		switch (instance.get(Calendar.DAY_OF_WEEK)) {
		case Calendar.SUNDAY:
			builder.append("Sun");
			break;
		case Calendar.MONDAY:
			builder.append("Mon");
			break;
		case Calendar.TUESDAY:
			builder.append("Tue");
			break;
		case Calendar.WEDNESDAY:
			builder.append("Wed");
			break;
		case Calendar.THURSDAY:
			builder.append("Thu");
			break;
		case Calendar.FRIDAY:
			builder.append("Fri");
			break;
		case Calendar.SATURDAY:
			builder.append("Sat");
			break;
		default:
			break;
		}
		builder.append(',');
		builder.append(' ');

		int time = 0;
		time = instance.get(Calendar.DAY_OF_MONTH);
		if (time < 10)
			builder.append(0);
		builder.append(time);
		builder.append(' ');
		switch (instance.get(Calendar.MONTH)) {
		case Calendar.JANUARY:
			builder.append("Jan");
			break;
		case Calendar.FEBRUARY:
			builder.append("Feb");
			break;
		case Calendar.MARCH:
			builder.append("Mar");
			break;
		case Calendar.APRIL:
			builder.append("Apr");
			break;
		case Calendar.MAY:
			builder.append("May");
			break;
		case Calendar.JUNE:
			builder.append("Jun");
			break;
		case Calendar.JULY:
			builder.append("Jul");
			break;
		case Calendar.AUGUST:
			builder.append("Aug");
			break;
		case Calendar.SEPTEMBER:
			builder.append("Sep");
			break;
		case Calendar.OCTOBER:
			builder.append("Oct");
			break;
		case Calendar.NOVEMBER:
			builder.append("Nov");
			break;
		case Calendar.DECEMBER:
			builder.append("Dec");
			break;

		default:
			break;
		}
		builder.append(' ');
		time = instance.get(Calendar.YEAR);
		if (time < 1000)
			builder.append("0");
		if (time < 100)
			builder.append("0");
		if (time < 10)
			builder.append("0");
		builder.append(time);
		builder.append(' ');

		time = instance.get(Calendar.HOUR_OF_DAY);
		if (time < 10)
			builder.append(0);
		builder.append(time);
		builder.append(':');
		time = instance.get(Calendar.MINUTE);
		if (time < 10)
			builder.append(0);
		builder.append(time);
		builder.append(':');
		time = instance.get(Calendar.SECOND);
		if (time < 10)
			builder.append(0);
		builder.append(time);

		// this is always GMT offset -> 0
		builder.append(" +0000");
		return builder.toString();
	}

	public GOMLink getHtmlLink(List<GOMLink> links) {
		for (GOMLink link : links) {
			if ((link.getRel() == null || link.getRel().equals("alternate"))
					&& (link.getType() == null || link.getType()
							.equalsIgnoreCase("html")))
				return link;
		}
		return null;
	}
}
