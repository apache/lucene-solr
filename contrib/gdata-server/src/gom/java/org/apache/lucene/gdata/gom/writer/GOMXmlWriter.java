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
package org.apache.lucene.gdata.gom.writer;

import java.io.Writer;
import java.nio.charset.CharsetEncoder;

import javax.xml.stream.XMLStreamException;

import com.bea.xml.stream.XMLWriterBase;

/**
 * @author Simon Willnauer
 * 
 */
public class GOMXmlWriter extends XMLWriterBase {
	private CharsetEncoder encoder;

	/**
	 * 
	 */
	public GOMXmlWriter() {
		super();
	}

	/**
	 * @param arg0
	 */
	public GOMXmlWriter(Writer arg0) {
		super(arg0);
	}

	/**
	 * @see com.bea.xml.stream.XMLWriterBase#writeCharacters(java.lang.String)
	 */
	@Override
	public void writeCharacters(String aString) throws XMLStreamException {
		closeStartElement();
		char[] ch = aString.toCharArray();
		escapeCharacters(ch, 0, ch.length);
	}

	/*
	 * The default implementation escapes all xml chars in the writeCharacters
	 * method. This is not expected for xhtml blobs. To make it easier to write
	 * xhtml blobs the writeCharacters(String) mehtod will be reimplemented for
	 * internal use.
	 */
	private void escapeCharacters(char chars[], int start, int length)
			throws XMLStreamException {
		for (int i = 0; i < length; i++) {
			final char c = chars[i + start];

			if (c < 32) {
				if ((c != '\t' && c != '\n')) {
					write("&#");
					write(Integer.toString(c));
					write(';');
					continue;
				}
			} else if (c > 127 && encoder != null && !encoder.canEncode(c)) {
				write("&#");
				write(Integer.toString(c));
				write(';');
				continue;
			}

			write(c);
		}
	}

}
