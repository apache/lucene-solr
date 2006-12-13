package org.apache.lucene.gdata.gom.core;

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
import java.io.StringWriter;

import javax.xml.stream.XMLStreamException;

import org.apache.lucene.gdata.gom.GOMAuthor;
import org.apache.lucene.gdata.gom.writer.GOMStaxWriter;
import org.apache.lucene.gdata.gom.writer.GOMOutputWriter;

import junit.framework.TestCase;

/**
 * @author Simon Willnauer
 * 
 */
public class GOMAuthorImplTest extends TestCase {

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMAuthorImpl.writeRssOutput(GOMWriter)'
	 */
	public void testWriteRssOutput() throws XMLStreamException {
		GOMAuthorImpl impl = new GOMAuthorImpl();
		try {
			impl.writeRssOutput(null);
			fail("writer is null");
		} catch (GDataParseException e) {
		}
		StringWriter writer2 = new StringWriter();

		GOMOutputWriter writer = new GOMStaxWriter(writer2);
		impl.writeRssOutput(writer, "test");
		writer.flush();
		writer2.flush();

		assertEquals("<test></test>", writer2.toString());
		impl.setEmail("simonw@apache.org");
		impl.setUri("someuri");
		writer.close();

		writer2 = new StringWriter();
		writer = new GOMStaxWriter(writer2);
		impl.writeRssOutput(writer);
		writer.flush();
		writer2.flush();
		assertTrue(writer2.toString().length() > 0);

		assertEquals("<" + GOMAuthor.LOCALNAME + ">" + impl.getEmail() + "</"
				+ GOMAuthor.LOCALNAME + ">", writer2.toString());

		writer.close();

		impl.setName("simonw");
		writer2 = new StringWriter();
		writer = new GOMStaxWriter(writer2);
		impl.writeRssOutput(writer);
		writer.flush();
		writer2.flush();
		assertTrue(writer2.toString().length() > 0);

		assertEquals("<" + GOMAuthor.LOCALNAME + ">" + impl.getEmail() + "("
				+ impl.getName() + ")</" + GOMAuthor.LOCALNAME + ">", writer2
				.toString());

	}

}
