package org.apache.solr.solrjs;
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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class ReutersService {
	
	private Map<String, String> countryCodesMap = new HashMap<String, String>();
	
	public static void main(String[] args) throws XPathExpressionException, IOException, ParserConfigurationException, SAXException, SolrServerException, ParseException {
		String usage = "Usage: java -jar reutersimporter.jar <solrUrl> <datadir>";
		
		URL solrUrl = null;
		try {
			solrUrl = new URL(args[0]);
		} catch (Exception e) {
			System.out.println("First argument needs to be an URL!");
			System.out.println(usage);
		}
		File baseDir = null;
		try {
			baseDir = new File(args[1]);
			if (!baseDir.exists() || !baseDir.isDirectory()) {
				System.out.println("Second argument needs to be an existing directory!");;
				System.out.println(usage);
			}
		} catch (Exception e) {
			System.out.println("Second argument needs to be an existing directory!");;
			System.out.println(usage);
		}
		
		if (solrUrl != null && baseDir!= null && baseDir.exists() && baseDir.isDirectory()) {
			ReutersService reutersService = new ReutersService(solrUrl.toExternalForm());
			reutersService.readDirectory(baseDir);
		}
	}

	/**
	 * The Solr Server to use
	 */
	private SolrServer solrServer;
	
	/**
	 * A shared xpath instance
	 */
	private final XPath xPath = XPathFactory.newInstance().newXPath();

	/**
	 * The format used in the sgml files.
	 * eg. 26-FEB-1987 15:01:01.79
	 */                                              
	private final SimpleDateFormat reutersDateFormat = new SimpleDateFormat("dd-MMM-yyyy kk:mm:ss.SS", Locale.ENGLISH);
	
	/**
	 * A service that inputs the reuters TODO dataset.
	 * @param solrUrl The url of the solr server.
	 */
	public ReutersService(String solrUrl) {
		try {
			this.solrServer = new CommonsHttpSolrServer(solrUrl);
			this.solrServer.ping();
		} catch (Exception e) {
			throw new RuntimeException("unable to connect to solr server: " + solrUrl, e );
		} 
	}

	/**
	 * Takes a <REUTERS> node and converts it into a SolrInoutDocument.
	 * @param element A <REUTERS> node.
	 * @throws XPathExpressionException
	 * @throws SolrServerException
	 * @throws IOException
	 * @throws ParseException 
	 */
	public void readDocument(Element element) throws XPathExpressionException, SolrServerException, IOException, ParseException {
		
		SolrInputDocument inputDocument = new SolrInputDocument();
		inputDocument.addField("id", element.getAttribute("NEWID"));
		inputDocument.addField("title", xPath.evaluate("TEXT/TITLE", element));
		inputDocument.addField("dateline", xPath.evaluate("TEXT/DATELINE", element).trim());
		inputDocument.addField("text", xPath.evaluate("TEXT/BODY", element).trim());
		inputDocument.addField("places", readList("PLACES/D", element));
		inputDocument.addField("topics", readList("TOPICS/D", element));
		inputDocument.addField("organisations", readList("ORGS/D", element));
		inputDocument.addField("exchanges", readList("EXCHANGES/D", element));
		inputDocument.addField("companies", readList("COMPANIES/D", element));
		try {
			inputDocument.addField("date", this.reutersDateFormat.parse(xPath.evaluate("DATE", element)));
		} catch (ParseException e) {
			inputDocument.addField("date", this.reutersDateFormat.parse("0" + xPath.evaluate("DATE", element)));
		}
		
		for (Object place : inputDocument.getFieldValues("places")) {
			String code = this.countryCodesMap.get(place);
			if (code == null) {
				try {
					code = getCodeForPlace((String) place);
					this.countryCodesMap.put((String) place, code);
				} catch (SAXException e) {
					e.printStackTrace();
				} catch (ParserConfigurationException e) {
					e.printStackTrace();
				}
			} 
			inputDocument.addField("countryCodes", code);
		}
		
		this.solrServer.add(inputDocument);
		System.out.println(inputDocument.getField("title"));
	}
	
	private String getCodeForPlace(String place) throws MalformedURLException, SAXException, IOException, ParserConfigurationException, XPathExpressionException {
		DocumentBuilderFactory dbfac = DocumentBuilderFactory.newInstance();
		DocumentBuilder docBuilder = dbfac.newDocumentBuilder();
		Document doc = docBuilder.parse(new URL("http://ws.geonames.org/search?q=" + place).openStream());
		return xPath.evaluate("/geonames/geoname/countryCode", doc);
	}
	
	/**
	 * Reads a whole .sgml file.
	 * @param file The sgml reuters file.
	 * @throws XPathExpressionException
	 * @throws IOException
	 * @throws ParserConfigurationException
	 * @throws SAXException
	 * @throws SolrServerException
	 * @throws ParseException 
	 */
	public void readFile(File file) throws XPathExpressionException, IOException, ParserConfigurationException, SAXException, SolrServerException, ParseException {
		String documentString = readFileAsString(file);
		
		// remove "bad" entities
		documentString = documentString.replaceAll("&#\\d;", "");
		documentString = documentString.replaceAll("&#\\d\\d;", "");
		
		// remove doctype declaration
		documentString = documentString.replaceAll("<!DOCTYPE lewis SYSTEM \"lewis.dtd\">", "");
        
		// add a document root
		documentString = "<root>" + documentString + "</root>";
		
		DocumentBuilderFactory dbfac = DocumentBuilderFactory.newInstance();
		DocumentBuilder docBuilder = dbfac.newDocumentBuilder();
		Document doc = docBuilder.parse(new ByteArrayInputStream(documentString.getBytes()));
		
		NodeList nodeList = (NodeList) xPath.evaluate("/root/REUTERS", doc, XPathConstants.NODESET);
		System.out.println("READING FILE: " + file.getAbsoluteFile());
		for (int i=0; i< nodeList.getLength(); i++) {
			System.out.print(" - " + file.getName() + "(" + i + ") ");
			readDocument((Element) nodeList.item(i));
		}
		
		this.solrServer.commit();
		
	}
	
	/**
	 * Reads a whole directory containing reuters .sgml files-
	 * @param directory
	 * @throws XPathExpressionException
	 * @throws IOException
	 * @throws ParserConfigurationException
	 * @throws SAXException
	 * @throws SolrServerException
	 * @throws ParseException 
	 */
    public void readDirectory(File directory) throws XPathExpressionException, IOException, ParserConfigurationException, SAXException, SolrServerException, ParseException {
    	File[] files = directory.listFiles(new FileFilter() {
			public boolean accept(File pathname) {
				if (pathname.getName().contains(".sgm")) {
					return true;
				}
				return false;
			}
    	});
    	if (files.length == 0) {
    		throw new RuntimeException("Directory doesn't contain sgml files!");
    	}
    	for (int i = 0; i < files.length; i++) {
			File file = files[i];
			readFile(file);
		}
	}
	
    /**
     * Helper that converts a listnode into a java list.
     * @param path
     * @param element
     * @return
     * @throws XPathExpressionException
     */
	private List<String> readList(String path, Element element) throws XPathExpressionException {
		List<String> list = new ArrayList<String>();
		NodeList nodeList = (NodeList) xPath.evaluate(path, element, XPathConstants.NODESET);
		for (int i=0; i< nodeList.getLength(); i++) {
			list.add(nodeList.item(i).getTextContent());
		}
		return list;
	}
	
	/**
	 * Helper that reads a file into a string.
	 * @param file
	 * @return
	 * @throws java.io.IOException
	 */
	private static String readFileAsString(File file) throws java.io.IOException {
        StringBuilder fileData = new StringBuilder(1000);
        BufferedReader reader = new BufferedReader(new FileReader(file));
        char[] buf = new char[1024];
        int numRead=0;
        while((numRead=reader.read(buf)) != -1){
            String readData = String.valueOf(buf, 0, numRead);
            fileData.append(readData);
            buf = new char[1024];
        }
        reader.close();
        return fileData.toString();
    }
	
}
