<?xml version="1.0" encoding="UTF-8"?>
<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
<xsl:stylesheet version="1.0" 
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:str="http://exslt.org/strings"
  extension-element-prefixes="str"
>
  <xsl:param name="buildfiles"/>
  <xsl:param name="version"/>
  <xsl:param name="luceneJavadocUrl"/>
  <xsl:param name="solrGuideVersion"/>
  
  <!--
    NOTE: This template matches the root element of any given input XML document!
    The XSL input file is ignored completely, but XSL expects one to be given,
    so build.xml passes itself here. The list of module build.xmls is given via
    string parameter, that must be splitted at '|'.
  --> 
  <xsl:template match="/">
    <html>
      <head>
        <title><xsl:text>Apache Solr </xsl:text><xsl:value-of select="$version"/><xsl:text> Documentation</xsl:text></title>
        <link rel="icon" type="image/x-icon" href="images/favicon.ico"/>
        <link rel="shortcut icon" type="image/x-icon" href="images/favicon.ico"/>
      </head>
      <body>
        <div>
          <a href="http://lucene.apache.org/solr/">
            <img src="images/solr.svg" style="width:210px; margin:22px 0px 7px 20px; border:none;" title="Apache Solr Logo" alt="Solr" />
          </a>
          <div style="z-index:100;position:absolute;top:25px;left:226px">
            <span style="font-size: x-small">TM</span>
          </div>
        </div>
        <h1>
          <xsl:text>Apache Solr</xsl:text>
          <span style="vertical-align: top; font-size: x-small">
            <xsl:text>TM</xsl:text>
          </span>
          <xsl:text> </xsl:text>
          <xsl:value-of select="$version"/>
          <xsl:text> Documentation</xsl:text>
        </h1>
        <p>Solr is the popular, blazing fast, open source NoSQL search platform from the Apache Lucene project. Its major 
          features include powerful full-text search, hit highlighting, faceted search and analytics, rich document 
          parsing, geospatial search, extensive REST APIs as well as parallel SQL. Solr is enterprise grade, secure and 
          highly scalable, providing fault tolerant distributed search and indexing, and powers the search and navigation 
          features of many of the world's largest internet sites.</p>
        <p>Solr is written in Java and runs as a standalone full-text search server. Solr uses the Lucene Java search 
          library at its core for full-text indexing and search, and has REST-like JSON APIs that make it easy to use 
          from virtually any programming language. Solr's powerful configuration APIs and files allows it to be tailored 
          to almost any type of application without Java coding, and it has an extensive plugin architecture when more 
          advanced customization is required.</p>
        <p>
          This is the official documentation for <b><xsl:text>Apache Solr </xsl:text>
          <xsl:value-of select="$version"/></b>.
        </p>
        <h2>Reference Documents</h2>
          <ul>
            <li><a href="https://lucene.apache.org/solr/guide/{$solrGuideVersion}/">Reference Guide</a>: The main documentation for Solr</li>
            <li><a href="changes/Changes.html">Changes</a>: List of changes in this release.</li>
            <li><a href="SYSTEM_REQUIREMENTS.html">System Requirements</a>: Minimum and supported Java versions.</li>
            <li><a href="https://lucene.apache.org/solr/guide/{$solrGuideVersion}/solr-tutorial.html">Solr Tutorial</a>: This document covers the basics of running Solr using an example schema, and some sample data.</li>
            <li><a href="{$luceneJavadocUrl}index.html">Lucene Documentation</a></li>
          </ul>
        <h2>API Javadocs</h2>
        <xsl:call-template name="modules"/>
      </body>
    </html>
  </xsl:template>
  
  <xsl:template name="modules">
    <ul>
      <xsl:for-each select="str:split($buildfiles,'|')">
        <!-- hack to list "core" and "solrj" first, contains() returns "true" which sorts before "false" if descending: -->
        <xsl:sort select="string(contains(text(), '/core/'))" order="descending" lang="en"/>
        <xsl:sort select="string(contains(text(), '/solrj/'))" order="descending" lang="en"/>
        <!-- hack to list "test-framework" at the end, contains() returns "true" which sorts after "false" if ascending: -->
        <xsl:sort select="string(contains(text(), '/test-framework/'))" order="ascending" lang="en"/>
        <!-- sort the remaining build files by path name: -->
        <xsl:sort select="text()" order="ascending" lang="en"/>
        
        <xsl:variable name="buildxml" select="document(.)"/>
        <xsl:variable name="name" select="$buildxml/*/@name"/>
        <li>
          <xsl:if test="$name='solr-core'">
            <xsl:attribute name="style">font-size:larger; margin-bottom:.5em;</xsl:attribute>
          </xsl:if>
          <b><a href="{$name}/index.html"><xsl:value-of select="$name"/>
          </a><xsl:text>: </xsl:text></b>
          <xsl:value-of select="normalize-space($buildxml/*/description)"/>
        </li>
      </xsl:for-each>
    </ul>
  </xsl:template>

</xsl:stylesheet>
