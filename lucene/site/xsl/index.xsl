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
  
	<xsl:template match="/">
		<html>
      <head>
        <title><xsl:text>Apache Lucene </xsl:text><xsl:value-of select="$version"/><xsl:text> Documentation</xsl:text></title>
      </head>
      <body>
        <div><img src="lucene_green_300.gif"/></div>
        <h1><xsl:text>Apache Lucene </xsl:text><xsl:value-of select="$version"/><xsl:text> Documentation</xsl:text></h1>
        <p>
          This is the official documentation for <b><xsl:text>Apache Lucene </xsl:text>
          <xsl:value-of select="$version"/></b>. Additional documentation is available in the
          <a href="http://wiki.apache.org/lucene-java">Wiki</a>.
        </p>
        <h2>Index</h2>
        <ul>
          <li><a href="changes/Changes.html">Changes</a></li>
          <li><a href="fileformats.html">File Formats Documentation</a></li>
          <li><a href="scoring.html">Scoring in Lucene</a></li>
          <li><a href="queryparser/org/apache/lucene/queryparser/classic/package-summary.html#package_description">Classic QueryParser Syntax</a></li>
        </ul>
        <h2>Getting Started</h2>
        <p>This document is intended as a "getting started" guide. It has three
        audiences: first-time users looking to install Apache Lucene in their
        application; developers looking to modify or base the applications they develop
        on Lucene; and developers looking to become involved in and contribute to the
        development of Lucene. This document is written in tutorial and walk-through
        format. The goal is to help you "get started". It does not go into great depth
        on some of the conceptual or inner details of Lucene.</p>
        <p>Each section listed below builds on one another. More advanced users may
        wish to skip sections.</p>
        <ul>
        <li><a href="demo.html">About the command-line Lucene demo and its usage</a>.
        This section is intended for anyone who wants to use the command-line Lucene
        demo.</li>
        <li><a href="demo2.html">About the sources and implementation for the
        command-line Lucene demo</a>. This section walks through the implementation
        details (sources) of the command-line Lucene demo. This section is intended for
        developers.</li>
        </ul>
        <h2>Javadocs</h2>
        <xsl:call-template name="modules"/>
      </body>
    </html>
	</xsl:template>
	
  <xsl:template name="modules">
    <ul>
      <xsl:for-each select="str:split($buildfiles,'|')">
        <!-- hack to list "core" first, contains() returns "true" which sorts before "false" if descending: -->
        <xsl:sort select="string(contains(text(), '/core/'))" order="descending" lang="en"/>
        <!-- hack to list "test-framework" at the end, contains() returns "true" which sorts after "false" if ascending: -->
        <xsl:sort select="string(contains(text(), '/test-framework/'))" order="ascending" lang="en"/>
        <!-- sort the remaining build files by path name: -->
        <xsl:sort select="text()" order="ascending" lang="en"/>
        
        <xsl:variable name="buildxml" select="document(.)"/>
        <xsl:variable name="name" select="$buildxml/*/@name"/>
        <li>
          <xsl:if test="$name='core'">
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
