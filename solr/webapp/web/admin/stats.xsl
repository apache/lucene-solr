<?xml version="1.0" encoding="utf-8"?>
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
<!-- $Id$ -->
<!-- $URL$ -->

<xsl:stylesheet
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  version="1.0">


  <xsl:output
    method="html"
    encoding="utf-8"
    media-type="text/html"
    indent="yes"
    doctype-public="-//W3C//DTD HTML 4.01//EN"
    doctype-system="http://www.w3.org/TR/html4/strict.dtd" />


  <xsl:template match="/">
    <xsl:variable name="title">
      <!-- no whitespace before the colon -->
      Solr Statistics<xsl:if test="solr/core">:
         <xsl:value-of select="solr/core"/>
      </xsl:if>
(<xsl:value-of select="solr/schema" />)
    </xsl:variable>
    <html>
      <head>
        <link rel="stylesheet" type="text/css" href="solr-admin.css"></link>
	<link rel="icon" href="favicon.ico" type="image/ico"></link>
	<link rel="shortcut icon" href="favicon.ico" type="image/ico"></link>
        <title><xsl:value-of select="$title"/></title>
      </head>
      <body>
        <a href=".">
	   <img border="0" align="right" height="78" width="142" src="solr_small.png" alt="Apache Solr">
	   </img>
	</a>
        <h1><xsl:value-of select="$title"/></h1>
          <xsl:value-of select="solr/host" />
          <br clear="all" />
        <xsl:apply-templates/>
        <br /><br />
        <a href=".">Return to Admin Page</a>
      </body>
    </html>
  </xsl:template>

  <xsl:template match="solr">
  <table>
    <tr>
      <td>
        <H3>Category</H3>
      </td>
      <td>
        [<a href="#core">Core</a>]
        [<a href="#cache">Cache</a>]
        [<a href="#query">Query</a>]
        [<a href="#update">Update</a>]
        [<a href="#highlight">Highlighting</a>]
        [<a href="#other">Other</a>]
      </td>
    </tr>
    <tr>
      <td>
      </td>
      <td>
        Current Time: <xsl:value-of select="now" />
      </td>
    </tr>
    <tr>
      <td>
      </td>
      <td>
        Server Start Time: <xsl:value-of select="start" />
      </td>
    </tr>
    <xsl:apply-templates select="*" mode="header" />
  </table>
  <xsl:apply-templates select="solr-info" mode="main" />
  </xsl:template>

  <!-- catch all in case new header info gets added to XML -->
  <xsl:template match="solr/*" mode="header" priority="-10">
    <tr>
      <td>
      </td>
      <td>
        <xsl:value-of select="local-name()" />: <xsl:value-of select="text()" />
      </td>
    </tr>
  </xsl:template>

  <!-- things we've already explicitly taken care of -->
  <xsl:template match="solr/schema"    mode="header" />
  <xsl:template match="solr/core"      mode="header" />
  <xsl:template match="solr/host"      mode="header" />
  <xsl:template match="solr/now"       mode="header" />
  <xsl:template match="solr/start"     mode="header" />
  <xsl:template match="solr/solr-info" mode="header" />

  <xsl:template match="solr/solr-info" mode="main">
    <xsl:apply-templates/>
  </xsl:template>

  <xsl:template match="solr/solr-info/*">
    <br />
    <xsl:apply-templates select="." mode="sub-header" />
    <table>
        <tr>
          <td align="right">
            &#xa0;
          </td>
          <td>
          </td>
        </tr>
        <xsl:apply-templates/>
    </table>
  </xsl:template>

  <xsl:template match="solr/solr-info/CORE" mode="sub-header">
    <a name="core"><h2>Core</h2></a>
  </xsl:template>

  <xsl:template match="solr/solr-info/CACHE" mode="sub-header">
    <a name="cache"><h2>Cache</h2></a>
  </xsl:template>

  <xsl:template match="solr/solr-info/QUERYHANDLER" mode="sub-header">
    <a name="query"><h2>Query Handlers</h2></a>
  </xsl:template>

  <xsl:template match="solr/solr-info/UPDATEHANDLER" mode="sub-header">
    <a name="update"><h2>Update Handlers</h2></a>
  </xsl:template>

  <xsl:template match="solr/solr-info/HIGHLIGHTING" mode="sub-header">
    <a name="highlight"><h2>Highlighting</h2></a>
  </xsl:template>

  <!-- catch all for new types of plugins -->
  <xsl:template match="solr/solr-info/*" mode="sub-header" priority="-10">
    <h2><xsl:value-of select="local-name()"/></h2>
  </xsl:template>

  <xsl:template match="solr/solr-info/OTHER" mode="sub-header">
    <a name="other"><h2>Other</h2></a>
  </xsl:template>

  <xsl:template match="solr/solr-info/*/entry">
        <tr>
          <td align="right">
            <strong>name:&#xa0;</strong>
          </td>
          <td>
            <tt><xsl:value-of select="name"/>&#xa0;</tt>
          </td>
        </tr>
        <tr>
          <td align="right">
            <strong>class:&#xa0;</strong>
          </td>
          <td>
            <tt><xsl:value-of select="class"/>&#xa0;</tt>
          </td>
        </tr>
        <tr>
          <td align="right">
            <strong>version:&#xa0;</strong>
          </td>
          <td>
            <tt><xsl:value-of select="version"/>&#xa0;</tt>
          </td>
        </tr>
        <tr>
          <td align="right">
            <strong>description:&#xa0;</strong>
          </td>
          <td>
            <tt><xsl:value-of select="description"/>&#xa0;</tt>
          </td>
        </tr>
        <tr>
          <td align="right">
            <strong>stats:&#xa0;</strong>
          </td>
          <td>
            <xsl:for-each select="stats/stat[@name]">
              <xsl:value-of select="@name"/>
              <xsl:text> : </xsl:text>
              <xsl:variable name="name" select="@name" />
              <xsl:value-of select="." /><br />
            </xsl:for-each>
          </td>
        </tr>
        <tr>
          <td align="right">
          </td>
          <td>
          </td>
        </tr>
  </xsl:template>

</xsl:stylesheet>
