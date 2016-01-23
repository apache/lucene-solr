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
    <html>
      <head>
        <link rel="stylesheet" type="text/css" href="solr-admin.css"></link>
        <link rel="icon" href="favicon.ico" type="image/ico"></link>
        <link rel="shortcut icon" href="favicon.ico" type="image/ico"></link>
        <title>Solr Search Results</title>
      </head>
      <body>
        <a href=".">
           <img border="0" align="right" height="78" width="142" src="solr_small.png" alt="Apache Solr">
           </img>
        </a>
        <h1>Solr Search Results</h1>
          <br clear="all" />
        <xsl:apply-templates/>
        <br /><br />
        <a href=".">Return to Admin Page</a>
      </body>
    </html>
  </xsl:template>


  <xsl:template match="responseHeader">
    <table name="responseHeader">
      <xsl:apply-templates/>
    </table>
  </xsl:template>


  <xsl:template match="status">
    <tr>
      <td name="responseHeader"><strong>Status:&#xa0;</strong></td>
      <td><xsl:value-of select="."></xsl:value-of></td>
    </tr>
  </xsl:template>


  <xsl:template match="numFields">
    <tr>
      <td name="responseHeader"><strong>Number of Fields:&#xa0;</strong></td>
      <td><xsl:value-of select="."></xsl:value-of></td>
    </tr>
  </xsl:template>


  <xsl:template match="numRecords">
    <tr>
      <td name="responseHeader"><strong>Records Returned:&#xa0;</strong></td>
      <td><xsl:value-of select="."></xsl:value-of></td>
    </tr>
  </xsl:template>


  <xsl:template match="numFound">
    <tr>
      <td name="responseHeader"><strong>Records Found:&#xa0;</strong></td>
      <td><xsl:value-of select="."></xsl:value-of></td>
    </tr>
  </xsl:template>


  <xsl:template match="QTime">
    <tr>
      <td name="responseHeader"><strong>Query time:&#xa0;</strong></td>
      <td><xsl:value-of select="."></xsl:value-of>(ms)</td>
    </tr>
  </xsl:template>

  <!-- YCS.. match everything.  How to match only what is not
       matched above???
    -->
  <xsl:template match="responseHeader/*">
    <tr>
      <td name="responseHeader"><strong><xsl:value-of select="name(.)"></xsl:value-of>:&#xa0;</strong></td>
      <td><xsl:value-of select="."></xsl:value-of></td>
    </tr>
  </xsl:template>

  <xsl:template match="responseBody">
    <br></br><br></br>
    <table border="2">

      <!-- table headers -->
      <tr>
        <xsl:for-each select="record[1]/field">
          <th><xsl:value-of select="name"></xsl:value-of></th>
        </xsl:for-each>
      </tr>

      <!-- table rows -->
      <xsl:for-each select="record">
        <tr>
          <xsl:for-each select="field">
            <td><xsl:value-of select="value"></xsl:value-of>&#xa0;</td>
          </xsl:for-each>
        </tr>
      </xsl:for-each>

    </table>


  </xsl:template>


</xsl:stylesheet>
