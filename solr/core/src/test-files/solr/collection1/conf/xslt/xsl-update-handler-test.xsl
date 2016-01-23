<?xml version='1.0' encoding='UTF-8'?>

<!-- 
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
 -->

<!-- 


XSL transform used to test the XSLTUpdateRequestHandler.
Transforms a test XML into standard Solr <add><doc/></add> format.

 -->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
  <xsl:template match="/">
    <add>
      <xsl:apply-templates select="/random/document"/>
    </add>
  </xsl:template>

  <xsl:template match="document">
    <doc boost="5.5">
      <xsl:apply-templates select="*"/>
    </doc>
  </xsl:template>

  <xsl:template match="node">
    <field name="{@name}">
      <xsl:if test="@enhance!=''">
        <xsl:attribute name="boost"><xsl:value-of select="@enhance"/></xsl:attribute>
      </xsl:if>
      <xsl:value-of select="@value"/>
    </field>
  </xsl:template>

</xsl:stylesheet>
