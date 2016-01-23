<?xml version="1.0" encoding="ISO-8859-1"?>
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
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:template match="/Document">
<!--This template ANDs all fields together. Within a single field all terms are ORed.
  The query fields are fed directly through an analyzer and so do not need to adhere to  
  traditional Lucene query syntax.
 -->  
<BooleanQuery>
  <xsl:if test="count(artist)>0">
      <Clause occurs="must">
        <TermsQuery fieldName="artist"><xsl:value-of select="artist"/></TermsQuery>
     </Clause>
   </xsl:if>
  <xsl:if test="count(album)>0">
      <Clause occurs="must">
        <TermsQuery fieldName="album"><xsl:value-of select="album"/></TermsQuery>
     </Clause>
   </xsl:if>
  <xsl:if test="count(genre)>0">
      <Clause occurs="must">
        <TermsQuery fieldName="genre"><xsl:value-of select="genre"/></TermsQuery>
     </Clause>
   </xsl:if>
  <xsl:if test="count(releaseDate)>0">
      <Clause occurs="must">
        <TermsQuery fieldName="releaseDate"><xsl:value-of select="releaseDate"/></TermsQuery>
     </Clause>
   </xsl:if>
</BooleanQuery>

</xsl:template>
</xsl:stylesheet>