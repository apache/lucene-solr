<!--
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 -->

# Apache Solr Language Identifier


##Introduction

This module is intended to be used while indexing documents.
It is implemented as an UpdateProcessor to be placed in an UpdateChain.
Its purpose is to identify language from documents and tag the document with language code.
The module can optionally map field names to their language specific counterpart,
e.g. if the input is `title` and language is detected as `en`, map to `title_en`.
Language may be detected globally for the document, and/or individually per field.
Language detector implementations are pluggable.

##Getting Started

Please refer to the module documentation at http://wiki.apache.org/solr/LanguageDetection

##Dependencies

The Tika detector depends on Tika Core (which is part of extraction contrib)
The Langdetect detector depends on LangDetect library
The OpenNLP detector depends on OpenNLP tools and requires a previously trained user-supplied model
