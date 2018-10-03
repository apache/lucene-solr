/*
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

package org.apache.yipeng;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.yipeng.data.DataReader;

/**
 * Created by yipeng on 2018/10/1.
 */
public class TestIndexStructure {
  public static  Path indexPath = Paths.get("/lucene/index_structure");
  //ware_id,category_id,vender_id,title,online_time,stock,brand_id,s1,s2
  public static void main(String[] args) throws IOException {
    Directory directory = FSDirectory.open(indexPath);
    List<Map<String,String>> indexMapList = DataReader.getIndexDatas();
    IndexWriterConfig writerConfig = new IndexWriterConfig(new StandardAnalyzer());
    writerConfig.setUseCompoundFile(false);
    IndexWriter indexWriter = new IndexWriter(directory,writerConfig);

    for(Map<String,String> map : indexMapList){
      Document document = new Document();
      for(Map.Entry<String,String> entry : map.entrySet()){
        if(entry.getKey().equals("ware_id")){
          document.add(new LongPoint(entry.getKey(),Long.parseLong(entry.getValue())));
          document.add(new StoredField(entry.getKey(),Long.parseLong(entry.getValue())));
        }

        if(entry.getKey().equals("category_id")){
          document.add(new LongPoint(entry.getKey(),Long.parseLong(entry.getValue())));
          document.add(new StoredField(entry.getKey(),Long.parseLong(entry.getValue())));
        }

        if(entry.getKey().equals("vender_id")){
          document.add(new LongPoint(entry.getKey(),Long.parseLong(entry.getValue())));
          document.add(new StoredField(entry.getKey(),Long.parseLong(entry.getValue())));
        }

        if(entry.getKey().equals("title")){
          document.add(new TextField(entry.getKey(),entry.getValue(), Field.Store.YES));
        }

        if(entry.getKey().equals("online_time")){
          document.add(new StringField(entry.getKey(),entry.getValue(), Field.Store.YES));
        }

        if(entry.getKey().equals("stock")){
          document.add(new IntPoint(entry.getKey(),Integer.parseInt(entry.getValue())));
          document.add(new NumericDocValuesField(entry.getKey(),Integer.parseInt(entry.getValue())));
          document.add(new StoredField(entry.getKey(),Integer.parseInt(entry.getValue())));
        }

        if(entry.getKey().equals("brand_id")){
          document.add(new LongPoint(entry.getKey(),Long.parseLong(entry.getValue())));
          document.add(new StoredField(entry.getKey(),Long.parseLong(entry.getValue())));
        }

        if(entry.getKey().equals("s1")){
          document.add(new FloatPoint(entry.getKey(),Float.parseFloat(entry.getValue())));
          document.add(new FloatDocValuesField(entry.getKey(),Float.parseFloat(entry.getValue())));
          document.add(new StoredField(entry.getKey(),Float.parseFloat(entry.getValue())));
        }

        if(entry.getKey().equals("s2")){
          document.add(new FloatPoint(entry.getKey(),Float.parseFloat(entry.getValue())));
          document.add(new FloatDocValuesField(entry.getKey(),Float.parseFloat(entry.getValue())));
          document.add(new StoredField(entry.getKey(),Float.parseFloat(entry.getValue())));
        }
      }
      indexWriter.addDocument(document);
    }
    indexWriter.commit();
    indexWriter.close();
  }
}
