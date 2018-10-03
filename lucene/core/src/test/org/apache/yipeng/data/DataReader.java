package org.apache.yipeng.data;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.StreamSupport;

/**
 * Created by yipeng on 2018/10/1.
 */
public class DataReader {
  public static List<Map<String,String>> getIndexDatas() throws IOException {
    List<Map<String,String>> indexDataList= new ArrayList<>();
    String src = DataReader.class.getResource("").getPath();
    Path fpath= Paths.get(src.substring(1,src.length())+"ware.txt");
    BufferedReader bfr= Files.newBufferedReader(fpath);
    Random random = new Random();
    String titleStr = bfr.readLine();
    String[] titles = titleStr.split(",");
    String lines = bfr.readLine();

    while(lines != null){
      String[] lineArr = lines.split(",");
      if(lineArr.length == titles.length){
        Map<String,String> map = new HashMap<>();
        for (int i = 0; i < lineArr.length; i++) {
          if(lineArr[i] != null && !lineArr[i].equals("NULL")){
            map.put(titles[i],lineArr[i]);
          }
        }
        indexDataList.add(map);
      }
      lines = bfr.readLine();
    }
    return indexDataList;
  }
}
