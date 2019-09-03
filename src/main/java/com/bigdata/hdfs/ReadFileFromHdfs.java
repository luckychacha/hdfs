package com.bigdata.hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ReadFileFromHdfs {

    public static void main(String[] args) throws IOException {
        String src = "hdfs://node01:8020/leixinxin-dest";
        FileSystem fs = FileSystem.get(new Configuration());
        FSDataInputStream fsDataInputStream = fs.open(new Path(src));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream));
        String line = null;
        while ((line = bufferedReader.readLine()) != null) {
            System.out.println(line);
        }
    }
}
