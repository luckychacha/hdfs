package com.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mortbay.util.StringUtil;

import java.io.IOException;

public class ListDirFileInfo {
    public static void main(String[] args) throws IOException {
        String dir = "hdfs://node01:8020/test/love-o";
        Path path = new Path(dir);
        FileSystem fs = FileSystem.get(new Configuration());
        FileStatus[] fileStatuses = fs.listStatus(path);
        for (FileStatus fileStatus : fileStatuses) {
            if (true == fileStatus.isDirectory()) {
                continue;
            }
            System.out.println(StringUtil.replace(fileStatus.getPath().toUri().getPath(), path.toUri().getPath() + "/", ""));
        }
}
}
