package com.bigdata.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mortbay.util.StringUtil;

import java.io.IOException;

public class ListMultiDirInfo {

    public static void main(String[] args) throws IOException {
        String dir = "hdfs://node01:8020/";
        FileSystem fs = FileSystem.get(new Configuration());
        _recursion(fs, dir, "", " ");

    }

    private static void _recursion(FileSystem fileSystem, String dir, String space, String singleSpace) throws IOException {
        Path path = new Path(dir);
        String pathName = path.toUri().getPath().equals("/") ? path.toUri().getPath() : path.toUri().getPath() + "/";
        FileStatus[] fileStatuses = fileSystem.listStatus(path);
        for (FileStatus fs : fileStatuses) {
            String objName = fs.getPath().toUri().getPath();
            if (fs.isDirectory() == true) {
                String dirName = space + "|-" + StringUtil.replace(
                        objName,
                        pathName,
                        "") + "/";
                //打印目录
                System.out.println(dirName);
                _recursion(fileSystem, fs.getPath().toUri().toString(), space + singleSpace, singleSpace);
            } else {
                String fileName = space + "|-" + StringUtil.replace(
                        objName,
                        pathName,
                        "");
                System.out.println(fileName);
            }
        }
    }
}
