package com.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;

public class FileCopyFromLocal {
    public static void main(String[] args) throws IOException {
        //上传文件
        String src = "/Users/leixinxin/Sites/test/leixinxin";
        //到hdfs
        String dest = "hdfs://node01:8020/leixinxin-dest";

        //IO 本地文件-> InputStream
        InputStream in = new BufferedInputStream(new FileInputStream(src));

        //HDFS -> OutputStream
        //获得文件系统
        FileSystem fs = FileSystem.get(new Configuration());

        OutputStream out =  fs.create(new Path(dest));

        //input -> output
        IOUtils.copyBytes(in, out, 4096, false);

        //关闭流 copyBytes第四个参数设置为 false 时需要手动关闭
        in.close();
        out.close();
    }
}
