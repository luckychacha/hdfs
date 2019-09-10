package com.bigdata.hdfscompress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;
import java.net.URI;

public class FileCompress {

    public static void main(String[] args) throws ClassNotFoundException, IOException {
        //BZip2压缩
//        String codeClassName = "org.apache.hadoop.io.compress.BZip2Codec";
        //DEFLATE
//        String codeClassName = "org.apache.hadoop.io.compress.DeflateCodec";
        //gzip
        String codeClassName = "org.apache.hadoop.io.compress.GzipCodec";
        /**
         * TODO lzo snappy
         */

        Class<?> codecClass = Class.forName(codeClassName);

        Configuration conf = new Configuration();

        CompressionCodec compressionCodec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);

        String source = "/Users/leixinxin/a.log";

        String dest = "hdfs://node01:8020/a-dest.log.gzip";

        InputStream inputStream = new BufferedInputStream(new FileInputStream(source));

        FileSystem fileSystem = FileSystem.get(URI.create(dest), conf);

        OutputStream out = fileSystem.create(new Path(dest));

        CompressionOutputStream compressionOutputStream = compressionCodec.createOutputStream(out);

        IOUtils.copyBytes(inputStream, compressionOutputStream, 4096, true);


    }
}
