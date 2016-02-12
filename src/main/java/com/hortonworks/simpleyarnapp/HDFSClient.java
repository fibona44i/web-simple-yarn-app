package com.hortonworks.simpleyarnapp;
import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Created by Andrii_Krasnolob on 2/12/2016.
 */





    /**
     * Created by user on 12/8/15.
     */
    public class HDFSClient {

        final static String HADOOP_CONF_PATH = "/usr/hdp/2.3.0.0-2557/hadoop/conf/";
        FileSystem fileSystem;
        Configuration conf;

        public static void setLogger(Logger logger) {
            HDFSClient.logger = logger;
        }

        private static Logger logger;

        public HDFSClient() throws IOException {
            conf = new Configuration();
            conf.addResource(new Path(HADOOP_CONF_PATH + "core-site.xml"));
            conf.addResource(new Path(HADOOP_CONF_PATH + "hdfs-site.xml"));
            conf.addResource(new Path(HADOOP_CONF_PATH + "mapred-site.xml"));
            fileSystem = FileSystem.get(conf);
            setLogger(LoggerFactory.getLogger(HDFSClient.class));

            logger.info("HDFSClient()");
            logger.info(fileSystem.toString());

        }


        public void readFile(String file, OutputStream out) throws IOException {

            Path path = new Path(file);

            logger.info("Filesystem URI : " + fileSystem.getUri());
            logger.info("Filesystem Home Directory : " + fileSystem.getHomeDirectory());
            logger.info("Filesystem Working Directory : " + fileSystem.getWorkingDirectory());
            logger.info("HDFS File Path : " + path);

            if (!fileSystem.exists(path)) {
                System.out.println("File " + file + " does not exists");
                return;
            }

            FSDataInputStream in = fileSystem.open(path);

            byte[] b = new byte[1024];
            int numBytes = 0;
            while ((numBytes = in.read(b)) > 0) {
                out.write(b, 0, numBytes);
            }

            in.close();
            fileSystem.close();
        }


        public void dirContent(String directory) {
            try{
                FileStatus[] status = fileSystem.listStatus(new Path(directory));  // you need to pass in your hdfs path

                for (int i = 0; i < status.length; i++) {
                    logger.info(status[i].getPath().toString());

                }
            } catch (Exception e) {
                System.out.println("File not found");
            }
        }
    }
