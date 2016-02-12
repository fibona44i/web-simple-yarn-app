package com.hortonworks.simpleyarnapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by Andrii_Krasnolob on 2/11/2016.
 */
public class HBaseMyClient {
    Configuration hBaseConfig = HBaseConfiguration.create();

    public HBaseMyClient() {
        hBaseConfig.addResource(new Path("/usr/hdp/2.3.0.0-2557/hbase/conf/hbase-site.xml"));
    }

    public Set<String> getValues() throws IOException {
        HTable table = new HTable(hBaseConfig, "plane-data");
        Scan s = new Scan();
        s.addColumn(Bytes.toBytes("plane"),Bytes.toBytes("manufacturer"));
        ResultScanner scanner = table.getScanner(s);
        Set<String> result = new HashSet<>();
        for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
            String value = Bytes.toString(rr.getValue(Bytes.toBytes("plane"),Bytes.toBytes("manufacturer")));
            result.add(value);
        }
        return result;
    }

}
