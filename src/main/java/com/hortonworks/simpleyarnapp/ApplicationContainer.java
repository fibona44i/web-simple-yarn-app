package com.hortonworks.simpleyarnapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by user on 1/5/16.
 */
public class ApplicationContainer {

    AMRMClient<AMRMClient.ContainerRequest> rmClient;
    NMClient nmClient;
    Configuration conf;

    Priority priority = Records.newRecord(Priority.class);
    Resource capability = Records.newRecord(Resource.class);
    static int responseId = 0;

    public ApplicationContainer(AMRMClient<AMRMClient.ContainerRequest> rmClient,
                                NMClient nmClient,
                                Configuration conf) {
        this.rmClient = rmClient;
        this.nmClient = nmClient;
        this.conf = conf;
        initApplicationContainer();
    }

    public void initApplicationContainer() {

        // Priority for worker containers - priorities are intra-application
        priority.setPriority(0);
        // Resource requirements for worker containers
        capability.setMemory(128);
        capability.setVirtualCores(1);

    }

    public void containerAllocate(String[] parameters) throws IOException, YarnException, InterruptedException {

        AMRMClient.ContainerRequest containerAsk = new AMRMClient.ContainerRequest(capability, null, null, priority);
        System.out.println("Making resource request");
        rmClient.addContainerRequest(containerAsk);

        while (true) {
            AllocateResponse response = rmClient.allocate(responseId++);
            System.out.println("responseId:" + responseId);

            for (Container container : response.getAllocatedContainers()) {
                // Launch container by create ContainerLaunchContext
                String containerCmd = "$JAVA_HOME/bin/java" +
                        " -Xmx256M" +
                        " com.hortonworks.simpleyarnapp.ApplicationContainer" +
                        " '" + StringUtils.join(" ", parameters) + "'" +
                        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";
                System.out.println(containerCmd);
                ContainerLaunchContext ctx =
                        Records.newRecord(ContainerLaunchContext.class);
                ctx.setCommands(Collections.singletonList(containerCmd));
                LocalResource appMasterJar = Records.newRecord(LocalResource.class);
                setupContainerJar(new Path("hdfs:///yarn_application/simple-yarn-app-1.1.0.jar"), appMasterJar);
                ctx.setLocalResources(Collections.singletonMap("simple-yarn-app-1.1.0.jar", appMasterJar));

                // Setup CLASSPATH for ApplicationMaster
                Map<String, String> appMasterEnv = new HashMap<String, String>();
                setupContainerEnv(appMasterEnv);
                ctx.setEnvironment(appMasterEnv);
                System.out.println("Launching container " + container.getId());
                nmClient.startContainer(container, ctx);
                return;
            }
            Thread.sleep(100);
        }
    }

    private void setupContainerJar(Path jarPath, LocalResource appMasterJar) throws IOException {
        FileStatus jarStat = FileSystem.get(conf).getFileStatus(jarPath);
        appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
        appMasterJar.setSize(jarStat.getLen());
        appMasterJar.setTimestamp(jarStat.getModificationTime());
        appMasterJar.setType(LocalResourceType.FILE);
        appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC);
    }
    private void setupContainerEnv(Map<String, String> appMasterEnv) {
        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            Apps.addToEnvironment(appMasterEnv, ApplicationConstants.Environment.CLASSPATH.name(),
                    c.trim());
        }
        Apps.addToEnvironment(appMasterEnv,
                ApplicationConstants.Environment.CLASSPATH.name(),
                ApplicationConstants.Environment.PWD.$() + File.separator + "*");
    }

    public static void main(String args[]) throws Exception {
        System.out.println(args[0]);
        System.out.println("Launching ApplicationContainer");
        HWebCatMyClient hWebCatMyClient = new HWebCatMyClient();
        String query = "select * from default.hbase_plane_data where manufacturer='" + args[0] + "';";
        String outputDirectory = "/" + UUID.randomUUID();
        String jobID = hWebCatMyClient.submitQuery(query, outputDirectory);
        System.out.println(jobID);
        while ( ! hWebCatMyClient.getJobStatus(jobID) ){
            Thread.sleep(100);
        }
        System.out.println("Job finished! Reading content of the " + outputDirectory + "/stdout file from the HDFS");
        HDFSClient hdfsClient = new HDFSClient();
        hdfsClient.readFile(outputDirectory + "/stdout", System.out);
        /* get list of jars
        ClassLoader cl = ClassLoader.getSystemClassLoader();

        URL[] urls = ((URLClassLoader)cl).getURLs();

        for(URL url: urls) {
            System.out.println(url.getFile());
        }
        */

    }
}
