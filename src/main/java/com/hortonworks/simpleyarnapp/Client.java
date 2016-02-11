package com.hortonworks.simpleyarnapp;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;


public class Client {

  Configuration conf = new YarnConfiguration();
  
  public void run(String[] args) throws Exception {

    //final Path jarPath = new Path(args[0]);
  String[] parameters = new String[] {"test1", "test2"};
    // Create yarnClient
    YarnConfiguration conf = new YarnConfiguration();
    YarnClient yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();
    
    // Create application via yarnClient
    YarnClientApplication app = yarnClient.createApplication();

    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
    amContainer.setCommands(
            Collections.singletonList(
                    "$JAVA_HOME/bin/java" +
                            " -Xmx256M" +
                            " com.hortonworks.simpleyarnapp.ApplicationMasterServlet" +
                            " " + StringUtils.join(" ", parameters) +
                            //"ls -ltr" +
                            " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                            " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
            )
    );
    
    // Setup jar for ApplicationMaster

      LocalResource appMasterJar = Records.newRecord(LocalResource.class);
      setupAppMasterJar(new Path("hdfs:///yarn_application/simple-yarn-app-1.1.0.jar"), appMasterJar);

      LocalResource appMasterJarJetty = Records.newRecord(LocalResource.class);
      setupAppMasterJar(new Path("hdfs:///yarn_application/jetty-all-9.2.9.v20150224.jar"), appMasterJarJetty);

      LocalResource appMasterJarJavaxServlet = Records.newRecord(LocalResource.class);
      setupAppMasterJar(new Path("hdfs:///yarn_application/javax.servlet-api-3.1.0.jar"), appMasterJarJavaxServlet);

      //LocalResource containerResource = Records.newRecord(LocalResource.class);
      //setupAppMasterJar(new Path("hdfs:///app/gs-yarn-basic/index.html"), containerResource);

      LocalResource containerResource2 = Records.newRecord(LocalResource.class);
      setupAppMasterJar(new Path("hdfs:///yarn_application/web.xml"), containerResource2);

      Map <String, LocalResource> resourcesMap = new HashMap<>();
      resourcesMap.put("simple-yarn-app-1.1.0.jar", appMasterJar);
      resourcesMap.put("jetty-all-9.2.9.v20150224.jar", appMasterJarJetty);
      resourcesMap.put("javax.servlet-api-3.1.0.jar", appMasterJarJavaxServlet);


      //resourcesMap.put("index.html", containerResource);
      resourcesMap.put("web.xml", containerResource2);
      amContainer.setLocalResources(resourcesMap);

    // Setup CLASSPATH for ApplicationMaster
    Map<String, String> appMasterEnv = new HashMap<String, String>();
    setupAppMasterEnv(appMasterEnv);
    amContainer.setEnvironment(appMasterEnv);
    
    // Set up resource type requirements for ApplicationMaster
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(256);
    capability.setVirtualCores(1);

    // Finally, set-up ApplicationSubmissionContext for the application
    ApplicationSubmissionContext appContext = 
    app.getApplicationSubmissionContext();
    appContext.setApplicationName("simple-yarn-app"); // application name
    appContext.setAMContainerSpec(amContainer);
    appContext.setResource(capability);
    appContext.setQueue("default"); // queue 

    // Submit application
    ApplicationId appId = appContext.getApplicationId();
    appContext.setApplicationName("simple-yarn-app"+ ApplicationConstants.LOG_DIR_EXPANSION_VAR); // application name

    System.out.println("Submitting application " + appId);
    yarnClient.submitApplication(appContext);
    
    ApplicationReport appReport = yarnClient.getApplicationReport(appId);
    YarnApplicationState appState = appReport.getYarnApplicationState();

    int count=0;
    while (appState != YarnApplicationState.FINISHED && 
           appState != YarnApplicationState.KILLED && 
           appState != YarnApplicationState.FAILED //&& count < 100
            ) {

      Thread.sleep(100);
      appReport = yarnClient.getApplicationReport(appId);
      appState = appReport.getYarnApplicationState();
      count++;
    }
    System.out.println(
        "Application " + appId + " finished with" +
    		" state " + appState + 
    		" at " + appReport.getFinishTime());

  }
  
  private void setupAppMasterJar(Path jarPath, LocalResource appMasterJar) throws IOException {
    FileStatus jarStat = FileSystem.get(conf).getFileStatus(jarPath);
    appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
    appMasterJar.setSize(jarStat.getLen());
    appMasterJar.setTimestamp(jarStat.getModificationTime());
    appMasterJar.setType(LocalResourceType.FILE);
    appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC);
  }
  
  private void setupAppMasterEnv(Map<String, String> appMasterEnv) {
    for (String c : conf.getStrings(
        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
      Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
          c.trim());
    }
    Apps.addToEnvironment(appMasterEnv,
        Environment.CLASSPATH.name(),
        Environment.PWD.$() + File.separator + "*");
  }
  
  public static void main(String[] args) throws Exception {
    Client c = new Client();
    c.run(args);
  }
}
