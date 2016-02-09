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

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class ApplicationMasterServlet extends HttpServlet
{
    AMRMClient<AMRMClient.ContainerRequest> rmClient = AMRMClient.createAMRMClient();
    NMClient nmClient = NMClient.createNMClient();
    Priority priority = Records.newRecord(Priority.class);
    Resource capability = Records.newRecord(Resource.class);
    Configuration conf = new YarnConfiguration();

    int responseId = 0;

    public void initApplicationMaster() throws YarnException, IOException {
        // Initialize clients to ResourceManager and NodeManagers

        rmClient.init(conf);
        rmClient.start();

        nmClient.init(conf);
        nmClient.start();

        // Priority for worker containers - priorities are intra-application
        priority.setPriority(0);

        // Resource requirements for worker containers
        capability.setMemory(128);
        capability.setVirtualCores(1);

        // Register with ResourceManager
        System.out.println("registerApplicationMaster");
        rmClient.registerApplicationMaster("", 0, "");
    }


    // Obtain allocated containers, launch and check for responses

    public void containerAllocate(String[] parameters) throws IOException, YarnException, InterruptedException {




        AMRMClient.ContainerRequest containerAsk = new AMRMClient.ContainerRequest(capability, null, null, priority);
        System.out.println("Making resource request");
        rmClient.addContainerRequest(containerAsk);

        while (true) {
            AllocateResponse response = rmClient.allocate(responseId++);
            for (Container container : response.getAllocatedContainers()) {
                // Launch container by create ContainerLaunchContext
                ContainerLaunchContext ctx =
                        Records.newRecord(ContainerLaunchContext.class);
                ctx.setCommands(
                        Collections.singletonList(

                                "$JAVA_HOME/bin/java" +
                                        " -Xmx256M" +
                                        " com.hortonworks.simpleyarnapp.ApplicationContainer" +
                                        " " + StringUtils.join(" ", parameters) +
                                        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                                        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                        ));
                LocalResource appMasterJar = Records.newRecord(LocalResource.class);
                setupContainerJar(new Path("hdfs:///yarn_application/simple-yarn-app-1.1.0.jar"), appMasterJar);
                ctx.setLocalResources(Collections.singletonMap("simple-yarn-app-1.1.0.jar", appMasterJar));


                // Setup CLASSPATH for ApplicationMaster
                Map<String, String> appMasterEnv = new HashMap<String, String>();
                setupContainerEnv(appMasterEnv);
                ctx.setEnvironment(appMasterEnv);
                System.out.println("Launching container " + container.getId());
                nmClient.startContainer(container, ctx);
            }

            for (ContainerStatus status : response.getCompletedContainersStatuses()) {
                System.out.println("Completed container " + status.getContainerId());
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
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
    {
        response.setContentType("text/html");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().println("<h1>Hello Servlet</h1>");
        response.getWriter().println("session=" + request.getSession(true).getId() +
                        "<BR /><a href='/'>Return Back</a>"
        );
        ApplicationMasterServlet yarnAM = new ApplicationMasterServlet();
        try {
            yarnAM.initApplicationMaster();
        } catch (YarnException e) {
            e.printStackTrace();
        }
        // Make container requests to ResourceManager
        try {
            yarnAM.containerAllocate(new String[] {"test"});
        } catch (YarnException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Un-register with ResourceManager
        try {
            yarnAM.rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
        } catch (YarnException e) {
            e.printStackTrace();
        }
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("text/html");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().println("<form action='' method='post'>" +
                "<button name='foo' value='run'>Run</button>" +
                "</form>");
    }
}