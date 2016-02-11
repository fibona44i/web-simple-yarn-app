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
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class ApplicationMasterServlet extends AbstractHandler
{
    AMRMClient<AMRMClient.ContainerRequest> rmClient = AMRMClient.createAMRMClient();
    NMClient nmClient = NMClient.createNMClient();
    Priority priority = Records.newRecord(Priority.class);
    Resource capability = Records.newRecord(Resource.class);
    Configuration conf = new YarnConfiguration();
    private static Logger log = LoggerFactory.getLogger(MyHandler.class);
    private Server server = null;

    int responseId = 0;
    public ApplicationMasterServlet(Server server) {
        this.server = server;
    }
    private boolean stopServer(HttpServletResponse response) throws IOException {
        log.warn("Stopping Jetty");
        response.setStatus(HttpServletResponse.SC_ACCEPTED);
        response.setContentType("text/plain");
        ServletOutputStream os = response.getOutputStream();
        os.println("Shutting down.");
        os.close();
        response.flushBuffer();
        try {
            // Stop the server.
            new Thread() {

                @Override
                public void run() {
                    try {
                        log.info("Shutting down Jetty...");
                        System.out.println("Shutting down Jetty...");
                        server.stop();
                        log.info("Jetty has stopped.");
                        System.out.println("Jetty has stopped.");

                    } catch (Exception ex) {
                        log.error("Error when stopping Jetty: " + ex.getMessage(), ex);
                        System.out.println("Jetty has stopped.");

                    }
                }
            }.start();
        } catch (Exception ex) {
            log.error("Unable to stop Jetty: " + ex);
            return false;
        }
        return true;
    }

    @Override
    public void handle(String s, Request request, HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws IOException, ServletException {

        String pathInfo = httpServletRequest.getPathInfo();
        // THIS SHOULD OBVIOUSLY BE SECURED!!!
        System.out.println("pathInfo: " + pathInfo);

        if ("/stop".equals(pathInfo)) {
            stopServer(httpServletResponse);
            // Un-register with ResourceManager
            try {
                rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
            } catch (YarnException e) {
                e.printStackTrace();
            }
            return;
        }
        else if("/run".equals(pathInfo)){
            httpServletResponse.setContentType("text/html");
            httpServletResponse.setStatus(HttpServletResponse.SC_OK);
            ServletOutputStream os = httpServletResponse.getOutputStream();
            try {
                initApplicationMaster();
            } catch (YarnException e) {
                e.printStackTrace();
            }
            // Make container requests to ResourceManager
            try {
                containerAllocate(new String[] {"test"});
                os.println("Container allocated!");
            } catch (YarnException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            os.close();
        }
        else{
            httpServletResponse.setContentType("text/html");
            httpServletResponse.setStatus(HttpServletResponse.SC_OK);
            ServletOutputStream os = httpServletResponse.getOutputStream();
            os.println("<form action='/run' method='get'>" +
                    "<button name='foo' value='run'>Run</button>" +
                    "</form>");
            os.close();
            httpServletResponse.flushBuffer();
            return;
        }
    }

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

    public static void main(String[] args) throws Exception {

        Server server = new Server(21093);
/*
        WebAppContext webAppContext = new WebAppContext();
        webAppContext.setResourceBase(".");
        webAppContext.setDescriptor("web.xml");
        webAppContext.setContextPath("/");
        server.setHandler(webAppContext);
*/

        HandlerCollection handlers = new HandlerCollection();
        ContextHandlerCollection contexts = new ContextHandlerCollection();
        RequestLogHandler requestLogHandler = new RequestLogHandler();
        server.setStopAtShutdown(true);
        // Now add the handlers
        ApplicationMasterServlet myHandler = new ApplicationMasterServlet(server);
        handlers.setHandlers(new Handler[]{contexts, myHandler, requestLogHandler});
        server.setHandler(handlers);

       server.start();
       server.join();

   }
}