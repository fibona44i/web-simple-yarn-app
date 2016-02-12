package com.hortonworks.simpleyarnapp;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Set;


public class ApplicationMasterServlet extends AbstractHandler
{
    private static Logger log = LoggerFactory.getLogger(ApplicationMasterServlet.class);

    //ApplicationMaster section
    AMRMClient<AMRMClient.ContainerRequest> rmClient = AMRMClient.createAMRMClient();
    NMClient nmClient = NMClient.createNMClient();
    Configuration conf = new YarnConfiguration();

    //Servlet section
    private Server server = null;

    public ApplicationMasterServlet(Server server) throws IOException, YarnException {
        System.out.println("allocationg new ApplicationMasterServlet");

        this.server = server;
        initApplicationMaster();
    }

    public static void main(String[] args) throws Exception {

        Server server = new Server(21093);

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

    private void initApplicationMaster() {

        rmClient.init(conf);
        rmClient.start();
        // Initialize clients to ResourceManager and NodeManagers

        nmClient.init(conf);
        nmClient.start();

        // Register with ResourceManager
        System.out.println("registerApplicationMaster");
        try {
            rmClient.registerApplicationMaster("", 0, "");
        } catch (YarnException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private boolean stopServer(HttpServletResponse response) throws IOException {
        log.warn("Stopping Jetty");
        response.setStatus(HttpServletResponse.SC_ACCEPTED);
        response.setContentType("text/html");
        ServletOutputStream os = response.getOutputStream();
        os.println("Shutting down.");
        os.close();
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

        // Un-register with ResourceManager
        try {
            rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
        } catch (YarnException e) {
            e.printStackTrace();
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
        }
        else if("/run".equals(pathInfo)){
            showIndexPage(httpServletResponse);
            runContainer(httpServletRequest, httpServletResponse);
        }
        else {
            showIndexPage(httpServletResponse);
        }

        httpServletResponse.flushBuffer();
    }

    private void showIndexPage(HttpServletResponse httpServletResponse) throws IOException {
        httpServletResponse.setContentType("text/html");
        httpServletResponse.setStatus(HttpServletResponse.SC_OK);
        ServletOutputStream os = httpServletResponse.getOutputStream();
        os.println("<form action='/run' method='get'>" +
                "<button>Run</button>" +
                "<select name=\"manufacturer\">");
        HBaseMyClient hBaseClient = new HBaseMyClient();
        Set<String> hbaseContent = hBaseClient.getValues();
        for (String value:hbaseContent){
            os.println(" <option value=\"" + value +"\">" + value + "</option>");
        }
        os.println("</select>" +
                "</form>");
        os.println("<form action='/stop' method='get'>" +
                "<button>Stop</button>" +
                "</form>");
    }

    private void runContainer(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws IOException {

        httpServletResponse.setContentType("text/html");
        httpServletResponse.setStatus(HttpServletResponse.SC_OK);
        ServletOutputStream os = httpServletResponse.getOutputStream();

        ApplicationContainer applicationContainer = new ApplicationContainer(rmClient, nmClient, conf);
        String manufacturer = httpServletRequest.getParameter("manufacturer");
        String[] params = new String[] {manufacturer};
        // Make container requests to ResourceManager
        try {
            applicationContainer.containerAllocate(params);
            os.println("Container allocated! for " + manufacturer);
        } catch (YarnException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        os.close();
    }
}