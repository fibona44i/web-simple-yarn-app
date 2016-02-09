package com.hortonworks.simpleyarnapp;


import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;

public class ApplicationMaster {

    public static void main(String[] args) throws Exception {

        Server server = new Server(21093);

        WebAppContext webAppContext = new WebAppContext();
        webAppContext.setResourceBase(".");
        webAppContext.setDescriptor("web.xml");
        webAppContext.setContextPath("/");
        server.setHandler(webAppContext);
        server.start();
        server.join();
    }
}
