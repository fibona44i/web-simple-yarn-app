package com.hortonworks.simpleyarnapp;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Created by Andrii_Krasnolob on 2/9/2016.
 */
public class MyHandler extends AbstractHandler {
    private static Logger log = LoggerFactory.getLogger(MyHandler.class);
    private Server server = null;
    public MyHandler(Server server) {
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
        if ("/stop".equals(pathInfo)) {
            stopServer(httpServletResponse);
            return;
        }
        else{
            httpServletResponse.setContentType("text/html");
            httpServletResponse.setStatus(HttpServletResponse.SC_OK);
            ServletOutputStream os = httpServletResponse.getOutputStream();
            os.println("<form action='' method='get'>" +
                    "<button name='foo' value='run'>Run</button>" +
                    "</form>");
            os.close();
            httpServletResponse.flushBuffer();
            return;
        }
    }
}
