package com.hortonworks.simpleyarnapp;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.Handler;

import org.eclipse.jetty.webapp.WebAppContext;

import java.net.URL;
import java.net.URLClassLoader;

/**
 * Created by user on 1/5/16.
 */
public class ApplicationContainer {
    public static void main(String args[]) throws Exception {
        System.out.println(args[0]);
        System.out.println("Launching ApplicationContainer");
/*
        ClassLoader cl = ClassLoader.getSystemClassLoader();

        URL[] urls = ((URLClassLoader)cl).getURLs();

        for(URL url: urls) {
            System.out.println(url.getFile());
        }
*/

        /*
        Server server = new Server(8080);
        server.start();
        server.dumpStdErr();
        server.join();*/
    }
}
