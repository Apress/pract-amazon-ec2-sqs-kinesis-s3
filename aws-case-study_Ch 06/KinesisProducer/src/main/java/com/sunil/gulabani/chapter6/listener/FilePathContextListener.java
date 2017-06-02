package com.sunil.gulabani.chapter6.listener;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;
import java.io.File;

@WebListener
public class FilePathContextListener implements ServletContextListener {

    public void contextInitialized(ServletContextEvent servletContextEvent) {
        String rootPath = System.getProperty("catalina.home");
        ServletContext servletContext = servletContextEvent.getServletContext();
        String relativePath = servletContext.getInitParameter("file.dir");
        String filesDir = rootPath + File.separator + relativePath;
        File file = new File(filesDir);

        if(!file.exists()) {
            file.mkdirs();
        }

        servletContext.setAttribute("FILES_DIR_FILE", file);
        servletContext.setAttribute("FILES_DIR", filesDir);
    }

    public void contextDestroyed(ServletContextEvent servletContextEvent) {
    }
}
