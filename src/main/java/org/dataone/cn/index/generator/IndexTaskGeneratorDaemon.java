package org.dataone.cn.index.generator;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class IndexTaskGeneratorDaemon implements Daemon {

    private ApplicationContext context;
    private IndexTaskGeneratorEntryListener listener;

    public IndexTaskGeneratorDaemon() {
    }

    @Override
    public void start() throws Exception {
        System.out.println("starting index task generator daemon...");
        context = new ClassPathXmlApplicationContext("generator-daemon-context.xml");
        listener = (IndexTaskGeneratorEntryListener) context
                .getBean("indexTaskGeneratorEntryListener");
        listener.start();
    }

    @Override
    public void stop() throws Exception {
        System.out.println("stopping index task generator daemon...");
        listener.stop();
    }

    @Override
    public void destroy() {

    }

    @Override
    public void init(DaemonContext arg0) throws Exception {

    }

    // public static void main(String[] args) {
    // IndexTaskGeneratorDaemon daemon = new IndexTaskGeneratorDaemon();
    // try {
    // daemon.start();
    // } catch (Exception e) {
    // e.printStackTrace();
    // try {
    // daemon.stop();
    // } catch (Exception e1) {
    // e1.printStackTrace();
    // }
    // }
    // }
}
