package org.dataone.cn.index.generator;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class IndexTaskGeneratorDaemon implements Daemon {

    private static Logger logger = Logger.getLogger(IndexTaskGeneratorDaemon.class.getName());

    private ApplicationContext context;
    private IndexTaskGeneratorEntryListener listener;

    public IndexTaskGeneratorDaemon() {
    }

    @Override
    public void start() throws Exception {
        logger.info("starting index task generator daemon...");
        context = new ClassPathXmlApplicationContext("generator-daemon-context.xml");
        listener = (IndexTaskGeneratorEntryListener) context
                .getBean("indexTaskGeneratorEntryListener");
        listener.start();
    }

    @Override
    public void stop() throws Exception {
        logger.info("stopping index task generator daemon...");
        listener.stop();
    }

    @Override
    public void destroy() {

    }

    @Override
    public void init(DaemonContext arg0) throws Exception {

    }
}
