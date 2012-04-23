/**
 * This work was created by participants in the DataONE project, and is
 * jointly copyrighted by participating institutions in DataONE. For 
 * more information on DataONE, see our web site at http://dataone.org.
 *
 *   Copyright ${year}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 * 
 * $Id$
 */

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
