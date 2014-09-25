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

package org.dataone.cn.index.test;

import static org.junit.Assert.fail;
import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.dataone.cn.index.generator.IndexTaskGeneratorEntryListener;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v2.SystemMetadata;
import org.dataone.service.util.TypeMarshaller;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

@RunWith(SpringJUnit4ClassRunner.class)
// context files are located from the root of the test's classpath
// for example org/dataone/cn/index/test/
@ContextConfiguration(locations = { "test-context.xml" })
public class IndexTaskGeneratorEntryListenerTest {

    private static Logger logger = Logger.getLogger(IndexTaskGeneratorEntryListenerTest.class
            .getName());

    private HazelcastInstance hzMember;
    private IMap<Identifier, SystemMetadata> sysMetaMap;
    private IMap<Identifier, String> objectPaths;

    @Autowired
    IndexTaskGeneratorEntryListener listener;

    private static final String systemMetadataMapName = Settings.getConfiguration().getString(
            "dataone.hazelcast.systemMetadata");

    private static final String objectPathName = Settings.getConfiguration().getString(
            "dataone.hazelcast.objectPath");

    @Autowired
    private Resource systemMetadataResource;

    @Before
    public void setUp() throws Exception {

        Config hzConfig = new ClasspathXmlConfig("org/dataone/configuration/hazelcast.xml");

        System.out.println("Hazelcast Group Config:\n" + hzConfig.getGroupConfig());
        System.out.print("Hazelcast Maps: ");
        for (String mapName : hzConfig.getMapConfigs().keySet()) {
            System.out.print(mapName + " ");
        }
        System.out.println();
        hzMember = Hazelcast.newHazelcastInstance(hzConfig);
        System.out.println("Hazelcast member hzMember name: " + hzMember.getName());

        sysMetaMap = hzMember.getMap(systemMetadataMapName);
        objectPaths = hzMember.getMap(objectPathName);
    }

    @After
    public void tearDown() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testIndexTaskGeneration() {

        // create a new SystemMetadata object for testing
        SystemMetadata sysmeta = null;
        try {
            sysmeta = TypeMarshaller.unmarshalTypeFromStream(SystemMetadata.class,
                    systemMetadataResource.getInputStream());
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            fail("Test SystemMetadata misconfiguration - Exception " + ex);
        }

        try {
            listener.start();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        if (!sysMetaMap.containsKey(sysmeta.getIdentifier())) {
            objectPaths.putAsync(sysmeta.getIdentifier(), "test gen listener object path");
            sysMetaMap.put(sysmeta.getIdentifier(), sysmeta);
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }
        sysMetaMap.remove(sysmeta.getIdentifier());

        try {
            // processing time
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }

        try {
            listener.stop();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        Assert.assertTrue(true);
    }
}