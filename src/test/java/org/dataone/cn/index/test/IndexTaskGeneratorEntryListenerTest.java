package org.dataone.cn.index.test;

import static org.junit.Assert.fail;
import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.dataone.cn.index.generator.IndexTaskGeneratorDaemon;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.util.TypeMarshaller;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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
    private Config hzConfig;

    private static final String systemMetadataMapName = Settings.getConfiguration().getString(
            "dataone.hazelcast.systemMetadata");

    private static final String objectPathName = Settings.getConfiguration().getString(
            "dataone.hazelcast.objectPath");

    @Autowired
    private IndexTaskGeneratorDaemon daemon;

    @Autowired
    @Qualifier("readSystemMetadataResource")
    private org.springframework.core.io.Resource readSystemMetadataResource;

    @Before
    public void setUp() throws Exception {

        hzConfig = new ClasspathXmlConfig("org/dataone/configuration/hazelcast.xml");

        System.out.println("Hazelcast Group Config:\n" + hzConfig.getGroupConfig());
        System.out.print("Hazelcast Maps: ");
        for (String mapName : hzConfig.getMapConfigs().keySet()) {
            System.out.print(mapName + " ");
        }
        System.out.println();
        hzMember = Hazelcast.init(hzConfig);
        System.out.println("Hazelcast member hzMember name: " + hzMember.getName());
    }

    @After
    public void tearDown() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testCreateAndQueueTasks() {

        // create a new SystemMetadata object for testing
        SystemMetadata sysmeta = null;
        try {
            sysmeta = TypeMarshaller.unmarshalTypeFromStream(SystemMetadata.class,
                    readSystemMetadataResource.getInputStream());
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            fail("Test SystemMetadata misconfiguration - Exception " + ex);
        }

        try {
            daemon.start();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        sysMetaMap = hzMember.getMap(systemMetadataMapName);

        objectPaths = hzMember.getMap(objectPathName);

        sysMetaMap.putAsync(sysmeta.getIdentifier(), sysmeta);
        objectPaths.putAsync(sysmeta.getIdentifier(), "test gen listener object path");

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }

        try {
            daemon.stop();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        Assert.assertTrue(true);
    }
}