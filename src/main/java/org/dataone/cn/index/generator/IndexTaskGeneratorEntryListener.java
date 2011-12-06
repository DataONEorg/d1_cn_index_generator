package org.dataone.cn.index.generator;

import org.apache.log4j.Logger;
import org.dataone.cn.hazelcast.HazelcastClientInstance;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.SystemMetadata;
import org.springframework.beans.factory.annotation.Autowired;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class IndexTaskGeneratorEntryListener implements EntryListener<Identifier, SystemMetadata> {

    private static Logger logger = Logger
            .getLogger(IndexTaskGeneratorEntryListener.class.getName());

    @Autowired
    private IndexTaskGenerator generator;

    private HazelcastInstance hzClient;

    private static final String HZ_SYSTEM_METADATA = Settings.getConfiguration().getString(
            "dataone.hazelcast.systemMetadata");

    private static final String HZ_OBJECT_PATH = Settings.getConfiguration().getString(
            "dataone.hazelcast.objectPath");

    private IMap<Identifier, SystemMetadata> systemMetadata;
    private IMap<Identifier, String> objectPaths = null;

    public IndexTaskGeneratorEntryListener() {

    }

    public void start() {
        logger.info("starting index task generator entry listener...");
        this.hzClient = HazelcastClientInstance.getHazelcastClient();
        this.systemMetadata = this.hzClient.getMap(HZ_SYSTEM_METADATA);
        this.objectPaths = this.hzClient.getMap(HZ_OBJECT_PATH);
        this.systemMetadata.addEntryListener(this, true);
    }

    public void stop() {
        logger.info("stopping index task generator entry listener...");
        this.systemMetadata.removeEntryListener(this);
        this.hzClient.getLifecycleService().shutdown();
    }

    @Override
    public void entryAdded(EntryEvent<Identifier, SystemMetadata> event) {
        logger.info("index task generator - added system metadata callback invoked...");
        generator.processSystemMetaDataAdd(event.getValue(), getObjectPath(event));

    }

    @Override
    public void entryUpdated(EntryEvent<Identifier, SystemMetadata> event) {
        logger.info("index task generator - updated system metadata callback invoked...");
        generator.processSystemMetaDataUpdate(event.getValue(), getObjectPath(event));
    }

    private String getObjectPath(EntryEvent<Identifier, SystemMetadata> event) {
        return objectPaths.get(event.getKey().getValue());
    }

    @Override
    public void entryEvicted(EntryEvent<Identifier, SystemMetadata> arg0) {
    }

    @Override
    public void entryRemoved(EntryEvent<Identifier, SystemMetadata> arg0) {
    }

}
