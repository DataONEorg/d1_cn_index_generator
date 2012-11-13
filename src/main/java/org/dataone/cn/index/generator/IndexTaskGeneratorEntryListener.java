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

import org.apache.log4j.Logger;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.SystemMetadata;
import org.springframework.beans.factory.annotation.Autowired;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.IMap;

/**
 * An implementation of a Hazelcast EntryListener interface for the system
 * metadata distributed data structure. Delegates to IndexTaskGenerator class to
 * handle IndexTask creation.
 * 
 * @author sroseboo
 * 
 */
public class IndexTaskGeneratorEntryListener implements EntryListener<Identifier, SystemMetadata> {

    private static Logger logger = Logger
            .getLogger(IndexTaskGeneratorEntryListener.class.getName());

    @Autowired
    private IndexTaskGenerator generator;

    private HazelcastClient hzClient;

    private static final String HZ_SYSTEM_METADATA = Settings.getConfiguration().getString(
            "dataone.hazelcast.systemMetadata");

    private static final String HZ_OBJECT_PATH = Settings.getConfiguration().getString(
            "dataone.hazelcast.objectPath");

    private IMap<Identifier, SystemMetadata> systemMetadata;
    private IMap<Identifier, String> objectPaths;

    public IndexTaskGeneratorEntryListener() {

    }

    /**
     * Register this instance as a system metadata map event listener.
     */
    public void start() {
        logger.info("starting index task generator entry listener...");
        logger.info("System Metadata value: " + HZ_SYSTEM_METADATA);
        logger.info("Object path value: " + HZ_OBJECT_PATH);

        this.hzClient = HazelcastClientFactory.getStorageClient();
        this.systemMetadata = this.hzClient.getMap(HZ_SYSTEM_METADATA);
        this.objectPaths = this.hzClient.getMap(HZ_OBJECT_PATH);
        this.systemMetadata.addEntryListener(this, true);

        logger.info("System Metadata size: " + this.systemMetadata.size());
        logger.info("Object path size:" + this.objectPaths.size());
    }

    /**
     * Removes this instance as a system metadata map event listener.
     */
    public void stop() {
        logger.info("stopping index task generator entry listener...");
        this.systemMetadata.removeEntryListener(this);
    }

    /**
     * EntryListener interface method. Invoked when an entry is updated in
     * system metadata map. Delegates IndexTask creation behavior to
     * IndexTaskGenerator.
     */
    @Override
    public void entryUpdated(EntryEvent<Identifier, SystemMetadata> event) {
        logger.info("UPDATE EVENT - index task generator - system metadata callback invoked on pid: "
                + event.getKey().getValue());
        generator.processSystemMetaDataUpdate(event.getValue(), getObjectPath(event));
    }

    /**
     * EntryListener interface method. Invoked when an entry is added to the
     * system metadata map. Delegates IndexTask creation behavior to
     * IndexTaskGenerator.
     */
    @Override
    public void entryAdded(EntryEvent<Identifier, SystemMetadata> event) {
        if (event.getKey() != null && event.getValue() != null) {
            logger.info("ADD EVENT - index task generator - system metadata callback invoked on pid: "
                    + event.getKey().getValue());
            SystemMetadata smd = event.getValue();
            if (smd.getSerialVersion().longValue() > 1) {
                logger.info("Add event for pid: " + event.getKey().getValue()
                        + " determined to be invalid due to serial version: "
                        + smd.getSerialVersion().longValue() + ".  skipping add index task.");
            } else {
                logger.info("Processing add event index task for pid: " + event.getKey().getValue());
                generator.processSystemMetaDataAdd(event.getValue(), getObjectPath(event));
            }
        }
    }

    private String getObjectPath(EntryEvent<Identifier, SystemMetadata> event) {
        return objectPaths.get(event.getKey());
    }

    /**
     * EntryListener interface method for map evicted events. Unused in this
     * class. No behavior.
     */
    @Override
    public void entryEvicted(EntryEvent<Identifier, SystemMetadata> arg0) {
    }

    /**
     * EntryListener interface method for entry removed events. Unused in this
     * class. No behavior.
     */
    @Override
    public void entryRemoved(EntryEvent<Identifier, SystemMetadata> arg0) {
    }

}
