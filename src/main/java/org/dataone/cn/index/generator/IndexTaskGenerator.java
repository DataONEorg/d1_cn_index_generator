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

import java.util.List;

import org.apache.log4j.Logger;
import org.dataone.cn.index.task.IgnoringIndexIdPool;
import org.dataone.cn.index.task.IndexTask;
import org.dataone.cn.index.task.IndexTaskRepository;
import org.dataone.cn.index.util.PerformanceLogger;
import org.dataone.service.types.v2.SystemMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.orm.hibernate3.HibernateOptimisticLockingFailureException;

/**
 * IndexTaskGenerator is a strategy implementation for index processing to
 * handle updates and additions to the distributed system meta data map which
 * trigger the necessity to update the search index. This update to the search
 * index is represented by a new IndexTask instance.
 * 
 * @author sroseboo
 * 
 */
public class IndexTaskGenerator {

    private static Logger logger = Logger.getLogger(IndexTaskGenerator.class.getName());
    //private static final String IGNOREPID = "OBJECT_FORMAT_LIST.1.1";
    private static PerformanceLogger perfLog = PerformanceLogger.getInstance();

    @Autowired
    private IndexTaskRepository repo;

    /**
     * Call when system metadata add events are detected, to trigger new
     * IndexTask instance generation.
     * 
     * @param SystemMetadata
     * @return IndexTask
     */
    public IndexTask processSystemMetaDataAdd(SystemMetadata smd, String objectPath) {
        if (IgnoringIndexIdPool.isNotIgnorePid(smd)) {
            long start = System.currentTimeMillis();
            removeDuplicateNewTasks(smd);
            IndexTask task = new IndexTask(smd, objectPath);
            task.setAddPriority();
            task = repo.save(task);
            String id = "Unknow";
            if(smd != null && smd.getIdentifier() != null) {
                id = smd.getIdentifier().getValue();
            }
            perfLog.log("IdexTaskGenerator.processSystemMetaDataAdd add a adding task for id "+id, System.currentTimeMillis() - start);
            return task;
        }
        return null;
    }

    /**
     * Call when system metadata update events are detected, to trigger new
     * IndexTask instance generation.
     * 
     * @param SystemMetadata
     * @return IndexTask
     */
    public IndexTask processSystemMetaDataUpdate(SystemMetadata smd, String objectPath) {
        if (IgnoringIndexIdPool.isNotIgnorePid(smd)) {
            long start = System.currentTimeMillis();
            removeDuplicateNewTasks(smd);
            IndexTask task = new IndexTask(smd, objectPath);
            task.setUpdatePriority();
            task = repo.save(task);
            String id = "Unknow";
            if(smd != null && smd.getIdentifier() != null) {
                id = smd.getIdentifier().getValue();
            }
            perfLog.log("IdexTaskGenerator.processSystemMetaDataUpdate add a updating task for id "+id, System.currentTimeMillis() - start);
            return task;
        }
        return null;
    }

    /**
     * Call when system metadata remove events are detected, to trigger new
     * IndexTask instance generation to represent the removal of an item from the index.
     * 
     * @param SystemMetadata
     * @return IndexTask
     */
    public IndexTask processSystemMetaDataDelete(SystemMetadata smd) {
        if (IgnoringIndexIdPool.isNotIgnorePid(smd)) {
            long start = System.currentTimeMillis();
            removeDuplicateNewTasks(smd);
            IndexTask task = new IndexTask(smd, null);
            task.setDeleted(true);
            task = repo.save(task);
            String id = "Unknow";
            if(smd != null && smd.getIdentifier() != null) {
                id = smd.getIdentifier().getValue();
            }
            perfLog.log("IdexTaskGenerator.processSystemMetaDataUpdate add a deleting task for id "+id, System.currentTimeMillis() - start);
            return task;
        }
        return null;
    }

    /*private boolean isNotIgnorePid(SystemMetadata smd) {
        if (IGNOREPID.equals(smd.getIdentifier().getValue())) {
            return false;
        }
        return true;
    }*/

    /**
     * Find unprocessed (new) tasks and remove. Will be replaced by new version
     * of the task
     * 
     * @param SystemMetadata
     */
    private void removeDuplicateNewTasks(SystemMetadata smd) {
        removeDuplicateTasksByStatus(smd, IndexTask.STATUS_NEW);
        // new update on this pid, so remove failure and try to reprocess.
        removeDuplicateTasksByStatus(smd, IndexTask.STATUS_FAILED);
    }

    private void removeDuplicateTasksByStatus(SystemMetadata smd, String status) {
        List<IndexTask> itList = repo.findByPidAndStatus(smd.getIdentifier().getValue(), status);
        for (IndexTask indexTask : itList) {
            try {
                repo.delete(indexTask);
            } catch (HibernateOptimisticLockingFailureException e) {
                logger.debug("Unable to delete existing index task for pid: " + indexTask.getPid()
                        + " prior to generating new index task.");
            }
        }
    }
}
