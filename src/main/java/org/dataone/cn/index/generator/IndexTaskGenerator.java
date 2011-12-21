package org.dataone.cn.index.generator;

import java.util.List;

import org.apache.log4j.Logger;
import org.dataone.cn.index.task.IndexTask;
import org.dataone.cn.index.task.IndexTaskRepository;
import org.dataone.service.types.v1.SystemMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.orm.hibernate3.HibernateOptimisticLockingFailureException;

public class IndexTaskGenerator {

    private static Logger logger = Logger.getLogger(IndexTaskGenerator.class.getName());
    private static final String IGNOREPID = "OBJECT_FORMAT_LIST.1.1";

    @Autowired
    private IndexTaskRepository repo;

    /**
     * Entry point for hzCast systemMetadata map item added event listener
     * 
     * @param SystemMetadata
     * @return IndexTask
     */
    public IndexTask processSystemMetaDataAdd(SystemMetadata smd, String objectPath) {
        if (isNotIgnorePid(smd)) {
            removeDuplicateNewTasks(smd);
            IndexTask task = new IndexTask(smd, objectPath);
            task.setAddPriority();
            repo.save(task);
            return task;
        }
        return null;
    }

    /**
     * Entry point for hzCast systemMetadata map item updated event listener.
     * 
     * @param SystemMetadata
     * @return IndexTask
     */
    public IndexTask processSystemMetaDataUpdate(SystemMetadata smd, String objectPath) {
        if (isNotIgnorePid(smd)) {
            removeDuplicateNewTasks(smd);
            IndexTask task = new IndexTask(smd, objectPath);
            task.setUpdatePriority();
            repo.save(task);
            return task;
        }
        return null;
    }

    private boolean isNotIgnorePid(SystemMetadata smd) {
        if (IGNOREPID.equals(smd.getIdentifier().getValue())) {
            return false;
        }
        return true;
    }

    /**
     * Find unprocessed (new) tasks and remove. Will be replaced by new version
     * of the task
     * 
     * @param SystemMetadata
     */
    private void removeDuplicateNewTasks(SystemMetadata smd) {
        List<IndexTask> itList = repo.findByPidAndStatus(smd.getIdentifier().getValue(),
                IndexTask.STATUS_NEW);
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
