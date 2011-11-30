package org.dataone.cn.index.generator;

import org.dataone.cn.index.task.IndexTask;
import org.dataone.cn.index.task.IndexTaskRepository;
import org.dataone.service.types.v1.SystemMetadata;
import org.springframework.beans.factory.annotation.Autowired;

public class IndexTaskGenerator {

    @Autowired
    private IndexTaskRepository repo;

    public IndexTask processSystemMetaDataUpdate(SystemMetadata smd) {
        IndexTask task = new IndexTask(smd);
        repo.save(task);
        return task;
    }
}
