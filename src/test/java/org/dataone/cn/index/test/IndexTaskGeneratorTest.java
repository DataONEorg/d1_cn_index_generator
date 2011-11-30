package org.dataone.cn.index.test;

import java.util.Date;
import java.util.UUID;

import org.dataone.cn.index.generator.IndexTaskGenerator;
import org.dataone.cn.index.task.IndexTask;
import org.dataone.cn.index.task.IndexTaskRepository;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.SystemMetadata;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
// context files are located from the root of the test's classpath
// for example org/dataone/cn/index/test/
@ContextConfiguration(locations = { "test-context.xml" })
public class IndexTaskGeneratorTest {

    @Autowired
    private IndexTaskRepository repo;

    @Autowired
    private IndexTaskGenerator gen;

    @Test
    public void testInjection() {
        Assert.assertNotNull(repo);
        Assert.assertNotNull(gen);
    }

    // This test method is redundant to what is present in
    // IndexTaskJpaRepositoryTest.
    // Just sanity checking repo is working.
    @Test
    public void testRepoAddDelete() {
        IndexTask indexTask = new IndexTask();
        indexTask.setPid("generated-task");
        int initialSize = repo.findAll().size();
        repo.save(indexTask);
        Assert.assertEquals(initialSize + 1, repo.findAll().size());
        repo.delete(indexTask);
        Assert.assertEquals(initialSize, repo.findAll().size());
    }

    @Test
    public void testAddTask() {
        String pidValue = "task-test-built-" + UUID.randomUUID().toString();
        String formatValue = "CF-1.0";
        SystemMetadata smd = buildTestSysMetaData(pidValue, formatValue);
        IndexTask task = gen.processSystemMetaDataUpdate(smd);
        task = repo.findOne(task.getId());
        Assert.assertEquals(0, pidValue.compareTo(task.getPid()));
        Assert.assertEquals(0, formatValue.compareTo(task.getFormatId()));
    }

    private SystemMetadata buildTestSysMetaData(String pidValue, String formatValue) {
        SystemMetadata systemMetadata = new SystemMetadata();

        Identifier identifier = new Identifier();
        identifier.setValue(pidValue);
        systemMetadata.setIdentifier(identifier);

        ObjectFormatIdentifier fmtid = new ObjectFormatIdentifier();
        fmtid.setValue(formatValue);
        systemMetadata.setFormatId(fmtid);

        systemMetadata.setDateSysMetadataModified(new Date());
        return systemMetadata;
    }

}
