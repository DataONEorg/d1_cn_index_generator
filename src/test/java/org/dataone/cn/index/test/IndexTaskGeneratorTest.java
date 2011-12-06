package org.dataone.cn.index.test;

import java.math.BigInteger;
import java.util.Date;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.dataone.cn.index.generator.IndexTaskGenerator;
import org.dataone.cn.index.task.IndexTask;
import org.dataone.cn.index.task.IndexTaskRepository;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.Subject;
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

    private static Logger logger = Logger.getLogger(IndexTaskGeneratorTest.class.getName());

    @Autowired
    private IndexTaskRepository repo;

    @Autowired
    private IndexTaskGenerator gen;

    @Test
    public void testInjection() {
        Assert.assertNotNull(repo);
        Assert.assertNotNull(gen);
    }

    @Test
    public void testDuplicateTask() {
        String pidValue = "gen-test-duplicate-" + UUID.randomUUID().toString();
        String formatValue = "CF-1.0";
        SystemMetadata smd = buildTestSysMetaData(pidValue, formatValue);
        IndexTask task = gen.processSystemMetaDataAdd(smd, "test-obj-path");
        Long taskId = task.getId();
        logger.info("***Dupe test, first id: " + taskId);
        IndexTask task2 = gen.processSystemMetaDataAdd(smd, "test-obj-path");
        Long task2Id = task2.getId();
        logger.info("***Dupe test, second id: " + task2Id);
        Assert.assertFalse(repo.exists(taskId));
        Assert.assertNull(repo.findOne(taskId));
        Assert.assertTrue(repo.exists(task2Id));
        Assert.assertNotNull(repo.findOne(task2Id));
    }

    /**
     * This simple test method is redundant to what is present in
     * IndexTaskJpaRepositoryTest. Just sanity checking repo is working - ie
     * mapping correctly
     */
    @Test
    public void testRepoAddDelete() {
        IndexTask indexTask = new IndexTask();
        indexTask.setPid("repo task-" + UUID.randomUUID().toString());
        int initialSize = repo.findAll().size();
        repo.save(indexTask);
        Assert.assertEquals(initialSize + 1, repo.findAll().size());
        repo.delete(indexTask);
        Assert.assertEquals(initialSize, repo.findAll().size());
    }

    private SystemMetadata buildTestSysMetaData(String pidValue, String formatValue) {
        SystemMetadata systemMetadata = new SystemMetadata();

        Identifier identifier = new Identifier();
        identifier.setValue(pidValue);
        systemMetadata.setIdentifier(identifier);

        ObjectFormatIdentifier fmtid = new ObjectFormatIdentifier();
        fmtid.setValue(formatValue);
        systemMetadata.setFormatId(fmtid);

        systemMetadata.setSerialVersion(BigInteger.TEN);
        systemMetadata.setSize(BigInteger.TEN);
        Checksum checksum = new Checksum();
        checksum.setValue("V29ybGQgSGVsbG8h");
        checksum.setAlgorithm("SHA-1");
        systemMetadata.setChecksum(checksum);

        Subject rightsHolder = new Subject();
        rightsHolder.setValue("DataONE");
        systemMetadata.setRightsHolder(rightsHolder);

        Subject submitter = new Subject();
        submitter.setValue("Kermit de Frog");
        systemMetadata.setSubmitter(submitter);

        systemMetadata.setDateSysMetadataModified(new Date());
        return systemMetadata;
    }
}