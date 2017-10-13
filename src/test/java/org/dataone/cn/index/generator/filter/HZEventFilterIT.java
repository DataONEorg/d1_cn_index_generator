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

package org.dataone.cn.index.generator.filter;

import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.apache.solr.common.SolrDocument;
import org.dataone.cn.index.generator.IndexTaskGeneratorEntryListener;
import org.dataone.cn.index.task.IndexTask;
import org.dataone.cn.index.task.IndexTaskRepository;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.Replica;
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


@RunWith(SpringJUnit4ClassRunner.class)
//context files are located from the root of the test's classpath
//for example org/dataone/cn/index/test/
@ContextConfiguration(locations = { "../../test/test-context.xml" })

/**
 * This is IT. You should connect the solr server on cn-orc-1 by setting a ssh tunnel first:
 *  ssh -L 8983:localhost:8983  cn-orc-1.dataone.org
 * 
 * @author tao
 *
 */
public class HZEventFilterIT {
    
    @Before
    public void setUp() throws Exception {
        
    }
    
    @After
    public void tearDown() throws Exception {
        
    }
    
    @Test
    public void testgetSolrReponse() throws Exception {
        //Info maually read from solr doc
        String id = "doi:10.18739/A2J32X";
        String sysModificationDate = "2017-08-07T20:58:57.294Z";
        String replica1= "urn:node:ARCTIC";
        String vDate1= "2016-11-08T03:33:31.026Z";
        String replica2= "urn:node:CN";
        String vDate2= "2016-11-08T03:33:31.030Z";
        String replica3= "urn:node:mnUCSB1";
        String vDate3= "2017-08-10T05:50:18.659Z";
        String replica4= "urn:node:mnORC1";
        String vDate4= "2017-08-10T05:50:21.713Z";
        String replica5= "urn:node:KNB";
        String vDate5= "2017-08-10T05:50:26.686Z";
        String serialV="2";
        
        SystemMetadata sysmeta = new SystemMetadata();
        //id
        Identifier pid = new Identifier();
        pid.setValue(id);
        sysmeta.setIdentifier(pid);
        // modification date
        String ISO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
        SimpleDateFormat isoFormatter = new SimpleDateFormat(ISO_FORMAT);
        Date sysDate = isoFormatter.parse(sysModificationDate);
        sysmeta.setDateSysMetadataModified(sysDate);
        //replica list
        List<Replica> replicas = new ArrayList<Replica> ();
        Replica replicaMN1 = new Replica();
        NodeReference node1 = new NodeReference();
        node1.setValue(replica1);
        replicaMN1.setReplicaMemberNode(node1);
        replicaMN1.setReplicaVerified(isoFormatter.parse(vDate1));
        replicas.add(replicaMN1);
        
        Replica replicaMN2 = new Replica();
        NodeReference node2 = new NodeReference();
        node2.setValue(replica2);
        replicaMN2.setReplicaMemberNode(node2);
        replicaMN2.setReplicaVerified(isoFormatter.parse(vDate2));
        replicas.add(replicaMN2);
        
        Replica replicaMN3 = new Replica();
        NodeReference node3 = new NodeReference();
        node3.setValue(replica3);
        replicaMN3.setReplicaMemberNode(node3);
        replicaMN3.setReplicaVerified(isoFormatter.parse(vDate3));
        replicas.add(replicaMN3);
        
        Replica replicaMN4 = new Replica();
        NodeReference node4 = new NodeReference();
        node4.setValue(replica4);
        replicaMN4.setReplicaMemberNode(node4);
        replicaMN4.setReplicaVerified(isoFormatter.parse(vDate4));
        replicas.add(replicaMN4);
        
        Replica replicaMN5 = new Replica();
        NodeReference node5 = new NodeReference();
        node5.setValue(replica5);
        replicaMN5.setReplicaMemberNode(node5);
        replicaMN5.setReplicaVerified(isoFormatter.parse(vDate5));
        replicas.add(replicaMN5);
        
        sysmeta.setReplicaList(replicas);
        
        //compare - it should return true since there is not change.
        HZEventFilter filter = new HZEventFilter();
        Assert.assertTrue(filter.filter(sysmeta));
        
        //a newer (bigger) modification date, we should grant it
        String newModificationDate = "2017-09-07T20:58:57.294Z";
        sysmeta.setDateSysMetadataModified(isoFormatter.parse(newModificationDate));
        Assert.assertTrue(!filter.filter(sysmeta));
        
        // an older (smaller) modification date, we should filter out.
        String oldModificationDate = "2017-01-07T20:58:57.294Z";
        sysmeta.setDateSysMetadataModified(isoFormatter.parse(oldModificationDate));
        Assert.assertTrue(filter.filter(sysmeta));
        
        //even though removed a replica. But the still has an older modification date, filter it out
        replicas.remove(4);
        Assert.assertTrue(filter.filter(sysmeta));
        
        //set back the original modification date, but we remove a replica. So the index should be granted
        sysmeta.setDateSysMetadataModified(isoFormatter.parse(sysModificationDate));
        Assert.assertTrue(!filter.filter(sysmeta));
        
        //has the same mofication date, but a replica has different verification date
        Replica replicaMN6 = new Replica();
        NodeReference node6 = new NodeReference();
        node6.setValue(replica5);
        replicaMN6.setReplicaMemberNode(node6);
        String vDate6= "2017-10-10T05:50:26.686Z";
        replicaMN6.setReplicaVerified(isoFormatter.parse(vDate6));
        replicas.add(replicaMN6);
        Assert.assertTrue(!filter.filter(sysmeta));
        
        
    }

}
