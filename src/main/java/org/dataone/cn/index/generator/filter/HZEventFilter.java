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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v2.SystemMetadata;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Currently the index generator is listening the events of the systemmetadata map in hazelcast. Those add, delete and 
 * update events will generate index tasks. However, some events, such as hazecast loading system metadata from the store, 
 * shouldn't generate the index tasks since there is no change. This filter is filtering those events.
 * @author tao
 *
 */
public class HZEventFilter {
    private static Logger logger = Logger.getLogger(HZEventFilter.class);
    /**
     * Here is the algorithm:
     * 1. Fetching the solr index for the pid. 
     * 2. If there is no solr index for the pid, check the archive flag in the system metadata. If the archive=true, filter this pid out; if the archive=false, keep this pid (return false).
     * 3. If there is a solr index, compare the modification date between the solr index and the system metadata.
     *    3.1 if sysmeta > solr index, return false (keep index)
     *    3.2 if sysmeta < solr index, return true (filter it out)
     *    3.3 if sysemeta = solr index, compare the replica info.
     *        3.3.1  no change on replica info, return true (filter out)
     *        3.3.2  there is a change, return false (keep index)
     * If any exception happens, it will return false for the safety.
     * @param sysmeta
     * @return true if we don't need to index it (filter out)
     */
    public boolean filter(SystemMetadata sysmeta) {
        boolean needFilterOut = true; // please don't change the default value here.
        Identifier pid = sysmeta.getIdentifier();
        String queryUrl = null;
        Document solrDoc = getSolrReponse(queryUrl);
        Vector<String> id = getValues(solrDoc, "");
        if(id.isEmpty()) {
            //no slor doc
            boolean archive = sysmeta.getArchived();
            if(archive) {
                //this is an archived object and there is no solr doc either. All set! We don't need index it.
                logger.info("HZEventFilter.filter - the system metadata for the index event shows "+pid.getValue()+" is an archived object and the SOLR server doesn't have the record either. So this event has been filtered out for indexing (no indexing).");
                needFilterOut = true;
            } else {
                logger.info("HZEventFilter.filter - the system metadata  for the index event shows shows "+pid.getValue()+" is not an archived object but the SOLR server doesn't have the record. So this event should be granted for indexing.");
                needFilterOut = false;
            }
        } else {
            Date sysDate = sysmeta.getDateSysMetadataModified();
            Date solrDate = getModificationDateInSolr(solrDoc);
            if(sysDate.getTime() > solrDate.getTime()) {
                logger.info("HZEventFilter.filter - the system metadata for the index event shows shows "+pid.getValue()+
                        " having a newer version than the SOLR server. So this event should be granted for indexing.");
                needFilterOut = false;
            } else if (sysDate.getTime() < solrDate.getTime()) {
                logger.info("HZEventFilter.filter - the system metadata for the index event shows shows "+pid.getValue()+
                        " having an older version than the SOLR server. So this event has been filtered out for indexing (no indexing).");
                needFilterOut = true;
            } else {
                // the modification date equals. we need to compare replicas
               List<Replica> sysReplicas = sysmeta.getReplicaList();
               List<Replica> solrReplicas = getReplicasInSolr(solrDoc);//it wouldn't be null
               if(sysReplicas != null ) {
                   if(sysReplicas.size() != solrReplicas.size()) {
                       //system metaddata and solr have different replica size. We need to indexing.
                       logger.info("HZEventFilter.filter - the system metadata for the index event shows shows "+pid.getValue()+" having the same modification date as the SOLR server. But they have diffrerent size of the replica list. So this event should be granted for indexing.");
                       needFilterOut = false;
                   } else {
                       //compare the replica lists. If there is not match, the needFilterOut will be set to false. Otherwise (everything matching), it will keep true value.
                       outerloop:
                       for(Replica sysReplica : sysReplicas) {
                           boolean found = false;
                           boolean haveDifferentVerificationDate = false;
                           NodeReference sysNode = sysReplica.getReplicaMemberNode();
                           Date sysConfirmDate = sysReplica.getReplicaVerified();
                           for(Replica solrReplica : solrReplicas) {
                               NodeReference solrNode = sysReplica.getReplicaMemberNode();
                               Date solrConfirmDate = sysReplica.getReplicaVerified();
                               if(sysNode.equals(solrNode)) {
                                   found = true;
                                   if(sysConfirmDate.getTime() != solrConfirmDate.getTime()) {
                                     //they have the different verification date. We need to index
                                       haveDifferentVerificationDate = true;
                                   } 
                               }
                               
                               if(found && haveDifferentVerificationDate) {
                                   // we found the node but has different verification date. We need to break the loop (ignore others) and grant the event
                                   //system metaddata has an empty replica list but solr doesn't. We need to indexing.
                                   logger.info("HZEventFilter.filter - the system metadata for the index event shows shows "+pid.getValue()+" having the same modification date as the SOLR server. But at least one of the replica has different verified date. So this event should be granted for indexing.");
                                   needFilterOut = false;
                                   break outerloop;
                               }
                           }
                           if(!found) {
                               logger.info("HZEventFilter.filter - the system metadata for the index event shows shows "+pid.getValue()+" having the same modification date as the SOLR server. But at least one of the replica in system metadata can't be found on solr. So this event should be granted for indexing.");
                               needFilterOut = false;
                               break;
                           }
                       }
                       
                       if((needFilterOut)) {
                            //we found that everything match in replica list. So we should filter the event (no indexing)
                           //this block is only for log information
                           logger.info("HZEventFilter.filter - the system metadata for the index event shows shows "+pid.getValue()+
                                   " having the same modification date as the SOLR server. Also both have the same replica list. So this event has been filtered out for indexing (no indexing).");
                       }
                   }
               } else if(solrReplicas.isEmpty()) {
                   //both slor and system metadata has an empty replica list. we should filter out this even, so no indexing.
                   logger.info("HZEventFilter.filter - the system metadata for the index event shows shows "+pid.getValue()+
                           " having the same modification date as the SOLR server. Also both have an emply replica list. So this event has been filtered out for indexing (no indexing).");
                   needFilterOut = true;
               } else {
                   //system metaddata has an empty replica list but solr doesn't. We need to indexing.
                   logger.info("HZEventFilter.filter - the system metadata for the index event shows shows "+pid.getValue()+" having the same modification date as the SOLR server. But the system metadata for the event has an empty replica list while the solr doesn't. So this event should be granted for indexing.");
                   needFilterOut = false;
               }
            }
        }
        return needFilterOut;
    }
    
    
    /**
     * Parse the solr doc to get the replica information
     * @param doc
     * @return an empty list if there is no replica information
     */
    List<Replica>  getReplicasInSolr(Document doc) {
        List<Replica> replicas = new ArrayList<Replica>();
        return replicas;
    }
    
    Date getModificationDateInSolr(Document doc) {
        Date date = null;
        return date;
    }
    
    /**
     * Query the document by a given path query
     * @param doc
     * @param pathQuery
     * @return an empty vector if this no match
     */
    Vector<String> getValues(Document doc, String pathQuery) {
        Vector<String> values = new Vector<String> ();
        return values;
    }
    
    /**
     * Query solr to get the response
     * @param url
     * @return
     */
     Document getSolrReponse(String url) {
        Document result = null;
        return result;
        
    }
}
