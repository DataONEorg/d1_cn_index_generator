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

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v2.SystemMetadata;



/**
 * Currently the index generator is listening the events of the systemmetadata map in hazelcast. Those add, delete and 
 * update events will generate index tasks. However, some events, such as hazecast loading system metadata from the store, 
 * shouldn't generate the index tasks since there is no change. This filter is filtering those events.
 * @author tao
 *
 */
public class HZEventFilter {
    private static Logger logger = Logger.getLogger(HZEventFilter.class);
    private static String INDEX_EVENT_FILTERING_ACTIVE = "IndexEvent.filtering.active";
    private static String ID = "id";
    private static String DATEMODIFIED = "dateModified";
    private static String REPLICAMN = "replicaMN";
    private static String REPLICAVERIFIEDATE = "replicaVerifiedDate";
    private static String SERIALVERSION = "serialVersion";
    private static int FIRSTSOLRDOCINDEX = 0;

    private String solrBaseURL = null;
    private SolrClient client = null;

    /**
     * Constructor.
     * Initialize the solr client
     */
    public HZEventFilter() {
        solrBaseURL = Settings.getConfiguration().getString("solr.base.uri", "http://localhost:8983/solr/search_core");
        logger.info("HZEvetFilter.constructor - the base url is "+solrBaseURL);
        client = new HttpSolrClient(solrBaseURL);
    }
    
    /**
     * Here is the algorithm:
     * 1. Fetching the solr index for the pid. 
     * 2. If there is no solr index for the pid, 
     *    2.1 check the archive flag in the system metadata. If the archive=true, filter this pid out
     *    2.2  if the archive=false, keep this pid (return false).
     * 3. If there is a solr index, compare the modification date between the solr index and the system metadata.
     *    3.1 if sysmeta > solr index, return false (keep index)
     *    3.2 if sysmeta < solr index, return true (filter it out)
     *    3.3 if sysemeta = solr index, compare the replica info.
     *        3.3.1 If serialVersion in the solr is available, compare the value.
     *              3.3.1.1 If sysmeta = solr, return true (filter it out) since no change in replica
     *              3.3.1.2 If sysmeta != solr, return false (keep index) since there is a change
     *        3.3.2. If serialVersion in solr is Not availabe, comare replica lists:
     *              3.3.2.1 no change on replica info, return true (filter out)
     *              3.3.2.2  there is a change, return false (keep index)
     * If any exception happens, it will return false for safet.
     * @param sysmeta
     * @return true if we don't need to index it (filter out)
     */
    public boolean filter(SystemMetadata sysmeta) {
        boolean needFilterOut = true; 
        Identifier pid = sysmeta.getIdentifier();
        boolean enableFiltering = Settings.getConfiguration().getBoolean(INDEX_EVENT_FILTERING_ACTIVE, true);
        if(enableFiltering) {
            try {
                if(client == null) {
                    client = new HttpSolrClient(solrBaseURL);
                }
                String queryUrl = null;
                SolrDocument solrDoc = getSolrReponse(queryUrl); //step 1
                String id = getId(solrDoc);
                if(id == null) { //step 2
                    //no slor doc
                    boolean archive = sysmeta.getArchived();
                    if(archive) {
                        //2.1
                        //this is an archived object and there is no solr doc either. All set! We don't need index it.
                        logger.info("HZEventFilter.filter - the system metadata for the index event shows "+pid.getValue()+" is an archived object and the SOLR server doesn't have the record either. So this event has been filtered out for indexing (no indexing).");
                        needFilterOut = true;
                    } else {
                        //2.2
                        logger.info("HZEventFilter.filter - the system metadata  for the index event shows shows "+pid.getValue()+" is not an archived object but the SOLR server doesn't have the record. So this event should be granted for indexing.");
                        needFilterOut = false;
                    }
                } else {
                    Date sysDate = sysmeta.getDateSysMetadataModified();
                    Date solrDate = getModificationDateInSolr(solrDoc);
                    if(sysDate.getTime() > solrDate.getTime()) {
                        //3.1
                        logger.info("HZEventFilter.filter - the system metadata for the index event shows shows "+pid.getValue()+
                                " having a newer version than the SOLR server. So this event should be granted for indexing.");
                        needFilterOut = false;
                    } else if (sysDate.getTime() < solrDate.getTime()) {
                        //3.2
                        logger.info("HZEventFilter.filter - the system metadata for the index event shows shows "+pid.getValue()+
                                " having an older version than the SOLR server. So this event has been filtered out for indexing (no indexing).");
                        needFilterOut = true;
                    } else {
                        //3.3
                        // the modification date equals. we need to compare replicas
                       BigInteger sysSerial = sysmeta.getSerialVersion();
                       BigInteger solrSerial = getSerialVersion(solrDoc);//It is a new solr field and it can be null.
                       if(solrSerial != null) {
                           //3.3.1.1 If sysmeta = solr, return true (filter it out) since no change in replica
                          if(solrSerial.equals(sysSerial)) {
                              //3.3.1.1
                              logger.info("HZEventFilter.filter - the system metadata for the index event shows shows "+pid.getValue()+
                                      " having the same modification date and serial version in the solr document. So this event has been filtered out for indexing (no indexing).");
                              needFilterOut = true;
                          } else {
                              //3.3.1.2 If sysmeta != solr, return false (keep index) since there is a change
                              logger.info("HZEventFilter.filter - the system metadata for the index event shows shows "+pid.getValue()+
                                      " having the same modification date but a different serial version in the solr document. So this event should be granted for indexing.");
                              needFilterOut = false;
                          }
                       } else {
                           //3.3.2. If serialVersion in solr is Not availabe, comare replica lists (serilaVersion is a new added solr field)
                           List<Replica> sysReplicas = sysmeta.getReplicaList();
                           List<Replica> solrReplicas = getReplicasInSolr(solrDoc);//it wouldn't be null
                           boolean equal = compareRaplicaList(pid, sysReplicas, solrReplicas);
                           if(equal) {
                               //3.3.2.1
                               logger.info("HZEventFilter.filter - the system metadata for the index event shows "+pid.getValue()+
                                       " having the same modification date as the SOLR server. Also both have the same replica list. So this event has been filtered out for indexing (no indexing).");
                              needFilterOut = true;
                           } else {
                               //3.3.2.2
                               logger.info("HZEventFilter.filter - the system metadata for the index event shows "+pid.getValue()+
                                       " having the same modification date as the SOLR server. However, they have different replica lists. So this event should be granted for indexing.");
                               needFilterOut = false;
                           }
                       }
                      
                    }
                }
            } catch (Exception e) {
                logger.warn("HZEventFilter.filter - there was an exception in comparing the solr record for "+pid.getValue()+
                        " to its system metadata. However, this event still should be granted for indexing for safe.", e);
                needFilterOut = false;
            }
        } else {
            logger.info("HZEventFilter.filter - The filter was disable by setting IndexEvent.filtering.active=false. So the index event for "+pid.getValue()+" should be granted for indexing.");
            needFilterOut = false;
        }
        return needFilterOut;
    }
    
    /**
     * Close the solr client
     * @throws IOException
     */
    public void closeSolrClient() throws IOException {
        if(client != null) {
            client.close();
        }
    }
    
    /**
     * Compare two replica list from system metadata and slor doc. The one from solr doc should be null, but it can be empty.
     * Since the solr doc only have the replica name and verified date, we only compare those two items.
     * @param pid
     * @param sysReplicas
     * @param solrReplicas
     * @return true if the replicat lists are the same.
     */
    boolean compareRaplicaList(Identifier pid, List<Replica> sysReplicas, List<Replica> solrReplicas) {
        boolean equal = true;
        if(sysReplicas != null ) {
            if(sysReplicas.size() != solrReplicas.size()) {
                //system metaddata and solr have different replica size. We need to indexing.
                logger.debug("HZEventFilter.compareRaplicaList - the system metadata for the index event hows "+pid.getValue()+" having diffrerent size of the replica list to the solr doc. Not the same");
                equal = false;
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
                            logger.debug("HZEventFilter.compareReplicaList - the system metadata for the index event shows "+pid.getValue()+" having at least one of the replica has different verified date to solr doc. Not the same.");
                            equal = false;
                            break outerloop;
                        }
                    }
                    if(!found) {
                        logger.debug("HZEventFilter.compare - the system metadata for the index event shows "+pid.getValue()+" having at least one of the replica which can't be found on the solr doc. Not the same.");
                        equal = false;
                        break;
                    }
                }
                
                if((equal)) {
                     //we found that everything match in replica list. So we should filter the event (no indexing)
                    //this block is only for log information
                    logger.debug("HZEventFilter.compare - the system metadata for the index event shows "+pid.getValue()+
                            " having the same replica list as the solr doc.");
                    equal = true;
                }
            }
        } else if(solrReplicas.isEmpty()) {
            //both slor and system metadata has an empty replica list. we should filter out this even, so no indexing.
            logger.debug("HZEventFilter.compare - the system metadata for the index event shows "+pid.getValue()+
                    " having  an emply replica list. So does the solr doc.Same.");
            equal = true;
        } else {
            //system metaddata has an empty replica list but solr doesn't. We need to indexing.
            logger.debug("HZEventFilter.compare - the system metadata for the index event shows "+pid.getValue()+" having an empty replica list while the solr doesn't.Not same.");
            equal = false;
        }
        return equal;
    }
    
    
    /**
     * Parse the solr doc to get the replica information
     * @param doc
     * @return an empty list if there is no replica information
     */
    List<Replica>  getReplicasInSolr(SolrDocument doc) throws Exception {
        List<Replica> replicas = new ArrayList<Replica>();
        Collection<Object> mns =  getValues(doc, REPLICAMN); //the implementation class is ArrayList. So the order can be prserved.
        //System.out.println("==============The class name of collection is "+mns.getClass().getCanonicalName());
        Collection<Object> verifiedDates =  getValues(doc, REPLICAVERIFIEDATE);
        //System.out.println("==============The class name of collection is "+verifiedDates.getClass().getCanonicalName());
        if((mns == null && verifiedDates != null) || (mns != null && verifiedDates == null) ) {
            throw new Exception("The number of the repicat nodes doesn't match the number of the verified date in the solr document. There is an issue on the solr doc.");
        } else if (mns != null && verifiedDates != null) {
            if(mns.size() != verifiedDates.size()) {
                throw new Exception("The number of the repicat nodes doesn't match the number of the verified date in the solr document");
            } else {
                Object[] mnsArray = mns.toArray();
                Object[] verifiedDatesArray = verifiedDates.toArray();
                for(int i=0; i<mnsArray.length; i++) {
                    Object mnObj = mnsArray[i];
                    Object dateObj = verifiedDatesArray[i];
                    String mnStr = (String) mnObj;
                    Date date = (Date)dateObj;
                    if(mnStr== null || mnStr.trim().equals("") || date == null) {
                        throw new Exception("The replication information about the memeber node id or the verfidate date shouldn't be null or blank in the solr document.");
                    }
                    logger.debug("HZEventFilter.getReplicaInSor - the node id of the replica is "+mnStr);
                    logger.debug("HZEventFilter.getReplicaInSor - the verified date of the replica with id "+mnStr +" is "+date);
                    NodeReference mn = new NodeReference();
                    mn.setValue(mnStr);
                    Replica replica = new Replica();
                    replica.setReplicaMemberNode(mn);
                    replica.setReplicaVerified(date);
                    replicas.add(replica);
                }
                
               
            }
        }
        return replicas;
    }
    
    
    /**
     * Get the serial version value from the solr doc. Returns null if can't find it. 
     * @param doc
     * @return
     */
    BigInteger getSerialVersion(SolrDocument doc){
        BigInteger serialVersion = null;
        Collection<Object> values =  getValues(doc, SERIALVERSION);
        if (values != null) {
            for (Object obj : values) {
                serialVersion = (BigInteger) obj;
                 //serialVersion = BigInteger.valueOf(version.longValue());
                break;//get first element
            }
        }
        return serialVersion;
    }
    /**
     * Get the modified date from solr doc.
     * @param doc
     * @return null if no modified date was found
     */
    Date getModificationDateInSolr(SolrDocument doc) {
        Date date = null;
        Collection<Object> values =  getValues(doc, DATEMODIFIED);
        if (values != null) {
            for (Object obj : values) {
                date = (Date) obj;
                break;//get first element
            }
        }
        return date;
    }
    
    /**
     * Get the value of id
     * @param doc
     * @return null if no id was found. This means no solr doc was found.
     */
    String getId(SolrDocument doc) {
        String id = null;
        Collection<Object> values =  getValues(doc, ID);
        if (values != null) {
            for (Object obj : values) {
                id = (String) obj;
                break;//get first element
            }
        }
        return id;
    }
    
    /**
     * Query the document by a given path query
     * @param doc
     * @param fieldName
     * @return null if no values was found.
     */
    Collection<Object> getValues(SolrDocument doc, String fieldName) {
        Collection<Object> fieldValues = null;
        if(doc != null) {
            fieldValues = doc.getFieldValues(fieldName);
            /*for(Object obj : fieldValues) {
                System.out.println("The class of object is "+obj.getClass().getCanonicalName()+" and value is "+obj);
            }*/
        }  
        return fieldValues;
    }
    
    /**
     * Query solr to get the response
     * @param url
     * @return
     * @throws IOException 
     * @throws SolrServerException 
     */
     SolrDocument getSolrReponse(String id) throws SolrServerException, IOException {
        SolrDocument document = new SolrDocument();
        id = escapeQueryChars(id);
        String filter = ID+":"+id;
        System.out.println("the filter is "+filter);
        SolrQuery query = new SolrQuery(filter);
        query.setFields(ID,DATEMODIFIED, REPLICAMN, REPLICAVERIFIEDATE, SERIALVERSION);
        query.setStart(0);
        QueryResponse response = client.query(query);
        SolrDocumentList results = response.getResults();
        //System.out.println("the size of result is "+results.size());
        if(results.size() >0) {
            document = results.get(FIRSTSOLRDOCINDEX);
        }
        //System.out.println("the solr document is "+document);
        return document;
        
    }
    
    /**
     * Escape special characters in the query.
     * @param s
     * @return
     */
    public static String escapeQueryChars(String s) {
         StringBuilder sb = new StringBuilder();
         for (int i = 0; i < s.length(); i++) {
             char c = s.charAt(i);
             // These characters are part of the query syntax and must be escaped
             if (c == '\\' || c == '+' || c == '-' || c == '!' || c == '(' || c == ')' || c == ':'
                     || c == '^' || c == '[' || c == ']' || c == '\"' || c == '{' || c == '}'
                     || c == '~' || c == '*' || c == '?' || c == '|' || c == '&' || c == ';'
                     || Character.isWhitespace(c)) {
                 sb.append('\\');
             }
             sb.append(c);
         }
         return sb.toString();
     }
}
