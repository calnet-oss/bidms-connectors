/*
 * Copyright (c) 2017, Regents of the University of California and
 * contributors.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package edu.berkeley.bidms.connector.ldap

import edu.berkeley.bidms.connector.CallbackContext
import edu.berkeley.bidms.connector.Connector
import edu.berkeley.bidms.connector.ConnectorObjectNotFoundException
import edu.berkeley.bidms.connector.ObjectDefinition
import edu.berkeley.bidms.connector.ldap.event.LdapCallbackContext
import edu.berkeley.bidms.connector.ldap.event.LdapDeleteEventCallback
import edu.berkeley.bidms.connector.ldap.event.LdapEventType
import edu.berkeley.bidms.connector.ldap.event.LdapInsertEventCallback
import edu.berkeley.bidms.connector.ldap.event.LdapPersistCompletionEventCallback
import edu.berkeley.bidms.connector.ldap.event.LdapRemoveAttributesEventCallback
import edu.berkeley.bidms.connector.ldap.event.LdapRenameEventCallback
import edu.berkeley.bidms.connector.ldap.event.LdapSetAttributeEventCallback
import edu.berkeley.bidms.connector.ldap.event.LdapUniqueIdentifierEventCallback
import edu.berkeley.bidms.connector.ldap.event.LdapUpdateEventCallback
import edu.berkeley.bidms.connector.ldap.event.message.LdapDeleteEventMessage
import edu.berkeley.bidms.connector.ldap.event.message.LdapEventMessage
import edu.berkeley.bidms.connector.ldap.event.message.LdapInsertEventMessage
import edu.berkeley.bidms.connector.ldap.event.message.LdapPersistCompletionEventMessage
import edu.berkeley.bidms.connector.ldap.event.message.LdapRemoveAttributesEventMessage
import edu.berkeley.bidms.connector.ldap.event.message.LdapRenameEventMessage
import edu.berkeley.bidms.connector.ldap.event.message.LdapSetAttributeEventMessage
import edu.berkeley.bidms.connector.ldap.event.message.LdapUniqueIdentifierEventMessage
import edu.berkeley.bidms.connector.ldap.event.message.LdapUpdateEventMessage
import groovy.util.logging.Slf4j
import org.springframework.ldap.NameNotFoundException
import org.springframework.ldap.core.ContextMapper
import org.springframework.ldap.core.ContextSource
import org.springframework.ldap.core.DirContextAdapter
import org.springframework.ldap.core.LdapTemplate
import org.springframework.ldap.core.support.SingleContextSource
import org.springframework.ldap.query.LdapQuery
import org.springframework.ldap.query.LdapQueryBuilder
import org.springframework.ldap.query.SearchScope
import org.springframework.ldap.support.LdapNameBuilder
import org.springframework.transaction.annotation.Transactional

import javax.naming.Name
import javax.naming.directory.Attribute
import javax.naming.directory.Attributes
import javax.naming.directory.BasicAttribute
import javax.naming.directory.BasicAttributes
import javax.naming.directory.DirContext
import javax.naming.directory.ModificationItem
import javax.naming.directory.NoSuchAttributeException
import javax.naming.ldap.LdapName
import javax.naming.ldap.Rdn
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

/**
 * Connector for LDAP and Active Directory directory servers.
 */
@Slf4j
// LDAP isn't transactional but a caller may want to use
// TransactionAwareContextSourceProxy.
@Transactional
class LdapConnector implements Connector {

    ContextSource contextSource

    /**
     * Converts a search result to a DirContextAdatper.
     */
    ContextMapper<DirContextAdapter> toDirContextAdapterContextMapper = new ToDirContextAdapterContextMapper()

    /**
     * Converts a search result to a Map<String,Object>.
     */
    ContextMapper<Map<String, Object>> toMapContextMapper = new ToMapContextMapper()

    /**
     * Callbacks to be called when a delete happens.
     */
    List<LdapDeleteEventCallback> deleteEventCallbacks = []

    /**
     * Callbacks to be called when a rename happens.
     */
    List<LdapRenameEventCallback> renameEventCallbacks = []

    /**
     * Callbacks to be called when an update happens.  It's possible this
     * callback is still called when an update is requested but the
     * requested update results in no actual change to the directory object.
     */
    List<LdapUpdateEventCallback> updateEventCallbacks = []

    /**
     * Callbacks to be called when an insert happens.
     */
    List<LdapInsertEventCallback> insertEventCallbacks = []

    /**
     * Callbacks to be called when the unique identifier has been created or
     * possibly changed.  It's possible this callback is called on rename
     * and update events where the directory has not actually changed the
     * unique identifier.
     */
    List<LdapUniqueIdentifierEventCallback> uniqueIdentifierEventCallbacks = []

    /**
     * Callbacks to be called when removeAttributes() is called.
     */
    List<LdapRemoveAttributesEventCallback> removeAttributesEventCallbacks = []

    /**
     * Callbacks to be called when setAttribute() is called.
     */
    List<LdapSetAttributeEventCallback> setAttributeEventCallbacks = []

    /**
     * Callbacks to be called when persist() is at the bottom of its
     * finally{} block and about to exit.
     */
    List<LdapPersistCompletionEventCallback> persistCompletionEventCallbacks = []

    /**
     * Callbacks that dynamically determine the value of an attribute.
     * </p>
     * The key of the map is one of:
     * <ul>
     *     <li>attributeName.indicator - for a callback specific to an
     *         attribute and a dynamic indicator</li>
     *     <li>indicator - for a callback applicable to all attributes for a
     *         dynamic indicator (i.e., a global indicator)</li>
     * </ul>
     * where "indicator" matches the indicator in the values placed in the
     * <code>dynamicAttributeNames</code> array in the object definition. 
     * The callback only runs for an attribute if it's specified in the
     * <code>dynamicAttributeNames</code> array.
     *
     * <p/>
     * There are a few default global indicator callback implementations:
     * <ul>
     *     <li>ONCREATE - Only sets the attribute value when the object is
     *         being created in the downstream system.</li>
     *     <li>ONUPDATE - Only sets the attribute value when the object is
     *         being updated in the downstream system.</li>
     *     <li>APPEND - Only appends template values to the existing
     *         multi-value attribute rather than overwriting it.</li>
     * </ul>
     * These can be replaced with your own implementations (or removed), if
     * you have a need for different behavior.
     */
    Map<String, LdapDynamicAttributeCallback> dynamicAttributeCallbacks = [
            ONCREATE: new LdapDynamicAttributeCallback() {
                @Override
                LdapDynamicAttributeCallbackResult attributeValue(
                        String _eventId,
                        LdapObjectDefinition objectDef,
                        LdapCallbackContext context,
                        FoundObjectMethod foundObjectMethod,
                        String pkey,
                        Name _dn,
                        String attributeName,
                        Map<String, Object> newAttributeMap,
                        Map<String, Object> existingAttributeMap,
                        Object existingValue,
                        String dynamicCallbackIndicator,
                        Object dynamicValueTemplate
                ) {
                    if (!foundObjectMethod) {
                        // create
                        return new LdapDynamicAttributeCallbackResult(
                                attributeValue: dynamicValueTemplate
                        )
                    } else {
                        return null
                    }
                }
            },
            ONUPDATE: new LdapDynamicAttributeCallback() {
                @Override
                LdapDynamicAttributeCallbackResult attributeValue(
                        String _eventId,
                        LdapObjectDefinition objectDef,
                        LdapCallbackContext context,
                        FoundObjectMethod foundObjectMethod,
                        String pkey,
                        Name _dn,
                        String attributeName,
                        Map<String, Object> newAttributeMap,
                        Map<String, Object> existingAttributeMap,
                        Object existingValue,
                        String dynamicCallbackIndicator,
                        Object dynamicValueTemplate
                ) {
                    if (foundObjectMethod) {
                        // update
                        return new LdapDynamicAttributeCallbackResult(
                                attributeValue: dynamicValueTemplate
                        )
                    } else {
                        return null
                    }
                }
            },
            APPEND  : new LdapDynamicAttributeCallback() {
                @Override
                LdapDynamicAttributeCallbackResult attributeValue(
                        String _eventId,
                        LdapObjectDefinition objectDef,
                        LdapCallbackContext context,
                        FoundObjectMethod foundObjectMethod,
                        String pkey,
                        Name _dn,
                        String attributeName,
                        Map<String, Object> newAttributeMap,
                        Map<String, Object> existingAttributeMap,
                        Object existingValue,
                        String dynamicCallbackIndicator,
                        Object dynamicValueTemplate
                ) {
                    if (existingValue) {
                        // append to the existing list, but prevent case-insensitive duplicates
                        if (!(existingValue instanceof List)) {
                            existingValue = [existingValue]
                        }
                        HashSet<CaseInsensitiveString> set = new HashSet<CaseInsensitiveString>(((List) existingValue).collect { new CaseInsensitiveString(it.toString().trim()) })
                        if (dynamicValueTemplate instanceof List) {
                            set.addAll(((List) dynamicValueTemplate).collect { new CaseInsensitiveString(it.toString().trim()) })
                        } else {
                            set.add(new CaseInsensitiveString(dynamicValueTemplate.toString().trim()))
                        }
                        return new LdapDynamicAttributeCallbackResult(
                                attributeValue: new ArrayList<String>(set*.toString())
                        )
                    } else {
                        // insert
                        return new LdapDynamicAttributeCallbackResult(
                                attributeValue: dynamicValueTemplate
                        )
                    }
                }
            }
    ]

    /**
     * For queuing up asynchronous callback messages
     */
    private final LinkedBlockingQueue<LdapEventMessage> callbackMessageQueue = new LinkedBlockingQueue<LdapEventMessage>()

    /**
     * If true, calls to callbacks will be done synchronously instead of
     * asynchronously
     */
    boolean isSynchronousCallback = false

    /**
     * Thread for monitoring the callback queue
     */
    LdapCallbackMonitorThread callbackMonitorThread

    /**
     * Start the LDAP connector.  Responsible for starting the callback
     * queue monitor thread when running in asynchronous callback mode.
     */
    void start() {
        if (!isSynchronousCallback) {
            this.callbackMonitorThread = new LdapCallbackMonitorThread(this)
            callbackMonitorThread.start()
        }
    }

    /**
     * Stop the LDAP connector.  Responsible for stopping the callback queue
     * monitor thread when running in asynchronous callback mode.
     */
    void stop() {
        if (!isSynchronousCallback) {
            callbackMonitorThread.requestStop()
        }
    }

    /**
     * In current thread, invoke the callback for an event message.  This
     * invoked by the ldapConnector directly in synchronous callback mode
     * and invoked by the callback queue monitor thread when running in
     * asynchronous callback mode.
     *
     * @param eventMessage The event message to pass back to the callback.
     */
    protected void invokeCallback(LdapEventMessage eventMessage) {
        switch (eventMessage.eventType) {
            case LdapEventType.UPDATE_EVENT:
                updateEventCallbacks?.each { it.receive((LdapUpdateEventMessage) eventMessage) }
                break
            case LdapEventType.INSERT_EVENT:
                insertEventCallbacks?.each { it.receive((LdapInsertEventMessage) eventMessage) }
                break
            case LdapEventType.RENAME_EVENT:
                renameEventCallbacks?.each { it.receive((LdapRenameEventMessage) eventMessage) }
                break
            case LdapEventType.DELETE_EVENT:
                deleteEventCallbacks?.each { it.receive((LdapDeleteEventMessage) eventMessage) }
                break
            case LdapEventType.UNIQUE_IDENTIFIER_EVENT:
                uniqueIdentifierEventCallbacks?.each { it.receive((LdapUniqueIdentifierEventMessage) eventMessage) }
                break
            case LdapEventType.REMOVE_ATTRIBUTES_EVENT:
                removeAttributesEventCallbacks?.each { it.receive((LdapRemoveAttributesEventMessage) eventMessage) }
                break
            case LdapEventType.SET_ATTRIBUTE_EVENT:
                setAttributeEventCallbacks?.each { it.receive((LdapSetAttributeEventMessage) eventMessage) }
                break
            case LdapEventType.PERSIST_COMPLETION_EVENT:
                persistCompletionEventCallbacks?.each { it.receive((LdapPersistCompletionEventMessage) eventMessage) }
                break
            default:
                throw new RuntimeException("Unknown LdapEventType for event message: ${eventMessage.eventType}")
        }
    }

    /**
     * The callback queue monitor thread calls this to see if there are
     * messages in the callback queue.  Blocks until a message appears in
     * the queue or the timeout period has elapsed.
     *
     * @return The next message in the callback queue.  Returns null if
     *         timeout period has elapsed.
     * @throws InterruptedException if the thread is interrupted while
     *         blocking
     */
    protected LdapEventMessage pollCallbackQueue(long timeout, TimeUnit unit) throws InterruptedException {
        return callbackMessageQueue.poll(timeout, unit)
    }

    /**
     * Deliver a callback message either synchronously or asynchronously
     * depending on the isSynchronousCallback flag.
     *
     * @param eventMessage The event message to deliver.
     */
    void deliverCallbackMessage(LdapEventMessage eventMessage) {
        if (isSynchronousCallback) {
            invokeCallback(eventMessage)
        } else {
            if (eventMessage) {
                if (!callbackMessageQueue.offer(eventMessage)) {
                    throw new LdapConnectorException("Could not add event message to the callback message queue")
                }
            } else {
                log.warn("deliveryCallbackMessage was called with a null eventMessage")
            }
        }
    }

    /**
     * Search the directory for objects for a primary key if
     * search-by-primary-key is enabled.  Search-by-primary-key is disabled
     * if objectDef.getLdapQueryForPrimaryKey(pkey) returns null.
     *
     * @param reqCtx Context for the request
     * @param pkey Primary key
     * @return A list of objects found in the directory if search by primary
     *         key is enabled, null otherwise.
     */
    List<DirContextAdapter> searchByPrimaryKey(LdapRequestContext reqCtx, String pkey) {
        LdapQuery query = reqCtx.objectDef.getLdapQueryForPrimaryKey(pkey)
        return (query ? reqCtx.ldapTemplate.search(query, toDirContextAdapterContextMapper) : null)
    }

    /**
     * Search the directory for an object by its DN.
     *
     * @param reqCtx Context for the request
     * @param dn Distinguished name
     * @param attributes Optionally, a list of attributes to return for each
     *        object.  If null, returns all attributes except operational
     *        attributes.  If operational attributes are desired, they have
     *        to be specified.
     * @return The found directory object or null if it was not found
     */
    DirContextAdapter lookup(LdapRequestContext reqCtx, Name dn, String[] attributes = null) {
        if (!attributes) {
            return (DirContextAdapter) reqCtx.ldapTemplate.lookup(dn)
        } else {
            return (DirContextAdapter) reqCtx.ldapTemplate.lookup(dn, attributes, toDirContextAdapterContextMapper)
        }
    }

    /**
     * Search the directory for an object by its globally unique identifier.
     *
     * @param reqCtx Context for the request
     * @param pkey When searching by globally unique identifier, the object
     *        must also match this expected primary key.
     * @param uniqueIdentifier Globally unique identifier
     * @return The found directory object or null if it was not found
     */
    DirContextAdapter lookupByGloballyUniqueIdentifier(
            LdapRequestContext reqCtx,
            String pkey,
            Object uniqueIdentifier
    ) {
        return reqCtx.ldapTemplate.searchForObject(reqCtx.objectDef.getLdapQueryForGloballyUniqueIdentifier(pkey, uniqueIdentifier), toDirContextAdapterContextMapper)
    }

    /**
     * Delete an object in the directory matching the DN.  The primary key
     * (pkey) parameter is only passed in to pass back to the delete
     * callback and is otherwise unused in determining which object to
     * delete.
     *
     * @param reqCtx Context for the request
     * @param pkey The primary key.  The parameter is only passed in to pass
     *        back to the delete callback and is otherwise unused in
     *        determining which object to delete.
     * @param dn The distinguished name of the object to delete
     * @throws LdapConnectorException If an error occurs
     */
    void delete(
            LdapRequestContext reqCtx,
            String pkey,
            Name dn
    ) throws LdapConnectorException {
        Throwable exception
        try {
            // Recursively delete subordinates (leaves) first.  We have to search for them.
            LdapQuery subordinateQuery = LdapQueryBuilder.query()
                    .base(dn)
                    .searchScope(SearchScope.ONELEVEL)
                    .where("objectClass").isPresent()
            List<DirContextAdapter> subordinates = reqCtx.ldapTemplate.search(
                    subordinateQuery,
                    toDirContextAdapterContextMapper
            )
            subordinates.each { DirContextAdapter foundSubordinate ->
                if (!nameEquals(reqCtx.objectDef, foundSubordinate.dn, dn)) {
                    delete(reqCtx, pkey, foundSubordinate.dn)
                }
            }

            // now that the subordinates are deleted, delete the DN
            reqCtx.ldapTemplate.unbind(dn)
        }
        catch (Throwable t) {
            exception = t
            throw new LdapConnectorException(t)
        }
        finally {
            deliverCallbackMessage(new LdapDeleteEventMessage(
                    success: exception == null,
                    eventId: reqCtx.eventId,
                    objectDef: reqCtx.objectDef,
                    context: reqCtx.context,
                    pkey: pkey,
                    dn: dn,
                    exception: exception
            ))
        }
    }

    /**
     * Rename an object in the directory, which means changing its
     * distinguished name.
     *
     * @param reqCtx Context for the request
     * @param pkey Primary key
     * @param oldDn The original distinguished name of the object to be
     *        renamed.
     * @param newDn The new distingished name of the object after it is
     *        renamed.
     * @throws LdapConnectorException If an error occurs
     */
    void rename(
            LdapRequestContext reqCtx,
            String pkey,
            Name oldDn,
            Name newDn
    ) throws LdapConnectorException {
        Throwable exception
        Object directoryUniqueIdentifier = null
        try {
            reqCtx.ldapTemplate.rename(oldDn, newDn)

            if (reqCtx.objectDef.globallyUniqueIdentifierAttributeName && uniqueIdentifierEventCallbacks) {
                // Get the possibly-changed unique identifier for the
                // renamed object so we can pass it back in the unique identifier
                // callback
                directoryUniqueIdentifier = getGloballyUniqueIdentifier(reqCtx, newDn)
                if (!directoryUniqueIdentifier) {
                    log.warn("The ${reqCtx.objectDef.globallyUniqueIdentifierAttributeName} was unable to be retrieved from the just renamed entry of $newDn")
                }
            }
        }
        catch (Throwable t) {
            exception = t
            throw new LdapConnectorException(t)
        }
        finally {
            deliverCallbackMessage(new LdapRenameEventMessage(
                    success: exception == null,
                    eventId: reqCtx.eventId,
                    objectDef: reqCtx.objectDef,
                    context: reqCtx.context,
                    pkey: pkey,
                    oldDn: oldDn,
                    newDn: newDn,
                    exception: exception
            ))

            if (!exception && directoryUniqueIdentifier) {
                deliverCallbackMessage(new LdapUniqueIdentifierEventMessage(
                        success: true,
                        causingEvent: LdapEventType.RENAME_EVENT,
                        eventId: reqCtx.eventId,
                        objectDef: reqCtx.objectDef,
                        context: reqCtx.context,
                        pkey: pkey,
                        oldDn: oldDn,
                        newDn: newDn,
                        globallyUniqueIdentifier: directoryUniqueIdentifier,
                        wasRenamed: true
                ))
            }
        }
    }

    static class CaseInsensitiveString {
        String str

        CaseInsensitiveString(String str) {
            this.str = str
        }

        @Override
        int hashCode() {
            return str.toLowerCase().hashCode()
        }

        @Override
        boolean equals(Object obj) {
            return str.toLowerCase() == obj?.toString()?.trim()?.toLowerCase()
        }

        @Override
        String toString() {
            return str.toString()
        }
    }

    /**
     * Update an existing directory object with given values.
     *
     * Attribute names in the newReplaceAttributeMap and
     * newAppendOnlyAttributeMap are case sensitive in the context of
     * properly detecting changes of the attributes.  To detect changes
     * properly, attribute strings must match the case of the attribute
     * names of the directory schema, as retrieved from the directory via a
     * search() or lookup().
     *
     * @param reqCtx Context for the request
     * @param foundObjectMethod
     * @param pkey Primary key
     * @param existingEntry The existing directory entry to update
     * @param newReplaceAttributeMap Attributes to replace where the keys in
     *        the map are attribute names.  For changes to be detected
     *        properly the attribute names must match the case of the
     *        attribute in the directory.
     * @return true if an update actually occured in the directory.  false
     *         may be returned if the object is unchanged.
     * @throws LdapConnectorException If an error occurs
     */
    boolean update(
            LdapRequestContext reqCtx,
            FoundObjectMethod foundObjectMethod,
            String pkey,
            DirContextAdapter existingEntry,
            Map<String, Object> newReplaceAttributeMap
    ) throws LdapConnectorException {
        Throwable exception
        Map<String, Object> oldAttributeMap = null
        Map<String, Object> convertedNewAttributeMap = null
        ModificationItem[] modificationItems = null
        try {
            oldAttributeMap = toMapContextMapper.mapFromContext(existingEntry)
            oldAttributeMap.remove("dn")

            convertedNewAttributeMap = convertCallerProvidedMap(newReplaceAttributeMap)
            Map<String, Object> attributesToKeepOrUpdate
            if (reqCtx.objectDef.isKeepExistingAttributesWhenUpdating()) {
                attributesToKeepOrUpdate = new LinkedHashMap<String, Object>(oldAttributeMap)
                attributesToKeepOrUpdate.putAll(convertedNewAttributeMap)
            } else {
                attributesToKeepOrUpdate = convertedNewAttributeMap
            }

            Map<String, Object> changedAttributes = attributesToKeepOrUpdate - oldAttributeMap

            // Removing the attribute if keepExistingAttributes is false and
            // the attribute is not in the newAttributeMap or if the
            // attribute is explicitly set to null in the newAttributeMap.
            HashSet<String> attributeNamesToRemove = (
                    (!reqCtx.objectDef.isKeepExistingAttributesWhenUpdating() ? oldAttributeMap.keySet() - attributesToKeepOrUpdate.keySet() : []) as HashSet<String>
            ) + (
                    (newReplaceAttributeMap.findAll { it.value == null && oldAttributeMap.containsKey(it.key) }*.key) as HashSet<String>
            )

            Collection<Attribute> attributesToRemove = existingEntry.attributes.all.findAll { Attribute attr ->
                attr.ID in attributeNamesToRemove
            }

            attributesToRemove.each { Attribute attr ->
                attr.all.each { value ->
                    existingEntry.removeAttributeValue(attr.ID, value)
                }
            }

            changedAttributes.each { Map.Entry<String, Object> entry ->
                Collection toKeepOrUpdateCollection = (attributesToKeepOrUpdate[entry.key] instanceof Collection ? (Collection) attributesToKeepOrUpdate[entry.key] : [attributesToKeepOrUpdate[entry.key]])
                if (oldAttributeMap[entry.key] instanceof Collection) {
                    Collection attrsToRemove = ((Collection) oldAttributeMap[entry.key]) - toKeepOrUpdateCollection

                    // See if an attribute is being replaced with the same
                    // value, just a different case.  In these cases we need
                    // to force a REPLACE_ATTRIBUTE ModificationItem rather
                    // than a REMOVE/ADD.  We force this by passing
                    // "orderMatters=true" to
                    // existingEntry.setAttributeValues().
                    boolean hasADifferentCasedValue = attrsToRemove.any { it.toString().toLowerCase().trim() in toKeepOrUpdateCollection.collect { it.toString().toLowerCase().trim() } }
                    if (hasADifferentCasedValue) {
                        // REPLACE approach
                        existingEntry.setAttributeValues(entry.key, toKeepOrUpdateCollection.toArray(), true)
                    } else {
                        // ADD/REMOVE approach
                        attrsToRemove.each {
                            existingEntry.removeAttributeValue(entry.key, it)
                        }
                        existingEntry.setAttributeValues(entry.key, toKeepOrUpdateCollection.toArray())
                    }
                } else {
                    existingEntry.setAttributeValues(entry.key, toKeepOrUpdateCollection.toArray())
                }
            }

            modificationItems = existingEntry.modificationItems
            boolean isModified = modificationItems?.size()
            reqCtx.ldapTemplate.modifyAttributes(existingEntry)

            return isModified
        }
        catch (Throwable t) {
            exception = t
            throw new LdapConnectorException(t)
        }
        finally {
            deliverCallbackMessage(new LdapUpdateEventMessage(
                    success: exception == null,
                    eventId: reqCtx.eventId,
                    objectDef: reqCtx.objectDef,
                    context: reqCtx.context,
                    foundMethod: foundObjectMethod,
                    pkey: pkey,
                    oldAttributes: oldAttributeMap,
                    dn: existingEntry.dn,
                    newAttributes: convertedNewAttributeMap ?: newReplaceAttributeMap,
                    modificationItems: modificationItems,
                    exception: exception
            ))
        }
    }

    /**
     * Insert a new object into the directory.
     *
     * @param reqCtx Context for the request
     * @param pkey Primary key of object being created
     * @param dn Distinguished name of object being created
     * @param attributeMap Attributes for the object where the keys in the
     *        map are attribute names.
     * @returns The just-inserted globally unique identifier, if it could be
     *          determined.  It's not guaranteed this will always return
     *          non-null on a successful insert.  In other words, null does
     *          not indicate a failed insert.
     * @throws LdapConnectorException If an error occurs
     */
    Object insert(
            LdapRequestContext reqCtx,
            String pkey,
            Name dn,
            Map<String, Object> attributeMap
    ) throws LdapConnectorException {
        Throwable exception
        Map<String, Object> convertedNewAttributeMap = null
        Object directoryUniqueIdentifier = null
        try {
            convertedNewAttributeMap = convertCallerProvidedMap(attributeMap)

            // Spring method naming is a little confusing.  Spring uses the
            // word "bind" and "rebind" to mean "create" and "update." In
            // this context, it does not mean "authenticate (bind) to the
            // directory server.
            reqCtx.ldapTemplate.bind(dn, null, buildAttributes(convertedNewAttributeMap))

            if (reqCtx.objectDef.globallyUniqueIdentifierAttributeName && uniqueIdentifierEventCallbacks) {
                // Get the newly-created directory unique identifier so we
                // can pass it back in the insert callback
                directoryUniqueIdentifier = getGloballyUniqueIdentifier(reqCtx, dn)
                if (!directoryUniqueIdentifier) {
                    log.warn("The ${reqCtx.objectDef.globallyUniqueIdentifierAttributeName} was unable to be retrieved from the just inserted entry of $dn")
                }
                return directoryUniqueIdentifier
            }
            return null
        }
        catch (Throwable t) {
            exception = t
            throw new LdapConnectorException(t)
        }
        finally {
            deliverCallbackMessage(new LdapInsertEventMessage(
                    success: exception == null,
                    eventId: reqCtx.eventId,
                    objectDef: reqCtx.objectDef,
                    context: reqCtx.context,
                    pkey: pkey,
                    dn: dn,
                    newAttributes: convertedNewAttributeMap ?: attributeMap,
                    exception: exception
            ))

            if (!exception && directoryUniqueIdentifier) {
                deliverCallbackMessage(new LdapUniqueIdentifierEventMessage(
                        success: true,
                        causingEvent: LdapEventType.INSERT_EVENT,
                        eventId: reqCtx.eventId,
                        objectDef: reqCtx.objectDef,
                        context: reqCtx.context,
                        pkey: pkey,
                        newDn: dn,
                        globallyUniqueIdentifier: directoryUniqueIdentifier,
                        wasRenamed: false
                ))
            }
        }
    }

    static class MatchingEntryResult {
        DirContextAdapter entry
        FoundObjectMethod foundObjectMethod
        List<DirContextAdapter> searchResults
    }

    MatchingEntryResult findMatchingEntry(
            LdapRequestContext reqCtx,
            Name dn,
            String pkey,
            Object uniqueIdentifier
    ) {
        MatchingEntryResult result = new MatchingEntryResult()

        //
        // There's only supposed to be one entry-per-pkey in the
        // directory, but this is not guaranteed to always be the case.
        // If isRemoveDuplicatePrimaryKeys() returns true, pick one to
        // use according to our resolution algorithm and delete the
        // rest.  It's also possible the DN exists with a different
        // primary key.  In that case, use that object, but the primary
        // key will be updated.
        //

        // See if records belonging to the pkey exist.
        // This will return null if objectDef.getLdapQueryForPrimaryKey()
        // returns null, indicating search-by-primary-key is disabled.
        result.searchResults = searchByPrimaryKey(reqCtx, pkey)

        if (!result.entry && dn) {
            // Find entries with matching dn.  searchResults only
            // contains entries with matching pkey.
            result.entry = result.searchResults?.find { DirContextAdapter entry ->
                entry.dn == dn
            }
            if (result.entry) {
                result.foundObjectMethod = FoundObjectMethod.BY_DN_MATCHED_KEY
            }
        }

        // If none of the entries match DN but there's one value in
        // searchResults, use that.  If a dn was specified, this means
        // the DN has changed, but the object was found by pkey.
        if (!result.entry && result.searchResults?.size() == 1 && reqCtx.objectDef.acceptAsExistingDn(result.searchResults.first().dn.toString())) {
            result.entry = result.searchResults.first()
            result.foundObjectMethod = (dn ? FoundObjectMethod.BY_MATCHED_KEY_DN_MISMATCH : FoundObjectMethod.BY_MATCHED_KEY_DN_NOT_PROVIDED)
        }

        // If none of the entries match DN but there's more than one
        // value in searchResults, the objects in the searchResults
        // could be a match against the globally unique identifier,
        // which we want to prioritize over the others.  If there's a
        // match against a key and a dn was specified, this means the DN
        // has changed.
        if (!result.entry && result.searchResults?.size()) {
            if (uniqueIdentifier) {
                result.entry = lookupByGloballyUniqueIdentifier(reqCtx, pkey, uniqueIdentifier)
            }
            if (result.entry) {
                // match found against the globally unique identifier
                result.foundObjectMethod = (dn ? FoundObjectMethod.BY_MATCHED_KEY_DN_MISMATCH : FoundObjectMethod.BY_MATCHED_KEY_DN_NOT_PROVIDED)
            } else {
                // If none of the entires match DN or unique identifier
                // but there are more than one value in searchResults,
                // that means there are multiple entries in the
                // directory with the same pkey, none of them matching
                // our DN or unique identifier.  Just pick the first one
                // found that matches our DN acceptance criteria.
                result.entry = result.searchResults.find { DirContextAdapter entry ->
                    reqCtx.objectDef.acceptAsExistingDn(entry.dn.toString())
                }
                if (result.entry) {
                    result.foundObjectMethod = FoundObjectMethod.BY_FIRST_FOUND
                }
            }
        }

        // DN may still exist but with a different primary key
        if (!result.entry && dn) {
            try {
                result.entry = lookup(reqCtx, dn)
            }
            catch (NameNotFoundException ignored) {
                // no-op
            }
            if (result.entry) {
                result.foundObjectMethod = FoundObjectMethod.BY_DN_MISMATCHED_KEYS
            }
        }

        return result
    }

    /**
     * Insert, update or delete (persist) an object in the directory.
     *
     * <p/>
     *
     * Special values in the attrMap:
     * <ul>
     * <li>Optionally, the requested DN of the object is in attrMap[dn]. 
     *     Mandatory for an insert situation where pkey is not found in the
     *     directory.
     *     Also mandatory if search-by-primary-key is disabled.</li>
     * <li>The primary key of the object is in
     *     attrMap[objectDef.getPrimaryKeyAttrName()].</li>
     * <li>Optionally, the globally unique identifier of the object is in
     *     attrMap[objectDef.getGloballyUniqueIdentifierAttrName()].  The
     *     globally unique identifier is assumed to be an operational
     *     attribute that is read-only.  That is, it is only set
     *     automatically by the directory.</li>
     * </ul>
     *
     * Existing objects are found via searchByPrimaryKey() (if enabled)
     * or lookup().
     *
     * <p/>
     *
     * (Only applies if search-by-primary-key is enabled).
     * When attempting an insert of a new DN, if there is already an
     * existing object in the directory with the primary key, the existing
     * object will be updated instead and the DN will be renamed to your
     * requested DN, if renaming is enabled.  If there are multiple objects
     * already in the directory with the same primary key but different DNs
     * and objectDef.isRemoveDuplicatePrimaryKeys() returns true, the
     * duplicates will be deleted.
     *
     * <p/>
     *
     * (Only applies if search-by-primary-key is enabled).
     * When attempting an update of an object but the DN does not exist, and
     * there is already an existing object in the directory with the primary
     * key, the existing object will be updated and the DN will be renamed
     * to your requested DN, if renaming is enabled.  If there are multiple
     * objects already in the directory with the same primary key but
     * different DNs and objectDef.isRemoveDuplicatePrimaryKeys() returns
     * true, the duplicates will be deleted.
     *
     * <p/>
     *
     * (Only applies if search-by-primary-key is enabled).
     * When inserting or updating and multiple objects with the same primary
     * key are found, there is an algorithm for determining which one to
     * update.  The rest are deleted if
     * objectDef.isRemoveDuplicatePrimaryKeys()() returns true.  This
     * algorithm is:
     * <ul>
     * <li>Use searchByPrimaryKey to find objects by primary key.</li>
     * <li>If any of those match the requested DN, that object will be kept
     *     and the rest deleted.</li>
     * <li>If there's only one result found and
     *     objectDef.acceptAsExistingDn() returns true for it, use that
     *     object.</li>
     * <li>If there's an object that matches the pkey and the globally
     *     unique identifier, use that and delete the rest.</li>
     * <li>If there's an object that matches the primary key and
     *     objectDef.acceptAsExistingDn() returns true for it, use that
     *     object and delete the rest.</li>
     * <li>Search the directory for the DN using lookup().  If an object is
     *     found, that means the DN exists but the primary key is a mismatch
     *     and the primary key will be updated.</li>
     * <li>Otherwise, no existing object is found and it becomes an insert
     *     situation.</li>
     * </ul>
     *
     * <p/>
     *
     * When deleting an object, any object matching the DN or the primary
     * key (if objectDef.isRemoveDuplicatePrimaryKeys() returns true and
     * search-by-primary-key is enabled) will be deleted.
     *
     * <p/>
     *
     * For changes to be detected properly, attribute names in the attrMap
     * are case sensitive.  Attribute strings must match the case of the
     * attribute names in the directory schema, as when retrieved from the
     * directory via a search() or lookup().
     *
     * @param reqCtx Context for the request
     * @param attrMap The map of the attributes for the directory object
     *        where the keys in the map are attribute names.  For changes to
     *        be detected properly, attribute names are case sensitive and
     *        must match the case of the attribute names in the directory
     *        schema.
     * @param isDelete If true, the object matching the distinguished name
     *        will be deleted.  If objectDef.isRemoveDuplicatePrimaryKeys()
     *        is true, all objects matching the primary key, as returned by
     *        searchByPrimaryKey() (if enabled), will be deleted.
     * @return true if an update actually occured in the directory.  false
     *         may be returned if the object is unchanged.
     * @throws LdapConnectorException If an error occurs
     */
    @Override
    boolean persist(
            String eventId,
            ObjectDefinition objectDef,
            CallbackContext context,
            Map<String, Object> attrMap,
            boolean isDelete
    ) throws LdapConnectorException {
        LdapRequestContext reqCtx = new LdapRequestContext(singleContextLdapTemplate, eventId, (LdapObjectDefinition) objectDef, (LdapCallbackContext) context)
        Throwable exception = null
        try {
            LinkedHashMap<String, Object> attrMapCopy = new LinkedHashMap<String, Object>(attrMap)

            // (optional) globally unique identifier
            String uniqueIdentifierAttrName = ((LdapObjectDefinition) objectDef).globallyUniqueIdentifierAttributeName
            Object uniqueIdentifier = null
            if (uniqueIdentifierAttrName) {
                uniqueIdentifier = attrMapCopy[uniqueIdentifierAttrName]
                // Remove the uniqueIdentifier from the object -- we're
                // assuming this is an operational attribute that we can't
                // set.
                attrMapCopy.remove(uniqueIdentifierAttrName)
            }

            // primary key
            String pkeyAttrName = ((LdapObjectDefinition) objectDef).primaryKeyAttributeName
            String pkey = attrMapCopy[pkeyAttrName]
            if (!isDelete && !pkey) {
                throw new LdapConnectorException("Directory object is missing a required value for primary key $pkeyAttrName")
            }

            // (optional) DN
            Name dn = null
            boolean hasDynamicDn = false
            boolean hasDnOnCreate = false
            boolean hasDnOnUpdate = false
            boolean hasDnNotConditional = false
            dnDeterminationLabel:
            {
                String dnNotConditional = attrMapCopy.dn
                hasDnNotConditional = dnNotConditional != null
                if (hasDnNotConditional) {
                    // Remove the dn from the object -- not an actual
                    // attribute
                    attrMapCopy.remove("dn")
                }

                String dnDynamic = attrMapCopy["dn.DYNAMIC"]
                hasDynamicDn = dnDynamic != null
                if (hasDynamicDn) {
                    attrMapCopy.remove("dn.DYNAMIC")
                }
                if (hasDynamicDn && !((LdapObjectDefinition) objectDef).dynamicAttributeNames.contains("dn.DYNAMIC")) {
                    throw new LdapConnectorException("dn.DYNAMIC is provided but it is not listed in dynamicAttributeNames in the object definition")
                }

                String dnOnCreate = attrMapCopy["dn.ONCREATE"]
                hasDnOnCreate = dnOnCreate != null
                if (hasDnOnCreate) {
                    attrMapCopy.remove("dn.ONCREATE")
                }
                if (hasDnOnCreate && !((LdapObjectDefinition) objectDef).dynamicAttributeNames.contains("dn.ONCREATE")) {
                    throw new LdapConnectorException("dn.ONCREATE is provided but it is not listed in dynamicAttributeNames in the object definition")
                }

                String dnOnUpdate = attrMapCopy["dn.ONUPDATE"]
                hasDnOnUpdate = dnOnUpdate != null
                if (hasDnOnUpdate) {
                    attrMapCopy.remove("dn.ONUPDATE")
                }
                if (hasDnOnUpdate && !((LdapObjectDefinition) objectDef).dynamicAttributeNames.contains("dn.ONUPDATE")) {
                    throw new LdapConnectorException("dn.ONUPDATE is provided but it is not listed in dynamicAttributeNames in the object definition")
                }

                if (hasDnOnCreate && hasDnOnUpdate) {
                    throw new LdapConnectorException("Only one of dn.ONCREATE or dn.ONUPDATE is allowed: provide only one of these")
                }
                // dn.DYNAMIC trumps dn.UPDATE
                String conditionalDn = dnDynamic ?: dnOnUpdate ?: dnOnCreate

                if (hasDnNotConditional && conditionalDn) {
                    throw new LdapConnectorException("Only one of dn.DYNAMIC, dn.ONCREATE, dn.ONUPDATE or dn is allowed: provide only one of these")
                }

                String dnString = conditionalDn ?: dnNotConditional
                if (dnString) {
                    dn = buildDnName(dnString)
                }
            }

            MatchingEntryResult matchingEntryResult = null
            DirContextAdapter existingEntry = null
            FoundObjectMethod foundObjectMethod = null
            Map<String, Object> existingAttrMapForDynamicAttributeCallbacks = null

            if (!isDelete || hasDynamicDn) {
                // If dn.DYNAMIC is set, then primary key/unique identifier
                // must be used to retrieve the object.
                matchingEntryResult = findMatchingEntry(reqCtx, (!hasDynamicDn ? dn : null), pkey, uniqueIdentifier)
                existingEntry = matchingEntryResult.entry
                foundObjectMethod = matchingEntryResult.foundObjectMethod

                // For dn.DYNAMIC, need to execute the callback early to get
                // the real DN value.
                if (hasDynamicDn) {
                    Name existingDn = (existingEntry ? existingEntry.dn : null)
                    if (existingEntry && existingAttrMapForDynamicAttributeCallbacks == null) {
                        existingAttrMapForDynamicAttributeCallbacks = toMapContextMapper.mapFromContext(existingEntry)
                    }

                    LdapDynamicAttributeCallback callback = dynamicAttributeCallbacks["dn.DYNAMIC"]
                    if (!callback) {
                        throw new LdapConnectorException("No callback for dynamic attribute dn.DYNAMIC is set")
                    }
                    LdapDynamicAttributeCallbackResult result = callback.attributeValue(
                            eventId,
                            (LdapObjectDefinition) objectDef,
                            (LdapCallbackContext) context,
                            foundObjectMethod,
                            pkey,
                            null,
                            "dn",
                            attrMap,
                            existingAttrMapForDynamicAttributeCallbacks,
                            existingDn,
                            "DYNAMIC",
                            dn
                    )
                    dn = buildDnName(result.attributeValue as String)
                }
            }

            boolean isModified = false
            boolean wasRenamed = false

            if (!isDelete) {
                if (existingEntry && ((LdapObjectDefinition) objectDef).isRemoveDuplicatePrimaryKeys()) {
                    // Delete all the entries that we're not keeping as the
                    // existingEntry
                    matchingEntryResult.searchResults.each { DirContextAdapter entry ->
                        if (!nameEquals(objectDef, entry.dn, existingEntry.dn)) {
                            delete(reqCtx, pkey, entry.dn)
                            isModified = true
                        }
                    }
                }

                // renaming is only disabled when none of: dn.DYNAMIC,
                // dn.ONUPDATE, dn exists in the attribute map.
                boolean renamingEnabled = hasDynamicDn || hasDnOnUpdate || hasDnNotConditional

                // Deal with dynamic attributes
                ((LdapObjectDefinition) objectDef).dynamicAttributeNames?.each { String attrNameAndIndicator ->
                    // everything before the last dot is the attribute name
                    // and everything after the last dot is the dynamic
                    // callback indicator
                    String attributeName = attrNameAndIndicator.substring(0, attrNameAndIndicator.lastIndexOf('.'))
                    String dynamicCallbackIndicator = attrNameAndIndicator.substring(attributeName.length() + 1)

                    Object dynamicValueTemplate = attrMapCopy[attrNameAndIndicator]
                    attrMapCopy.remove(attrNameAndIndicator)

                    if (dynamicValueTemplate != null) {
                        Object existingAttributeValue = null
                        Attribute existingAttribute = null
                        try {
                            existingAttribute = (existingEntry ? existingEntry.attributes?.get(attributeName) : null)
                        }
                        catch (javax.naming.NameNotFoundException ignored) {
                            // no-op
                        }
                        if (existingAttribute) {
                            existingAttributeValue = ToMapContextMapper.convertAttribute(existingAttribute)
                        }
                        if (existingEntry && existingAttrMapForDynamicAttributeCallbacks == null) {
                            existingAttrMapForDynamicAttributeCallbacks = toMapContextMapper.mapFromContext(existingEntry)
                        }

                        LdapDynamicAttributeCallback callback = dynamicAttributeCallbacks[attrNameAndIndicator] ?: dynamicAttributeCallbacks[dynamicCallbackIndicator]
                        if (!callback) {
                            throw new LdapConnectorException("No callback for dynamic attribute $attrNameAndIndicator nor $dynamicCallbackIndicator is set")
                        }
                        LdapDynamicAttributeCallbackResult result = callback.attributeValue(
                                eventId,
                                (LdapObjectDefinition) objectDef,
                                (LdapCallbackContext) context,
                                foundObjectMethod,
                                pkey,
                                dn,
                                attributeName,
                                attrMap,
                                existingAttrMapForDynamicAttributeCallbacks,
                                existingAttributeValue,
                                dynamicCallbackIndicator,
                                dynamicValueTemplate
                        )

                        if (result) {
                            // a null attributeValue will result in
                            // attribute removal
                            attrMapCopy[attributeName] = result.attributeValue
                        } else {
                            // Don't modify: The attribute name shouldn't be
                            // in the map, but ust in case it is, remove it
                            // so we leave it unchanged downstream.
                            attrMapCopy.remove(attributeName)
                        }
                    }
                }

                // Group directives
                List<String> requestedGroupAdditions = []
                List<String> requestedGroupRemovals = []
                if (objectDef.groupDirectiveMetaAttributePrefix) {
                    String groupAddAttributeName = "${objectDef.groupDirectiveMetaAttributePrefix}.ADD"
                    String groupRemoveAttributeName = "${objectDef.groupDirectiveMetaAttributePrefix}.REMOVE"
                    if (attrMapCopy[groupAddAttributeName]) {
                        requestedGroupAdditions = attrMapCopy[groupAddAttributeName] instanceof Collection || attrMapCopy[groupAddAttributeName].getClass().array ? attrMapCopy[groupAddAttributeName] as List<String> : [attrMapCopy[groupAddAttributeName]] as List<String>
                    } else {
                        requestedGroupAdditions = []
                    }
                    attrMapCopy.remove(groupAddAttributeName)
                    if (attrMapCopy[groupRemoveAttributeName]) {
                        requestedGroupRemovals = attrMapCopy[groupRemoveAttributeName] instanceof Collection || attrMapCopy[groupRemoveAttributeName].getClass().array ? attrMapCopy[groupRemoveAttributeName] as List<String> : [attrMapCopy[groupRemoveAttributeName]] as List<String>
                    } else {
                        requestedGroupRemovals = []
                    }
                    attrMapCopy.remove(groupRemoveAttributeName)
                }

                if (existingEntry) {
                    // Already exists -- update

                    Name originalDn = existingEntry.dn

                    // Check for need to move DNs
                    if (renamingEnabled && dn && !nameEquals(objectDef, originalDn, dn)) {
                        // Move DN
                        rename(reqCtx, pkey, originalDn, dn)
                        try {
                            existingEntry = lookup(reqCtx, dn)
                        }
                        catch (NameNotFoundException ignored) {
                            existingEntry = null
                        }
                        if (!existingEntry) {
                            throw new LdapConnectorException("Unable to lookup $dn right after an existing object was renamed to this DN from the old $originalDn")
                        }
                        isModified = true
                        wasRenamed = true
                    }

                    // Do group membership additions (removals done after
                    // person entry has been updated)
                    if (doGroupMembershipChanges(reqCtx, requestedGroupAdditions, null, existingEntry)) {
                        isModified = true
                    }

                    if (!existingEntry.updateMode) {
                        existingEntry.updateMode = true
                    }

                    if (update(
                            reqCtx,
                            foundObjectMethod,
                            pkey,
                            existingEntry,
                            attrMapCopy
                    )) {
                        isModified = true
                    }

                    // Do group membership removals
                    if (doGroupMembershipChanges(reqCtx, null, requestedGroupRemovals, existingEntry)) {
                        isModified = true
                    }

                    //
                    // If we're updating with renaming disabled and the DN
                    // on the existing object is different than the
                    // "requested DN", then report a possible change of
                    // global unique identifier via a callback.
                    //
                    // Do the same if the globally unique identifier is
                    // missing from the input.  We want to give the caller
                    // the chance to store it and send it next time in the
                    // attrMap.
                    //
                    boolean renamingDisabledCase = !renamingEnabled &&
                            uniqueIdentifierAttrName &&
                            uniqueIdentifierEventCallbacks &&
                            dn && !nameEquals(objectDef, existingEntry.dn, dn)
                    boolean missingUniqIdCase = !wasRenamed &&
                            uniqueIdentifierAttrName &&
                            uniqueIdentifierEventCallbacks &&
                            !attrMap[uniqueIdentifierAttrName]
                    if (renamingDisabledCase || missingUniqIdCase) {
                        // Renaming disabled and the requested dn doesn't
                        // match the actual dn, indicating a rename from
                        // somewhere else, which could have resulted in a
                        // globally unique identifier change.  If renamed,
                        // existingEntry object was replaced with new entry.
                        Object directoryUniqueIdentifier = getGloballyUniqueIdentifier(reqCtx, existingEntry.dn)
                        if (!directoryUniqueIdentifier) {
                            log.warn("The ${((LdapObjectDefinition) objectDef).globallyUniqueIdentifierAttributeName} was unable to be retrieved from the just updated entry of ${existingEntry.dn}")
                        } else {
                            if (directoryUniqueIdentifier) {
                                deliverCallbackMessage(new LdapUniqueIdentifierEventMessage(
                                        success: true,
                                        causingEvent: LdapEventType.UPDATE_EVENT,
                                        eventId: eventId,
                                        objectDef: (LdapObjectDefinition) objectDef,
                                        context: (LdapCallbackContext) context,
                                        pkey: pkey,
                                        oldDn: originalDn,
                                        newDn: dn,
                                        globallyUniqueIdentifier: directoryUniqueIdentifier,
                                        wasRenamed: wasRenamed
                                ))
                            }
                        }
                    }
                } else {
                    // Doesn't already exist -- create
                    if (!dn) {
                        throw new LdapConnectorException("Unable to find existing object in directory by pkey $pkey but unable to insert a new object because the dn was not provided")
                    }
                    Object insertedGloballyUniqId = insert(reqCtx, pkey, dn, attrMapCopy)
                    isModified = true

                    boolean hasUpdateOnlyAttributes = ((LdapObjectDefinition) objectDef).dynamicAttributeNames.any {
                        it.endsWith(".ONUPDATE") && attrMap.containsKey(it)
                    }
                    boolean hasGroupDirectiveAttributes = ((LdapObjectDefinition) objectDef).groupDirectiveMetaAttributePrefix && (attrMap.containsKey("${objectDef.groupDirectiveMetaAttributePrefix}.ADD".toString()) || attrMap.containsKey("${objectDef.groupDirectiveMetaAttributePrefix}.REMOVAL".toString()))
                    if (hasUpdateOnlyAttributes || hasGroupDirectiveAttributes) {
                        // Since there are update-only or group directive attributes, we do a
                        // subsequent update after the insert, but only if
                        // we found the object we just inserted.
                        if (insertedGloballyUniqId) {
                            LinkedHashMap<String, Object> attrMapForUpdate = new LinkedHashMap<String, Object>(attrMap)
                            attrMapForUpdate[((LdapObjectDefinition) objectDef).globallyUniqueIdentifierAttributeName] = insertedGloballyUniqId
                            persist(eventId, objectDef, context, attrMapForUpdate, false)
                        } else {
                            log.warn("pkey $pkey has ONUPDATE or group directive attributes but we couldn't perform an update after the insert because we couldn't find the object right after inserting it")
                        }
                    }
                }
            } else {
                // is a deletion for the DN and/or pkey

                if (!dn && !pkey) {
                    throw new LdapConnectorException("When deleting, at least one of dn or $pkeyAttrName must be set in the attribute map")
                }

                // Delete by DN
                if (dn) {
                    try {
                        DirContextAdapter entryByDN = lookup(reqCtx, dn)
                        String entryByDNPkey = ((Attribute) entryByDN.attributes.all.find { Attribute attr -> attr.ID == pkeyAttrName })?.get()
                        delete(reqCtx, entryByDNPkey, entryByDN.dn)
                        isModified = true
                    }
                    catch (NameNotFoundException ignored) {
                        // not found by DN: no-op
                    }
                }

                // Delete by primary key
                if (pkey) {
                    if (((LdapObjectDefinition) objectDef).removeDuplicatePrimaryKeys) {
                        List<DirContextAdapter> searchResults = searchByPrimaryKey(reqCtx, pkey)
                        searchResults.each { DirContextAdapter entry ->
                            String entryPkey = ((Attribute) entry.attributes.all.find { Attribute attr -> attr.ID == pkeyAttrName })?.get()
                            delete(reqCtx, entryPkey, entry.dn)
                            isModified = true
                        }
                    }
                }
            }

            return isModified
        }
        catch (LdapConnectorException e) {
            exception = e
            throw e
        }
        catch (Throwable t) {
            exception = new LdapConnectorException(t)
            throw exception
        }
        finally {
            ((SingleContextSource) reqCtx.ldapTemplate.contextSource).destroy()
            deliverCallbackMessage(new LdapPersistCompletionEventMessage(
                    success: exception != null,
                    eventId: reqCtx.eventId,
                    objectDef: reqCtx.objectDef,
                    context: reqCtx.context,
                    exception: exception
            ))
        }
    }

    /**
     * Create a Name object that represents a distinguished name string.
     */
    @SuppressWarnings("GrMethodMayBeStatic")
    Name buildDnName(String dn) {
        return LdapNameBuilder.newInstance(dn).build()
    }

    /**
     * Convert a map to an Attributes object that contains all the keys and
     * values from the map.
     */
    @SuppressWarnings("GrMethodMayBeStatic")
    Attributes buildAttributes(Map<String, Object> attrMap) {
        Attributes attrs = new BasicAttributes()
        attrMap.each { entry ->
            if (entry.value instanceof List) {
                if (((List) entry.value).size()) {
                    BasicAttribute attr = new BasicAttribute(entry.key)
                    entry.value.each {
                        attr.add(it)
                    }
                    attrs.put(attr)
                } else {
                    attrs.remove(entry.key)
                }
            } else if (entry.value != null) {
                attrs.put(entry.key, entry.value)
            } else {
                attrs.remove(entry.key)
            }
        }
        return attrs
    }

    /**
     * Normalize a caller-provided map of attribute values.
     */
    protected Map<String, Object> convertCallerProvidedMap(Map<String, Object> map) {
        return (Map<String, Object>) map.findAll { it.value != null && !(it.value instanceof List && !((List) it.value).size()) }.collectEntries {
            [it.key, (it.value instanceof List ? convertCallerProvidedList((List) it.value) : convertCallerProvidedValue(it.value))]
        }
    }

    /**
     * Normalize a caller-provided list of attribute values.
     */
    @SuppressWarnings("GrMethodMayBeStatic")
    protected Object convertCallerProvidedList(List list) {
        if (list.size() == 1) {
            return convertCallerProvidedValue(list.first())
        } else {
            return list.collect { convertCallerProvidedValue(it) }
        }
    }

    /**
     * Normalize a caller-provided value.
     */
    @SuppressWarnings("GrMethodMayBeStatic")
    protected Object convertCallerProvidedValue(Object value) {
        if (value instanceof String || value instanceof Number || value instanceof Boolean) {
            // Directory servers interpret numbers and booleans as strings,
            // so we use toString()
            return value.toString()
        } else if (value instanceof byte[]) {
            // an example of using bytes for an attribute: AD unicodePwd
            return value
        } else if (value == null) {
            return null
        } else {
            throw new RuntimeException("Type ${value.getClass().name} is not a recognized list, string, number, boolean or null type")
        }
    }

    /**
     * Retrieve the globally unique identifier for a DN.
     *
     * @param reqCtx Context for the request
     * @param dn Distinguished name to retrieve the globally unique
     *        identifier for
     * @return The globally unique identifier value
     */
    Object getGloballyUniqueIdentifier(LdapRequestContext reqCtx, Name dn) {
        if (reqCtx.objectDef.globallyUniqueIdentifierAttributeName) {
            DirContextAdapter newEntry = null
            try {
                newEntry = lookup(reqCtx, dn, [reqCtx.objectDef.globallyUniqueIdentifierAttributeName] as String[])
            }
            catch (NameNotFoundException ignored) {
                // no-op
            }
            if (newEntry) {
                Attribute attr = newEntry?.attributes?.get(reqCtx.objectDef.globallyUniqueIdentifierAttributeName)
                if (attr?.size()) {
                    return attr.get()
                } else {
                    return null
                }
            } else {
                return null
            }
        } else {
            throw new LdapConnectorException("objectDef has no globallyUniqueIdentifierAttributeName set")
        }
    }

    /**
     * Remove attributes from an existing directory entry.  A
     * LdapConnectorException is possibly thrown, due to a server-side
     * error, if you attempt to remove an attribute that doesn't exist for
     * the entry.  This behavior is dependent on the directory server
     * implementation.  ApacheDS is confirmed to error out in this
     * situation.  OpenDJ behavior is not confirmed.
     *
     * It's possible to use persist() to remove attributes by setting the attribute value to a null, but it doesn't work for "write-only" attributes (e.g., userPassword).  This method supports "write-only" attributes.
     *
     * @param reqCtx Context for the request
     * @param dn (optional) DN of the object to modify
     * @param primaryKeyAttrValue Primary key attribute value of the object to modify
     * @param globallyUniqueIdentifierAttrValue (optional) Globally unique identifier value of the object to modify
     * @param attributeNamesToRemove A string array of attribute names to remove
     * @return true if an update actually occured in the directory.  false
     *         may be returned if the object is unchanged or not found.
     * @throws LdapConnectorException If an error occurs
     */
    boolean removeAttributes(
            LdapRequestContext reqCtx,
            Name dn,
            String primaryKeyAttrValue,
            Object globallyUniqueIdentifierAttrValue,
            String[] attributeNamesToRemove
    ) throws LdapConnectorException {
        MatchingEntryResult matchingEntryResult = null
        try {
            matchingEntryResult = findMatchingEntry(reqCtx, dn, primaryKeyAttrValue, globallyUniqueIdentifierAttrValue)
        }
        catch (Throwable t) {
            throw new LdapConnectorException(t)
        }
        if (!matchingEntryResult?.entry) {
            throw new LdapConnectorException(new ConnectorObjectNotFoundException("not found: dn=$dn, primaryKey=$primaryKeyAttrValue, globUniqId: $globallyUniqueIdentifierAttrValue"))
        }

        if (!matchingEntryResult.entry.updateMode) {
            matchingEntryResult.entry.updateMode = true
        }

        Throwable exception
        List modificationItems = []
        try {
            // Spring LDAP doesn't support removing write-only
            // attributes (like userPassword) so that's why we use
            // DirContext directly here.
            DirContext dirctx = reqCtx.ldapTemplate.contextSource.readWriteContext
            try {
                attributeNamesToRemove.each { String attrNameToRemove ->
                    ModificationItem item = new ModificationItem(DirContext.REMOVE_ATTRIBUTE, new BasicAttribute(attrNameToRemove, null))
                    modificationItems << item
                    dirctx.modifyAttributes(matchingEntryResult.entry.dn, item)
                }
            }
            finally {
                dirctx.close()
            }

            boolean isModified = modificationItems?.size()

            return isModified
        }
        catch (Throwable t) {
            exception = t
            throw new LdapConnectorException(t)
        }
        finally {
            deliverCallbackMessage(new LdapRemoveAttributesEventMessage(
                    success: exception == null,
                    eventId: reqCtx.eventId,
                    objectDef: reqCtx.objectDef,
                    context: reqCtx.context,
                    foundMethod: matchingEntryResult.foundObjectMethod,
                    pkey: primaryKeyAttrValue,
                    removedAttributeNames: attributeNamesToRemove,
                    dn: dn,
                    modificationItems: modificationItems,
                    exception: exception
            ))
        }
    }

    /**
     * Set an attribute for an existing directory entry.
     *
     * Normally persist() is used to add or modify attribute values, but
     * persist() doesn't work when trying to modify a "write-only" attribute
     * (e.g., userPassword).  This method supports "write-only" attributes.
     */
    boolean setAttribute(
            LdapRequestContext reqCtx,
            Name dn,
            String primaryKeyAttrValue,
            Object globallyUniqueIdentifierAttrValue,
            String attributeName,
            Object newAttributeValue,
            boolean useRemoveAndAddApproach = false,
            Object oldAttributeValue = null // when useRemoveAndAddApproach is true
    ) throws LdapConnectorException {
        MatchingEntryResult matchingEntryResult = null
        try {
            matchingEntryResult = findMatchingEntry(reqCtx, dn, primaryKeyAttrValue, globallyUniqueIdentifierAttrValue)
        }
        catch (Throwable t) {
            throw new LdapConnectorException(t)
        }
        if (!matchingEntryResult?.entry) {
            throw new LdapConnectorException(new ConnectorObjectNotFoundException("not found: dn=$dn, primaryKey=$primaryKeyAttrValue, globUniqId: $globallyUniqueIdentifierAttrValue"))
        }

        if (!matchingEntryResult.entry.updateMode) {
            matchingEntryResult.entry.updateMode = true
        }

        Throwable exception
        ModificationItem[] items = null
        try {
            // Spring LDAP doesn't support write-only attributes (like
            // userPassword) so that's why we use DirContext directly
            // here.
            DirContext dirctx = reqCtx.ldapTemplate.contextSource.readWriteContext
            try {
                if (!useRemoveAndAddApproach) {
                    ModificationItem item = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, new BasicAttribute(attributeName, newAttributeValue))
                    items = [item]
                    dirctx.modifyAttributes(matchingEntryResult.entry.dn, items)
                } else {
                    // Active Directory requires this approach when user
                    // changes own password.  First try remove and add and
                    // if that fails, try just an add.
                    try {
                        ModificationItem removeItem = new ModificationItem(DirContext.REMOVE_ATTRIBUTE, new BasicAttribute(attributeName, oldAttributeValue))
                        ModificationItem addItem = new ModificationItem(DirContext.ADD_ATTRIBUTE, new BasicAttribute(attributeName, newAttributeValue))
                        items = [removeItem, addItem]
                        dirctx.modifyAttributes(matchingEntryResult.entry.dn, items)
                    }
                    catch (NoSuchAttributeException ignored) {
                        ModificationItem addItem = new ModificationItem(DirContext.ADD_ATTRIBUTE, new BasicAttribute(attributeName, newAttributeValue))
                        items = [addItem]
                        dirctx.modifyAttributes(matchingEntryResult.entry.dn, items)
                    }
                }
            }
            finally {
                dirctx.close()
            }

            return true
        }
        catch (Throwable t) {
            exception = t
            throw new LdapConnectorException(t)
        }
        finally {
            deliverCallbackMessage(new LdapSetAttributeEventMessage(
                    success: exception == null,
                    eventId: reqCtx.eventId,
                    objectDef: reqCtx.objectDef,
                    context: reqCtx.context,
                    foundMethod: matchingEntryResult.foundObjectMethod,
                    pkey: primaryKeyAttrValue,
                    attributeName: attributeName,
                    attributeValue: newAttributeValue,
                    dn: dn,
                    modificationItems: items,
                    exception: exception
            ))
        }
    }

    /**
     * If objectDef indicates that case sensitive DN checking is enabled,
     * then the attribute values of name1 and name2 are checked with case
     * sensitivity.  Otherwise, name1 and name2 are compared without case
     * sensitivity.  Different implementations of LDAP and AD servers behave
     * differently in regards to DN case sensitivity so this is why it's
     * configurable in the objectDef.
     *
     * @param objectDef Object definition
     * @param name1 Left name to check for equality
     * @param name2 Left name to check for equality
     * @return true if name1 and name2 are considered equivalent
     */
    static boolean nameEquals(ObjectDefinition objectDef, Name name1, Name name2) {
        if (((LdapObjectDefinition) objectDef).caseSensitiveDnCheckingEnabled) {
            return caseSensitiveNameEquals(name1, name2)
        } else {
            return name1 == name2
        }
    }

    /**
     * {@link Name#equals} is not case sensitive.  This method will do an
     * equality check on name1 and name2 (typically both {@link LdapName}s)
     * where {@link Rdn} attribute values are checked with case sensitivity. 
     * {@link Rdn} attribute names are still case insensitive.  If name1 and
     * name2 aren't both {@link LdapName}s, then equality checking is done
     * with {@link Name#toString} and the whole name strings are compared
     * with case sensitivity, both attribute names and values.
     *
     * Note that DN case sensitivity is treated differently with different
     * LDAP or AD server implementations.  For example, when attempting a
     * MODRDN operation where the new string is in a different case than the
     * original DN, this will succeed.  For Apache Derby, this will result
     * in a NameAlreadyBoundException.
     *
     * @param name1 Left name to check for equality
     * @param name2 Left name to check for equality
     * @return true if name1 and name2 are considered equivalent
     */
    static boolean caseSensitiveNameEquals(Name name1, Name name2) {
        // Allow one of the parameters to be null but not both
        if (name1 == null && name2 == null) {
            throw new NullPointerException("Both name1 and name2 are null")
        }
        if ((name1 == null && name2 != null) || (name1 != null && name2 == null)) {
            return false
        }
        if (name1 instanceof LdapName && name2 instanceof LdapName) {
            def name1Map = name1.rdns.collectEntries { Rdn rdn ->
                // rdn.type is the attribute name and we store it lower case
                // to make it case insensitive when we compare maps
                [rdn.type.toLowerCase(), rdn.value]
            }
            def name2Map = name2.rdns.collectEntries { Rdn rdn ->
                [rdn.type.toLowerCase(), rdn.value]
            }
            return name1Map == name2Map

        } else {
            // not both LdapNames - use toString() to compare
            return name1.toString() == name2.toString()
        }
    }

    /**
     * @return A LdapTemplate built with a SingleContextSource that wraps
     * the parent contextSource, which will cause the LDAP connection to be
     * used across calls of this same ldapTemplate instance.  The caller
     * must ensure the contextSource is closed when done with the returned
     * ldapTemplate by using something like:
     * <code>((SingleContextSource)ldapTemplate.contextSource).destroy()</code>
     */
    protected LdapTemplate getSingleContextLdapTemplate() {
        return new LdapTemplate(new SingleContextSource(contextSource.readWriteContext))
    }

    /**
     * @return true if there was at least one group membership modification
     */
    @SuppressWarnings("GrMethodMayBeStatic")
    protected boolean doGroupMembershipChanges(LdapRequestContext reqCtx, List<String> requestedGroupAdditions, List<String> requestedGroupRemovals, DirContextAdapter existingEntry) {
        requestedGroupAdditions?.each { String groupDN ->
            addDnToGroup(reqCtx, existingEntry.dn.toString(), buildDnName(groupDN))
        }
        requestedGroupRemovals?.each { String groupDN ->
            removeDnFromGroup(reqCtx, existingEntry.dn.toString(), buildDnName(groupDN))
        }
        return requestedGroupAdditions || requestedGroupRemovals
    }

    @SuppressWarnings("GrMethodMayBeStatic")
    void addDnToGroup(LdapRequestContext reqCtx, String memberDN, Name groupDN) throws LdapConnectorException {
        try {
            ModificationItem mod = new ModificationItem(DirContext.ADD_ATTRIBUTE, new BasicAttribute(reqCtx.objectDef.groupMemberAttributeName, memberDN))
            reqCtx.ldapTemplate.modifyAttributes(groupDN, [mod] as ModificationItem[])
        }
        catch (Throwable t) {
            throw new LdapConnectorException(t)
        }
    }

    @SuppressWarnings("GrMethodMayBeStatic")
    void removeDnFromGroup(LdapRequestContext reqCtx, String memberDN, Name groupDN) throws LdapConnectorException {
        try {
            ModificationItem mod = new ModificationItem(DirContext.REMOVE_ATTRIBUTE, new BasicAttribute(reqCtx.objectDef.groupMemberAttributeName, memberDN))
            reqCtx.ldapTemplate.modifyAttributes(groupDN, [mod] as ModificationItem[])
        }
        catch (Throwable t) {
            throw new LdapConnectorException(t)
        }
    }
}
