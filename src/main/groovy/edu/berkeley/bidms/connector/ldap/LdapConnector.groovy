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
import edu.berkeley.bidms.connector.ObjectDefinition
import edu.berkeley.bidms.connector.ldap.event.LdapCallbackContext
import edu.berkeley.bidms.connector.ldap.event.LdapDeleteEventCallback
import edu.berkeley.bidms.connector.ldap.event.LdapEventType
import edu.berkeley.bidms.connector.ldap.event.LdapInsertEventCallback
import edu.berkeley.bidms.connector.ldap.event.LdapRemoveAttributesEventCallback
import edu.berkeley.bidms.connector.ldap.event.LdapRenameEventCallback
import edu.berkeley.bidms.connector.ldap.event.LdapUniqueIdentifierEventCallback
import edu.berkeley.bidms.connector.ldap.event.LdapUpdateEventCallback
import edu.berkeley.bidms.connector.ldap.event.message.LdapDeleteEventMessage
import edu.berkeley.bidms.connector.ldap.event.message.LdapEventMessage
import edu.berkeley.bidms.connector.ldap.event.message.LdapInsertEventMessage
import edu.berkeley.bidms.connector.ldap.event.message.LdapRemoveAttributesEventMessage
import edu.berkeley.bidms.connector.ldap.event.message.LdapRenameEventMessage
import edu.berkeley.bidms.connector.ldap.event.message.LdapUniqueIdentifierEventMessage
import edu.berkeley.bidms.connector.ldap.event.message.LdapUpdateEventMessage
import groovy.util.logging.Slf4j
import org.springframework.ldap.NameNotFoundException
import org.springframework.ldap.core.ContextMapper
import org.springframework.ldap.core.DirContextAdapter
import org.springframework.ldap.core.LdapTemplate
import org.springframework.ldap.query.LdapQuery
import org.springframework.ldap.support.LdapNameBuilder

import javax.naming.Name
import javax.naming.directory.Attribute
import javax.naming.directory.Attributes
import javax.naming.directory.BasicAttribute
import javax.naming.directory.BasicAttributes
import javax.naming.directory.DirContext
import javax.naming.directory.ModificationItem

/**
 * Connector for LDAP and Active Directory directory servers.
 */
@Slf4j
class LdapConnector implements Connector {

    LdapTemplate ldapTemplate

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
     * For queuing up asynchronous callback messages
     */
    private final LinkedList<LdapEventMessage> callbackMessageQueue = (LinkedList<LdapEventMessage>) Collections.synchronizedList(new LinkedList<LdapEventMessage>());

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
            default:
                throw new RuntimeException("Unknown LdapEventType for event message: ${eventMessage.eventType}")
        }
    }

    /**
     * The callback queue monitor thread calls this to see if there are
     * messages in the callback queue.
     *
     * @return The next message in the callback queue or null if the queue
     *         is empty.
     */
    protected LdapEventMessage pollCallbackQueue() {
        return callbackMessageQueue.poll()
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
            synchronized (callbackMonitorThread) {
                callbackMessageQueue.add(eventMessage)
                callbackMonitorThread.notify()
            }
        }
    }

    /**
     * Search the directory for objects for a primary key if
     * search-by-primary-key is enabled.  Search-by-primary-key is disabled
     * if objectDef.getLdapQueryForPrimaryKey(pkey) returns null.
     *
     * @param eventId Event id
     * @param objectDef Object definition
     * @param context Callback context
     * @param pkey Primary key
     * @return A list of objects found in the directory if search by primary
     *         key is enabled, null otherwise.
     */
    List<DirContextAdapter> searchByPrimaryKey(String eventId, LdapObjectDefinition objectDef, LdapCallbackContext context, String pkey) {
        LdapQuery query = objectDef.getLdapQueryForPrimaryKey(pkey)
        return (query ? ldapTemplate.search(query, toDirContextAdapterContextMapper) : null)
    }

    /**
     * Search the directory for an object by its DN.
     *
     * @param eventId Event id
     * @param objectDef Object definition
     * @param context Callback context
     * @param dn Distinguished name
     * @param attributes Optionally, a list of attributes to return for each
     *        object.  If null, returns all attributes except operational
     *        attributes.  If operational attributes are desired, they have
     *        to be specified.
     * @return The found directory object or null if it was not found
     */
    DirContextAdapter lookup(String eventId, LdapObjectDefinition objectDef, LdapCallbackContext context, String dn, String[] attributes = null) {
        if (!attributes) {
            return (DirContextAdapter) ldapTemplate.lookup(buildDnName(dn))
        } else {
            return (DirContextAdapter) ldapTemplate.lookup(buildDnName(dn), attributes, toDirContextAdapterContextMapper)
        }
    }

    /**
     * Search the directory for an object by its globally unique identifier.
     *
     * @param eventId Event id
     * @param objectDef Object definition
     * @param context Callback context
     * @param pkey When searching by globally unique identifier, the object
     *        must also match this expected primary key.
     * @param uniqueIdentifier Globally unique identifier
     * @return The found directory object or null if it was not found
     */
    DirContextAdapter lookupByGloballyUniqueIdentifier(
            String eventId,
            LdapObjectDefinition objectDef,
            LdapCallbackContext context,
            String pkey,
            Object uniqueIdentifier
    ) {
        return ldapTemplate.searchForObject(objectDef.getLdapQueryForGloballyUniqueIdentifier(pkey, uniqueIdentifier), toDirContextAdapterContextMapper)
    }

    /**
     * Delete an object in the directory matching the DN.  The primary key
     * (pkey) parameter is only passed in to pass back to the delete
     * callback and is otherwise unused in determining which object to
     * delete.
     *
     * @param eventId Event id
     * @param objectDef Object definition
     * @param context Callback context
     * @param pkey The primary key.  The parameter is only passed in to pass
     *        back to the delete callback and is otherwise unused in
     *        determining which object to delete.
     * @param dn The distinguished name of the object to delete
     * @throws LdapConnectorException If an error occurs
     */
    void delete(
            String eventId,
            LdapObjectDefinition objectDef,
            LdapCallbackContext context,
            String pkey,
            String dn
    ) throws LdapConnectorException {
        Throwable exception
        try {
            ldapTemplate.unbind(buildDnName(dn))
        }
        catch (Throwable t) {
            exception = t
            throw new LdapConnectorException(t)
        }
        finally {
            deliverCallbackMessage(new LdapDeleteEventMessage(
                    success: exception == null,
                    eventId: eventId,
                    objectDef: objectDef,
                    context: context,
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
     * @param eventId Event id
     * @param objectDef Object definition
     * @param context Callback context
     * @param pkey Primary key
     * @param oldDn The original distinguished name of the object to be
     *        renamed.
     * @param newDn The new distingished name of the object after it is
     *        renamed.
     * @throws LdapConnectorException If an error occurs
     */
    void rename(
            String eventId,
            LdapObjectDefinition objectDef,
            LdapCallbackContext context,
            String pkey,
            String oldDn,
            String newDn
    ) throws LdapConnectorException {
        Throwable exception
        Object directoryUniqueIdentifier = null
        try {
            ldapTemplate.rename(buildDnName(oldDn), buildDnName(newDn))

            if (objectDef.globallyUniqueIdentifierAttributeName && uniqueIdentifierEventCallbacks) {
                // Get the possibly-changed unique identifier for the
                // renamed object so we can pass it back in the unique identifier
                // callback
                directoryUniqueIdentifier = getGloballyUniqueIdentifier(eventId, (LdapObjectDefinition) objectDef, context, newDn)
                if (!directoryUniqueIdentifier) {
                    log.warn("The ${objectDef.globallyUniqueIdentifierAttributeName} was unable to be retrieved from the just renamed entry of $newDn")
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
                    eventId: eventId,
                    objectDef: objectDef,
                    context: context,
                    pkey: pkey,
                    oldDn: oldDn,
                    newDn: newDn,
                    exception: exception
            ))

            if (!exception && directoryUniqueIdentifier) {
                deliverCallbackMessage(new LdapUniqueIdentifierEventMessage(
                        success: true,
                        causingEvent: LdapEventType.RENAME_EVENT,
                        eventId: eventId,
                        objectDef: objectDef,
                        context: context,
                        pkey: pkey,
                        oldDn: oldDn,
                        newDn: newDn,
                        globallyUniqueIdentifier: directoryUniqueIdentifier
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
            return str.toLowerCase().equals(obj?.toString()?.trim()?.toLowerCase())
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
     * @param eventId Event id
     * @param objectDef Object definition
     * @param context Callback context
     * @param foundObjectMethod
     * @param pkey Primary key
     * @param existingEntry The existing directory entry to update
     * @param dn Distinguished name of the object being updated
     * @param newReplaceAttributeMap Attributes to replace where the keys in
     *        the map are attribute names.  For changes to be detected
     *        properly the attribute names must match the case of the
     *        attribute in the directory.
     * @param newAppendOnlyAttributeMap Multi-value attributes to append to
     *        where the keys in the map are the attribute names.  For
     *        changes to be detected properly the attribute names must match
     *        the case of the attribute in the directory.
     * @return true if an update actually occured in the directory.  false
     *         may be returned if the object is unchanged.
     * @throws LdapConnectorException If an error occurs
     */
    boolean update(
            String eventId,
            LdapObjectDefinition objectDef,
            LdapCallbackContext context,
            FoundObjectMethod foundObjectMethod,
            String pkey,
            DirContextAdapter existingEntry,
            String dn,
            Map<String, Object> newReplaceAttributeMap,
            Map<String, List<Object>> newAppendOnlyAttributeMap
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
            if (objectDef.isKeepExistingAttributesWhenUpdating()) {
                attributesToKeepOrUpdate = new LinkedHashMap<String, Object>(oldAttributeMap)
                attributesToKeepOrUpdate.putAll(convertedNewAttributeMap)
            } else {
                attributesToKeepOrUpdate = convertedNewAttributeMap
            }

            newAppendOnlyAttributeMap?.each {
                if (attributesToKeepOrUpdate.containsKey(it.key)) {
                    // append to the existing list, but prevent case-insensitive duplicates
                    if (!(attributesToKeepOrUpdate[it.key] instanceof List)) {
                        attributesToKeepOrUpdate[it.key] = [attributesToKeepOrUpdate[it.key]]
                    }
                    HashSet<CaseInsensitiveString> set = new HashSet<CaseInsensitiveString>(((List) attributesToKeepOrUpdate[it.key]).collect { new CaseInsensitiveString(it.toString().trim()) })
                    if (it.value instanceof List) {
                        set.addAll(((List) it.value).collect { new CaseInsensitiveString(it.toString().trim()) })
                    } else {
                        set.add(new CaseInsensitiveString(it.value.toString().trim()))
                    }
                    attributesToKeepOrUpdate[it.key] = new ArrayList<String>(set*.toString())
                } else {
                    // insert
                    attributesToKeepOrUpdate[it.key] = it.value
                }
            }

            Map<String, Object> changedAttributes = attributesToKeepOrUpdate - oldAttributeMap

            // Removing the attribute if keepExistingAttributes is false and
            // the attribute is not in the newAttributeMap or if the
            // attribute is explicitly set to null in the newAttributeMap.
            HashSet<String> attributeNamesToRemove = (
                    (!objectDef.isKeepExistingAttributesWhenUpdating() ? oldAttributeMap.keySet() - attributesToKeepOrUpdate.keySet() : []) as HashSet<String>
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
            ldapTemplate.modifyAttributes(existingEntry)

            return isModified
        }
        catch (Throwable t) {
            exception = t
            throw new LdapConnectorException(t)
        }
        finally {
            deliverCallbackMessage(new LdapUpdateEventMessage(
                    success: exception == null,
                    eventId: eventId,
                    objectDef: objectDef,
                    context: context,
                    foundMethod: foundObjectMethod,
                    pkey: pkey,
                    oldAttributes: oldAttributeMap,
                    dn: dn,
                    newAttributes: convertedNewAttributeMap ?: newReplaceAttributeMap,
                    modificationItems: modificationItems,
                    exception: exception
            ))
        }
    }

    /**
     * Insert a new object into the directory.
     *
     * @param eventId Event id
     * @param objectDef Object definition
     * @param context Callback context
     * @param pkey Primary key of object being created
     * @param dn Distinguished name of object being created
     * @param attributeMap Attributes for the object where the keys in the
     *        map are attribute names.
     * @throws LdapConnectorException If an error occurs
     */
    void insert(
            String eventId,
            LdapObjectDefinition objectDef,
            LdapCallbackContext context,
            String pkey,
            String dn,
            Map<String, Object> attributeMap
    ) throws LdapConnectorException {
        Throwable exception
        Map<String, Object> convertedNewAttributeMap = null
        Map<String, Object> convertedUpdateOnlyAttributeMap = (objectDef.updateOnlyAttributeNames ? [:] : null)
        Object directoryUniqueIdentifier = null
        try {
            convertedNewAttributeMap = convertCallerProvidedMap(attributeMap)

            // Remove update-only attributes from the map, since we're about
            // to insert.
            objectDef.updateOnlyAttributeNames?.each {
                if (convertedNewAttributeMap.containsKey(it)) {
                    convertedUpdateOnlyAttributeMap[it] = convertedNewAttributeMap[it]
                    convertedNewAttributeMap.remove(it)
                }
            }

            // Spring method naming is a little confusing.  Spring uses the
            // word "bind" and "rebind" to mean "create" and "update." In
            // this context, it does not mean "authenticate (bind) to the
            // directory server.
            ldapTemplate.bind(buildDnName(dn), null, buildAttributes(convertedNewAttributeMap))

            if (objectDef.globallyUniqueIdentifierAttributeName && uniqueIdentifierEventCallbacks) {
                // Get the newly-created directory unique identifier so we
                // can pass it back in the insert callback
                directoryUniqueIdentifier = getGloballyUniqueIdentifier(eventId, (LdapObjectDefinition) objectDef, context, dn)
                if (!directoryUniqueIdentifier) {
                    log.warn("The ${objectDef.globallyUniqueIdentifierAttributeName} was unable to be retrieved from the just inserted entry of $dn")
                }
            }
        }
        catch (Throwable t) {
            exception = t
            throw new LdapConnectorException(t)
        }
        finally {
            deliverCallbackMessage(new LdapInsertEventMessage(
                    success: exception == null,
                    eventId: eventId,
                    objectDef: objectDef,
                    context: context,
                    pkey: pkey,
                    dn: dn,
                    newAttributes: convertedNewAttributeMap ?: attributeMap,
                    exception: exception
            ))

            if (!exception && directoryUniqueIdentifier) {
                deliverCallbackMessage(new LdapUniqueIdentifierEventMessage(
                        success: true,
                        causingEvent: LdapEventType.INSERT_EVENT,
                        eventId: eventId,
                        objectDef: objectDef,
                        context: context,
                        pkey: pkey,
                        newDn: dn,
                        globallyUniqueIdentifier: directoryUniqueIdentifier
                ))
            }
        }

        if (convertedUpdateOnlyAttributeMap) {
            // Since there are update-only attributes, we do a subsequent
            // update after the insert.
            convertedUpdateOnlyAttributeMap.dn = dn
            convertedUpdateOnlyAttributeMap[objectDef.primaryKeyAttributeName] = convertedNewAttributeMap[objectDef.primaryKeyAttributeName]
            if (directoryUniqueIdentifier) {
                convertedUpdateOnlyAttributeMap[objectDef.globallyUniqueIdentifierAttributeName] = directoryUniqueIdentifier
            }
            persist(eventId, objectDef, context, convertedUpdateOnlyAttributeMap, false)
        }
    }

    static class MatchingEntryResult {
        DirContextAdapter entry
        FoundObjectMethod foundObjectMethod
        List<DirContextAdapter> searchResults
    }

    MatchingEntryResult findMatchingEntry(
            String eventId,
            ObjectDefinition objectDef,
            CallbackContext context,
            String dn,
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
        result.searchResults = searchByPrimaryKey(eventId, (LdapObjectDefinition) objectDef, (LdapCallbackContext) context, pkey)

        if (!result.entry && dn) {
            // Find entries with matching dn.  searchResults only
            // contains entries with matching pkey.
            result.entry = result.searchResults?.find { DirContextAdapter entry ->
                entry.dn.toString() == dn
            }
            if (result.entry) {
                result.foundObjectMethod = FoundObjectMethod.BY_DN_MATCHED_KEY
            }
        }

        // If none of the entries match DN but there's one value in
        // searchResults, use that.  If a dn was specified, this means
        // the DN has changed, but the object was found by pkey.
        if (!result.entry && result.searchResults?.size() == 1 && ((LdapObjectDefinition) objectDef).acceptAsExistingDn(result.searchResults.first().dn.toString())) {
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
                result.entry = lookupByGloballyUniqueIdentifier(eventId, (LdapObjectDefinition) objectDef, (LdapCallbackContext) context, pkey, uniqueIdentifier)
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
                    ((LdapObjectDefinition) objectDef).acceptAsExistingDn(entry.dn.toString())
                }
                if (result.entry) {
                    result.foundObjectMethod = FoundObjectMethod.BY_FIRST_FOUND
                }
            }
        }

        // DN may still exist but with a different primary key
        if (!result.entry && dn) {
            try {
                result.entry = lookup(eventId, (LdapObjectDefinition) objectDef, (LdapCallbackContext) context, dn)
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
     * @param eventId Event id
     * @param objectDef Object definition
     * @param context Callback context
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
        LinkedHashMap<String, Object> attrMapCopy = new LinkedHashMap<String, Object>(attrMap)

        // (optional) globally unique identifier
        String uniqueIdentifierAttrName = ((LdapObjectDefinition) objectDef).globallyUniqueIdentifierAttributeName
        Object uniqueIdentifier = null
        if (uniqueIdentifierAttrName) {
            uniqueIdentifier = attrMapCopy[uniqueIdentifierAttrName]
            // Remove the uniqueIdentifier from the object -- we're assuming
            // this is an operational attribute that we can't set.
            attrMapCopy.remove(uniqueIdentifierAttrName)
        }

        // primary key
        String pkeyAttrName = ((LdapObjectDefinition) objectDef).primaryKeyAttributeName
        String pkey = attrMapCopy[pkeyAttrName]
        if (!isDelete && !pkey) {
            throw new LdapConnectorException("Directory object is missing a required value for primary key $pkeyAttrName")
        }

        // (optional) DN
        String dn = attrMapCopy.dn
        // Remove the dn from the object -- not an actual attribute
        attrMapCopy.remove("dn")

        boolean isModified = false
        boolean wasRenamed = false

        if (!isDelete) {
            MatchingEntryResult matchingEntryResult = findMatchingEntry(eventId, objectDef, context, dn, pkey, uniqueIdentifier)
            DirContextAdapter existingEntry = matchingEntryResult.entry
            FoundObjectMethod foundObjectMethod = matchingEntryResult.foundObjectMethod

            if (((LdapObjectDefinition) objectDef).isRemoveDuplicatePrimaryKeys()) {
                // Delete all the entries that we're not keeping as the
                // existingEntry
                matchingEntryResult.searchResults.each { DirContextAdapter entry ->
                    if (entry.dn != existingEntry?.dn) {
                        delete(eventId, (LdapObjectDefinition) objectDef, (LdapCallbackContext) context, pkey, entry.dn.toString())
                        isModified = true
                    }
                }
            }

            boolean renamingEnabled = !((LdapObjectDefinition) objectDef).insertOnlyAttributeNames?.contains("dn")

            if (existingEntry) {
                // Already exists -- update

                String existingDn = existingEntry.dn

                // Check for need to move DNs
                if (renamingEnabled && dn && existingDn != dn) {
                    // Move DN
                    rename(eventId, (LdapObjectDefinition) objectDef, (LdapCallbackContext) context, pkey, existingDn, dn)
                    try {
                        existingEntry = lookup(eventId, (LdapObjectDefinition) objectDef, (LdapCallbackContext) context, dn)
                    }
                    catch (NameNotFoundException ignored) {
                        // no-op
                    }
                    if (!existingEntry) {
                        throw new LdapConnectorException("Unable to lookup $dn right after an existing object was renamed to this DN")
                    }
                    isModified = true
                    wasRenamed = true
                }

                Map<String, Object> newReplaceAttributeMap = new LinkedHashMap<String, Object>((Map<String, Object>) attrMapCopy.findAll {
                    !((LdapObjectDefinition) objectDef).insertOnlyAttributeNames?.contains(it.key)
                })
                Map<String, List<Object>> newAppendOnlyAttributeMap = [:]
                ((LdapObjectDefinition) objectDef).appendOnlyAttributeNames.each { String attrName ->
                    if (newReplaceAttributeMap.containsKey(attrName)) {
                        if (newReplaceAttributeMap[attrName] instanceof List) {
                            newAppendOnlyAttributeMap[attrName] = (List) newReplaceAttributeMap[attrName]
                        } else {
                            newAppendOnlyAttributeMap[attrName] = [newReplaceAttributeMap[attrName]]
                        }
                        newReplaceAttributeMap.remove(attrName)
                    }
                }

                if (!existingEntry.updateMode) {
                    existingEntry.updateMode = true
                }

                if (update(
                        eventId,
                        (LdapObjectDefinition) objectDef,
                        (LdapCallbackContext) context,
                        foundObjectMethod,
                        pkey,
                        existingEntry,
                        existingEntry.dn.toString(),
                        newReplaceAttributeMap,
                        newAppendOnlyAttributeMap
                )) {
                    isModified = true
                }

                //
                // If we're updating with renaming disabled and the DN on
                // the existing object is different than the "requested DN",
                // then report a possible change of global unique identifier
                // via a callback.
                //
                // Do the same if the globally unique identifier is missing
                // from the input.  We want to give the caller the chance
                // to store it and send it next time in the attrMap.
                //
                boolean renamingDisabledCase = !renamingEnabled &&
                        uniqueIdentifierAttrName &&
                        uniqueIdentifierEventCallbacks &&
                        dn && existingEntry.dn.toString() != dn
                boolean missingUniqIdCase = !wasRenamed &&
                        uniqueIdentifierAttrName &&
                        uniqueIdentifierEventCallbacks &&
                        !attrMap[uniqueIdentifierAttrName]
                if (renamingDisabledCase || missingUniqIdCase) {
                    // Renaming disabled and the requested dn doesn't match
                    // the actual dn, indicating a rename from somewhere
                    // else, which could have resulted in a globally unique
                    // identifier change.
                    Object directoryUniqueIdentifier = getGloballyUniqueIdentifier(eventId, (LdapObjectDefinition) objectDef, (LdapCallbackContext) context, existingEntry.dn.toString())
                    if (!directoryUniqueIdentifier) {
                        log.warn("The ${((LdapObjectDefinition) objectDef).globallyUniqueIdentifierAttributeName} was unable to be retrieved from the just updated entry of $newDn")
                    } else {
                        if (directoryUniqueIdentifier) {
                            deliverCallbackMessage(new LdapUniqueIdentifierEventMessage(
                                    success: true,
                                    causingEvent: LdapEventType.UPDATE_EVENT,
                                    eventId: eventId,
                                    objectDef: (LdapObjectDefinition) objectDef,
                                    context: (LdapCallbackContext) context,
                                    pkey: pkey,
                                    oldDn: dn,
                                    newDn: existingEntry.dn.toString(),
                                    globallyUniqueIdentifier: directoryUniqueIdentifier
                            ))
                        }
                    }
                }
            } else {
                // Doesn't already exist -- create
                if (!dn) {
                    throw new LdapConnectorException("Unable to find existing object in directory by pkey $pkey but unable to insert a new object because the dn was not provided")
                }
                insert(eventId, (LdapObjectDefinition) objectDef, (LdapCallbackContext) context, pkey, dn, attrMapCopy)
                isModified = true
            }
        } else {
            // is a deletion for the DN and/or pkey

            if (!dn && !pkey) {
                throw new LdapConnectorException("When deleting, at least one of dn or $pkeyAttrName must be set in the attribute map")
            }

            // Delete by DN
            if (dn) {
                try {
                    DirContextAdapter entryByDN = lookup(eventId, (LdapObjectDefinition) objectDef, (LdapCallbackContext) context, dn)
                    String entryByDNPkey = ((Attribute) entryByDN.attributes.all.find { Attribute attr -> attr.ID == pkeyAttrName })?.get()
                    delete(eventId, (LdapObjectDefinition) objectDef, (LdapCallbackContext) context, entryByDNPkey, entryByDN.dn.toString())
                    isModified = true
                }
                catch (NameNotFoundException ignored) {
                    // not found by DN: no-op
                }
            }

            // Delete by primary key
            if (pkey) {
                if (((LdapObjectDefinition) objectDef).removeDuplicatePrimaryKeys) {
                    List<DirContextAdapter> searchResults = searchByPrimaryKey(eventId, (LdapObjectDefinition) objectDef, (LdapCallbackContext) context, pkey)
                    searchResults.each { DirContextAdapter entry ->
                        String entryPkey = ((Attribute) entry.attributes.all.find { Attribute attr -> attr.ID == pkeyAttrName })?.get()
                        delete(eventId, (LdapObjectDefinition) objectDef, (LdapCallbackContext) context, entryPkey, entry.dn.toString())
                        isModified = true
                    }
                }
            }
        }

        return isModified
    }

    /**
     * Create a Name object that represents a distinguished name string.
     *
     * @param dn
     * @return
     */
    @SuppressWarnings("GrMethodMayBeStatic")
    Name buildDnName(String dn) {
        return LdapNameBuilder.newInstance(dn).build()
    }

    /**
     * Convert a map to an Attributes object that contains all the keys and
     * values from the map.
     *
     * @param attrMap
     * @return
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
     *
     * @param map
     * @return
     */
    private Map<String, Object> convertCallerProvidedMap(Map<String, Object> map) {
        return map.findAll { it.value != null && !(it.value instanceof List && !((List) it.value).size()) }.collectEntries {
            [it.key, (it.value instanceof List ? convertCallerProvidedList((List) it.value) : convertCallerProvidedValue(it.value))]
        }
    }

    /**
     * Normalize a caller-provided list of attribute values.
     *
     * @param list
     * @return
     */
    @SuppressWarnings("GrMethodMayBeStatic")
    private Object convertCallerProvidedList(List list) {
        if (list.size() == 1) {
            return convertCallerProvidedValue(list.first())
        } else {
            return list.collect { convertCallerProvidedValue(it) }
        }
    }

    /**
     * Normalize a caller-provided value.
     *
     * @param value
     * @return
     */
    @SuppressWarnings("GrMethodMayBeStatic")
    private Object convertCallerProvidedValue(Object value) {
        if (value instanceof String || value instanceof Number || value instanceof Boolean) {
            // Directory servers interpret numbers and booleans as strings,
            // so we use toString()
            return value.toString()
        } else if (value == null) {
            return null
        } else {
            throw new RuntimeException("Type ${value.getClass().name} is not a recognized list, string, number, boolean or null type")
        }
    }

    /**
     * Retrieve the globally unique identifier for a DN.
     *
     * @param eventId Event id
     * @param objectDef Object definition
     * @param context Callback context
     * @param dn Distinguished name to retrieve the globally unique
     *        identifier for
     * @return The globally unique identifier value
     */
    Object getGloballyUniqueIdentifier(String eventId, LdapObjectDefinition objectDef, LdapCallbackContext context, String dn) {
        if (objectDef.globallyUniqueIdentifierAttributeName) {
            DirContextAdapter newEntry = null
            try {
                newEntry = lookup(eventId, (LdapObjectDefinition) objectDef, (LdapCallbackContext) context, dn, [objectDef.globallyUniqueIdentifierAttributeName] as String[])
            }
            catch (NameNotFoundException ignored) {
                // no-op
            }
            if (newEntry) {
                Attribute attr = newEntry?.attributes?.get(objectDef.globallyUniqueIdentifierAttributeName)
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
     * @param eventId Event id
     * @param objectDef Object definition
     * @param context Callback context
     * @param dn (optional) DN of the object to modify
     * @param primaryKeyAttrValue Primary key attribute value of the object to modify
     * @param globallyUniqueIdentifierAttrValue (optional) Globally unique identifier value of the object to modify
     * @param attributeNamesToRemove A string array of attribute names to remove
     * @return true if an update actually occured in the directory.  false
     *         may be returned if the object is unchanged or not found.
     * @throws LdapConnectorException If an error occurs
     */
    boolean removeAttributes(
            String eventId,
            ObjectDefinition objectDef,
            CallbackContext context,
            String dn,
            String primaryKeyAttrValue,
            Object globallyUniqueIdentifierAttrValue,
            String[] attributeNamesToRemove
    ) throws LdapConnectorException {
        MatchingEntryResult matchingEntryResult = findMatchingEntry(eventId, objectDef, context, dn, primaryKeyAttrValue, globallyUniqueIdentifierAttrValue)

        if (matchingEntryResult?.entry) {
            if (!matchingEntryResult.entry.updateMode) {
                matchingEntryResult.entry.updateMode = true
            }

            Throwable exception
            List modificationItems = []
            try {
                // Spring LDAP doesn't support removing write-only
                // attributes (like userPassword) so that's why we use
                // DirContext directly here.
                DirContext dirctx = ldapTemplate.contextSource.readWriteContext
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
                        eventId: eventId,
                        objectDef: (LdapObjectDefinition) objectDef,
                        context: (LdapCallbackContext) context,
                        foundMethod: matchingEntryResult.foundObjectMethod,
                        pkey: primaryKeyAttrValue,
                        removedAttributeNames: attributeNamesToRemove,
                        dn: dn,
                        modificationItems: modificationItems,
                        exception: exception
                ))
            }
        } else {
            return false
        }
    }
}
