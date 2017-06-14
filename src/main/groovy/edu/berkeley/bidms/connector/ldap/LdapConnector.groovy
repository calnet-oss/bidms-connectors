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
import edu.berkeley.bidms.connector.ldap.event.*
import edu.berkeley.bidms.connector.ldap.event.message.*
import groovy.util.logging.Slf4j
import org.springframework.ldap.NameNotFoundException
import org.springframework.ldap.core.ContextMapper
import org.springframework.ldap.core.DirContextAdapter
import org.springframework.ldap.core.LdapTemplate
import org.springframework.ldap.support.LdapNameBuilder

import javax.naming.Name
import javax.naming.directory.*

@Slf4j
class LdapConnector implements Connector {

    LdapTemplate ldapTemplate
    ContextMapper<DirContextAdapter> toDirContextAdapterContextMapper = new ToDirContextAdapterContextMapper()
    ContextMapper<Map<String, Object>> toMapContextMapper = new ToMapContextMapper()

    List<LdapDeleteEventCallback> deleteEventCallbacks = []
    List<LdapRenameEventCallback> renameEventCallbacks = []
    List<LdapUpdateEventCallback> updateEventCallbacks = []
    List<LdapInsertEventCallback> insertEventCallbacks = []

    // For queuing up asynchronous callback messages
    private final LinkedList<LdapEventMessage> callbackMessageQueue = (LinkedList<LdapEventMessage>) Collections.synchronizedList(new LinkedList<LdapEventMessage>());
    // If true, calls to callbacks will be done synchronously instead of
    // asynchronously
    boolean isSynchronousCallback = false
    // Thread for monitoring the callback queue
    final Thread callbackMonitorThread = (!isSynchronousCallback ? new Thread() {
        volatile boolean requestStop = false

        @Override
        void run() {
            while (!requestStop) {
                try {
                    synchronized (this) {
                        wait(1000)
                        LdapEventMessage eventMessage = null
                        int dequeueCount = 0
                        while (!requestStop && (eventMessage = callbackMessageQueue.poll())) {
                            if (eventMessage != null) {
                                invokeCallback(eventMessage)
                                dequeueCount++
                            }
                        }
                        if (dequeueCount) {
                            notify()
                        }
                    }
                }
                catch (InterruptedException ignored) {
                    // no-op
                }
            }
        }
    } : null)

    void start() {
        if (!isSynchronousCallback) {
            callbackMonitorThread.start()
        }
    }

    void stop() {
        if (!isSynchronousCallback) {
            callbackMonitorThread.requestStop = true
            callbackMonitorThread.interrupt()
        }
    }

    private void invokeCallback(LdapEventMessage eventMessage) {
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
            default:
                throw new RuntimeException("Unknown LdapEventType for event message: ${eventMessage.eventType}")
        }
    }

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

    List<DirContextAdapter> searchByPrimaryKey(String eventId, LdapObjectDefinition objectDef, LdapCallbackContext context, String pkey) {
        return ldapTemplate.search(objectDef.getLdapQueryForPrimaryKey(pkey), toDirContextAdapterContextMapper)
    }

    List<DirContextAdapter> searchByGloballyUniqueIdentifierOrPrimaryKey(String eventId, LdapObjectDefinition objectDef, LdapCallbackContext context, Object uniqueIdentifier, String pkey) {
        return ldapTemplate.search(objectDef.getLdapQueryForGloballyUniqueIdentifierOrPrimaryKey(uniqueIdentifier, pkey), toDirContextAdapterContextMapper)
    }

    DirContextAdapter lookup(String eventId, LdapObjectDefinition objectDef, LdapCallbackContext context, String dn, String[] attributes = null) {
        if (!attributes) {
            return (DirContextAdapter) ldapTemplate.lookup(buildDnName(dn))
        } else {
            return (DirContextAdapter) ldapTemplate.lookup(buildDnName(dn), attributes, toDirContextAdapterContextMapper)
        }
    }

    DirContextAdapter lookupByGloballyUniqueIdentifier(
            String eventId,
            LdapObjectDefinition objectDef,
            LdapCallbackContext context,
            Object uniqueIdentifier
    ) {
        return ldapTemplate.searchForObject(objectDef.getLdapQueryForGloballyUniqueIdentifier(uniqueIdentifier), toDirContextAdapterContextMapper)
    }

    void delete(String eventId, LdapObjectDefinition objectDef, LdapCallbackContext context, String pkey, String dn) throws LdapConnectorException {
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
                    isSuccess: exception == null,
                    eventId: eventId,
                    objectDef: objectDef,
                    context: context,
                    pkey: pkey,
                    dn: dn,
                    exception: exception
            ))
        }
    }

    void rename(String eventId, LdapObjectDefinition objectDef, LdapCallbackContext context, String pkey, String oldDn, String newDn) throws LdapConnectorException {
        Throwable exception
        try {
            ldapTemplate.rename(buildDnName(oldDn), buildDnName(newDn))
        }
        catch (Throwable t) {
            exception = t
            throw new LdapConnectorException(t)
        }
        finally {
            deliverCallbackMessage(new LdapRenameEventMessage(
                    isSuccess: exception == null,
                    eventId: eventId,
                    objectDef: objectDef,
                    context: context,
                    pkey: pkey,
                    oldDn: oldDn,
                    newDn: newDn,
                    exception: exception
            ))
        }
    }

    /**
     * Update an existing LDAP object with values in newAttributeMap.
     *
     * Attribute names in the newReplaceAttributeMap and
     * newAppendOnlyAttributeMap are case sensitive.  Attribute strings must
     * match the case of the attribute names of the directory schema, as
     * retrieved from the directory via a search() or lookup().
     *
     * @return true if a modification occurred
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
                    // append
                    if (attributesToKeepOrUpdate[it.key] instanceof List) {
                        // it's already a list -- append to the existing
                        // list, but prevent duplicates
                        HashSet set = new HashSet((List) attributesToKeepOrUpdate[it.key])
                        set.addAll(it.value)
                        attributesToKeepOrUpdate[it.key] = new ArrayList(set)
                    } else {
                        // it's a single value -- create a list containing
                        // existing value plus new values, but prevent
                        // duplicates
                        HashSet set = new HashSet()
                        set.add(attributesToKeepOrUpdate[it.key])
                        set.addAll(it.value)
                        attributesToKeepOrUpdate[it.key] = new ArrayList(set)
                    }
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
                if (entry.value instanceof Collection) {
                    existingEntry.setAttributeValues(entry.key, ((Collection) entry.value).toArray())
                } else if (entry.value) {
                    existingEntry.setAttributeValues(entry.key, [entry.value] as Object[])
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
                    isSuccess: exception == null,
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
        Object directoryUniqueIdentifier = null
        try {
            convertedNewAttributeMap = convertCallerProvidedMap(attributeMap)
            ldapTemplate.bind(buildDnName(dn), null, buildAttributes(convertedNewAttributeMap))

            if (objectDef.globallyUniqueIdentifierAttributeName && insertEventCallbacks) {
                // Get the newly-created directory unique identifier so we
                // can pass it back in the insert callback
                DirContextAdapter newEntry = null
                try {
                    newEntry = lookup(eventId, (LdapObjectDefinition) objectDef, (LdapCallbackContext) context, dn, [objectDef.globallyUniqueIdentifierAttributeName] as String[])
                }
                catch (NameNotFoundException ignored) {
                    // no-op
                }
                if (!newEntry) {
                    throw new LdapConnectorException("Unable to lookup $dn right after inserting this DN")
                }
                Attribute attr = newEntry?.attributes?.get(objectDef.globallyUniqueIdentifierAttributeName)
                if (attr?.size()) {
                    directoryUniqueIdentifier = attr.get()
                } else {
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
                    isSuccess: exception == null,
                    eventId: eventId,
                    objectDef: objectDef,
                    context: context,
                    pkey: pkey,
                    dn: dn,
                    newAttributes: convertedNewAttributeMap ?: attributeMap,
                    globallyUniqueIdentifier: directoryUniqueIdentifier,
                    exception: exception
            ))
        }
    }

    /**
     * Persist the values in attrMap to LDAP.
     *
     * Attribute names in the attrMap are case sensitive.  Attribute strings
     * must match the case of the attribute names in the directory schema,
     * as when retrieved from the directory via a search() or lookup().
     *
     * @param objectDef Object definition of the LDAP object
     * @param attrMap The map of the attributes for the LDAP object.  Keys
     *        are case sensitive and must match the case of the attribute
     *        names in the directory schema.
     * @return true if a modification occurred
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
        if (!pkey) {
            throw new LdapConnectorException("LDAP object is missing a required value for primary key $pkeyAttrName")
        }

        // DN
        String dn = null
        if (!isDelete) {
            dn = attrMapCopy.dn
            if (!dn) {
                throw new LdapConnectorException("LDAP object for $pkeyAttrName $pkey does not contain the required dn attribute")
            }
        }
        // Remove the dn from the object -- not an actual attribute
        attrMapCopy.remove("dn")

        // Spring method naming is a little confusing.  Spring uses the word
        // "bind" and "rebind" to mean "create" and "update." In this
        // context, it does not mean "authenticate (bind) to the LDAP
        // server."

        // See if records belonging to the globally unique identifier or
        // pkey exist
        List<DirContextAdapter> searchResults = (uniqueIdentifier
                ? searchByGloballyUniqueIdentifierOrPrimaryKey(eventId, (LdapObjectDefinition) objectDef, (LdapCallbackContext) context, uniqueIdentifier, pkey)
                : searchByPrimaryKey(eventId, (LdapObjectDefinition) objectDef, (LdapCallbackContext) context, pkey))

        boolean isModified = false

        if (!isDelete) {
            //
            // There's only supposed to be one entry-per-pkey in the
            // directory, but this is not guaranteed to always be the case. 
            // When the search returns more than one entry, first see if
            // there's any one that already matches the primary key of our
            // downstream object and if that yields nothing, do the same for
            // the DN.  If none match and isRemoveDuplicatePrimaryKeys()
            // returns true, pick one to rename and delete the rest.
            //

            DirContextAdapter existingEntry = null
            FoundObjectMethod foundObjectMethod = null

            if (!existingEntry) {
                // Find entries with matching dn.  searchResults only
                // contains entries with matching unique identifier or pkey.
                existingEntry = searchResults?.find { DirContextAdapter entry ->
                    entry.dn.toString() == dn
                }
                if (existingEntry) {
                    // key is unique identifier or pkey
                    foundObjectMethod = FoundObjectMethod.BY_DN_MATCHED_KEY
                }
            }
            // If none of the entries match DN but there's one value in
            // searchResults, that means the DN has changed, but the object
            // was found by unique identifier or pkey
            if (!existingEntry && searchResults?.size() == 1 && ((LdapObjectDefinition) objectDef).acceptAsExistingDn(searchResults.first().dn.toString())) {
                existingEntry = searchResults.first()
                // key is unique identifier or pkey
                foundObjectMethod = FoundObjectMethod.BY_MATCHED_KEY_DN_MISMATCH
            }
            // If none of the entries match DN but there's more than one
            // value in searchResults, that means DN has changed and one of
            // the objects in the searchResults could be a match against the
            // globally unique identifier, which we want to prioritize over
            // the others
            if (!existingEntry && searchResults?.size()) {
                if (uniqueIdentifier) {
                    existingEntry = lookupByGloballyUniqueIdentifier(eventId, (LdapObjectDefinition) objectDef, (LdapCallbackContext) context, uniqueIdentifier)
                }
                if (existingEntry) {
                    foundObjectMethod = FoundObjectMethod.BY_MATCHED_KEY_DN_MISMATCH
                } else {
                    // If none of the entires match DN or unique identifier
                    // but there are more than one value in searchResults,
                    // that means there are multiple entries in the
                    // directory with the same pkey, none of them matching
                    // our DN or unique identifier.  Just pick the first one found that matches
                    // our DN acceptance criteria.
                    existingEntry = searchResults.find { DirContextAdapter entry ->
                        ((LdapObjectDefinition) objectDef).acceptAsExistingDn(entry.dn.toString())
                    }
                    if (existingEntry) {
                        foundObjectMethod = FoundObjectMethod.BY_FIRST_FOUND
                    }
                }
            }

            // DN may still exist but with a different unique identifier and
            // primary key
            if (!existingEntry) {
                try {
                    existingEntry = lookup(eventId, (LdapObjectDefinition) objectDef, (LdapCallbackContext) context, dn)
                }
                catch (NameNotFoundException ignored) {
                    // no-op
                }
                if (existingEntry) {
                    foundObjectMethod = FoundObjectMethod.BY_DN_MISMATCHED_KEYS
                }
            }

            if (((LdapObjectDefinition) objectDef).isRemoveDuplicatePrimaryKeys()) {
                // Delete all the entriepersistences that we're not keeping
                // as the existingEntry
                searchResults.each { DirContextAdapter entry ->
                    if (entry.dn != existingEntry?.dn) {
                        delete(eventId, (LdapObjectDefinition) objectDef, (LdapCallbackContext) context, pkey, entry.dn.toString())
                        isModified = true
                    }
                }
            }

            if (existingEntry) {
                // Already exists -- update

                String existingDn = existingEntry.dn

                // Check for need to move DNs
                if (existingDn != dn) {
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
                }

                Map<String, Object> newReplaceAttributeMap = new LinkedHashMap<String, Object>(attrMapCopy)
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
                        dn,
                        newReplaceAttributeMap,
                        newAppendOnlyAttributeMap
                )) {
                    isModified = true
                }
            } else {
                // Doesn't already exist -- create
                insert(eventId, (LdapObjectDefinition) objectDef, (LdapCallbackContext) context, pkey, dn, attrMapCopy)
                isModified = true
            }
        } else {
            // is a deletion for the pkey
            searchResults.each { DirContextAdapter entry ->
                delete(eventId, (LdapObjectDefinition) objectDef, (LdapCallbackContext) context, pkey, entry.dn.toString())
                isModified = true
            }
        }

        return isModified
    }

    @SuppressWarnings("GrMethodMayBeStatic")
    Name buildDnName(String dn) {
        return LdapNameBuilder.newInstance(dn).build()
    }

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

    private Map<String, Object> convertCallerProvidedMap(Map<String, Object> map) {
        return map.findAll { it.value != null && !(it.value instanceof List && !((List) it.value).size()) }.collectEntries {
            [it.key, (it.value instanceof List ? convertCallerProvidedList((List) it.value) : convertCallerProvidedValue(it.value))]
        }
    }

    @SuppressWarnings("GrMethodMayBeStatic")
    private Object convertCallerProvidedList(List list) {
        if (list.size() == 1) {
            return convertCallerProvidedValue(list.first())
        } else {
            return list.collect { convertCallerProvidedValue(it) }
        }
    }

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
}
