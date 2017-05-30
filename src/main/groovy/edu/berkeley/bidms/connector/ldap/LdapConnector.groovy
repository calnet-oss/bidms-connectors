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

import edu.berkeley.bidms.connector.Connector
import edu.berkeley.bidms.connector.ObjectDefinition
import edu.berkeley.bidms.connector.ldap.event.LdapDeleteEventCallback
import edu.berkeley.bidms.connector.ldap.event.LdapInsertEventCallback
import edu.berkeley.bidms.connector.ldap.event.LdapRenameEventCallback
import edu.berkeley.bidms.connector.ldap.event.LdapUpdateEventCallback
import groovy.util.logging.Slf4j
import org.springframework.ldap.core.ContextMapper
import org.springframework.ldap.core.DirContextAdapter
import org.springframework.ldap.core.LdapTemplate
import org.springframework.ldap.support.LdapNameBuilder

import javax.naming.Name
import javax.naming.directory.Attribute
import javax.naming.directory.Attributes
import javax.naming.directory.BasicAttribute
import javax.naming.directory.BasicAttributes

@Slf4j
class LdapConnector implements Connector {

    LdapTemplate ldapTemplate
    ContextMapper<DirContextAdapter> toDirContextAdapterContextMapper = new ToDirContextAdapterContextMapper()
    ContextMapper<Map<String, Object>> toMapContextMapper = new ToMapContextMapper()

    List<LdapDeleteEventCallback> deleteEventCallbacks = []
    List<LdapRenameEventCallback> renameEventCallbacks = []
    List<LdapUpdateEventCallback> updateEventCallbacks = []
    List<LdapInsertEventCallback> insertEventCallbacks = []

    List<DirContextAdapter> search(String eventId, LdapObjectDefinition objectDef, String pkey) {
        return ldapTemplate.search(objectDef.getLdapQueryForPrimaryKey(pkey), toDirContextAdapterContextMapper)
    }

    DirContextAdapter lookup(String eventId, LdapObjectDefinition objectDef, String dn) {
        return (DirContextAdapter) ldapTemplate.lookup(buildDnName(dn))
    }

    void delete(String eventId, LdapObjectDefinition objectDef, String pkey, String dn) throws LdapConnectorException {
        Throwable exception
        try {
            ldapTemplate.unbind(buildDnName(dn))
        }
        catch (Throwable t) {
            exception = t
            throw new LdapConnectorException(t)
        }
        finally {
            if (exception) {
                deleteEventCallbacks.each { it.failure(eventId, objectDef, pkey, dn, exception) }
            } else {
                deleteEventCallbacks.each { it.success(eventId, objectDef, pkey, dn) }
            }
        }
    }

    void rename(String eventId, LdapObjectDefinition objectDef, String pkey, String oldDn, String newDn) throws LdapConnectorException {
        Throwable exception
        try {
            ldapTemplate.rename(buildDnName(oldDn), buildDnName(newDn))
        }
        catch (Throwable t) {
            exception = t
            throw new LdapConnectorException(t)
        }
        finally {
            if (exception) {
                renameEventCallbacks.each { it.failure(eventId, objectDef, pkey, oldDn, newDn, exception) }
            } else {
                renameEventCallbacks.each { it.success(eventId, objectDef, pkey, oldDn, newDn) }
            }
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
            String pkey,
            DirContextAdapter existingEntry,
            String dn,
            Map<String, Object> newReplaceAttributeMap,
            Map<String, List<Object>> newAppendOnlyAttributeMap
    ) throws LdapConnectorException {
        Throwable exception
        Map<String, Object> oldAttributeMap = null
        Map<String, Object> convertedNewAttributeMap = null
        boolean isModified = false
        try {
            oldAttributeMap = toMapContextMapper.mapFromContext(existingEntry)
            oldAttributeMap.remove("dn")

            convertedNewAttributeMap = convertCallerProvidedMap(newReplaceAttributeMap)
            Map<String, Object> attributesToKeepOrUpdate
            if (objectDef.keepExistingAttributesWhenUpdating()) {
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

            //log.debug("attributesToKeepOrUpdate = ${attributesToKeepOrUpdate}")
            Map<String, Object> changedAttributes = attributesToKeepOrUpdate - oldAttributeMap

            // Removing the attribute if keepExistingAttributes is false and
            // the attribute is not in the newAttributeMap or if the
            // attribute is explicitly set to null in the newAttributeMap.
            HashSet<String> attributeNamesToRemove = (
                    (!objectDef.keepExistingAttributesWhenUpdating() ? oldAttributeMap.keySet() - attributesToKeepOrUpdate.keySet() : []) as HashSet<String>
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
                //log.debug("${entry.key} OLD=${oldAttributeMap[entry.key]} NEW=${entry.value} (${entry.value?.getClass()?.name})")
                if (entry.value instanceof Collection) {
                    existingEntry.setAttributeValues(entry.key, ((Collection) entry.value).toArray())
                } else if (entry.value) {
                    existingEntry.setAttributeValues(entry.key, [entry.value] as Object[])
                }
            }

            /*
            log.debug("attributeNamesToRemove: ${attributeNamesToRemove}")
            log.debug("modifications: ${existingEntry.modificationItems}")
            log.debug("changesAttributes: ${changedAttributes}")
            */

            isModified = existingEntry.modificationItems.size()
            ldapTemplate.modifyAttributes(existingEntry)
            return isModified
        }
        catch (Throwable t) {
            exception = t
            throw new LdapConnectorException(t)
        }
        finally {
            if (exception) {
                updateEventCallbacks.each { it.failure(eventId, objectDef, pkey, oldAttributeMap, dn, convertedNewAttributeMap ?: newReplaceAttributeMap, exception) }
            } else {
                updateEventCallbacks.each { it.success(eventId, objectDef, pkey, oldAttributeMap, dn, convertedNewAttributeMap ?: newReplaceAttributeMap, isModified) }
            }
        }
    }

    void insert(String eventId, LdapObjectDefinition objectDef, String pkey, String dn, Map<String, Object> attributeMap) throws LdapConnectorException {
        Throwable exception
        Map<String, Object> convertedNewAttributeMap = null
        try {
            convertedNewAttributeMap = convertCallerProvidedMap(attributeMap)
            ldapTemplate.bind(buildDnName(dn), null, buildAttributes(convertedNewAttributeMap))
        }
        catch (Throwable t) {
            exception = t
            throw new LdapConnectorException(t)
        }
        finally {
            if (exception) {
                insertEventCallbacks.each { it.failure(eventId, objectDef, pkey, dn, convertedNewAttributeMap ?: attributeMap, exception) }
            } else {
                insertEventCallbacks.each { it.success(eventId, objectDef, pkey, dn, convertedNewAttributeMap ?: attributeMap) }
            }
        }
    }

    /**
     * Persist the values in attrMap to LDAP.
     *
     * Attribute names in the attrMap are case sensitive.  Attribute
     * strings must match the case of the attribute names in the directory
     * schema, as when retrieved from the directory via a search() or
     * lookup().
     *
     * @param objectDef Object definition of the LDAP object
     * @param attrMap The map of the attributes for the LDAP object.
     *        Keys are case sensitive and must match the case of the
     *        attribute names in the directory schema.
     * @return true if a modification occurred
     */
    @Override
    boolean persist(String eventId, ObjectDefinition objectDef, Map<String, Object> attrMap, boolean isDelete) throws LdapConnectorException {
        LinkedHashMap<String, Object> attrMapCopy = new LinkedHashMap<String, Object>(attrMap)
        String pkeyAttrName = ((LdapObjectDefinition) objectDef).primaryKeyAttributeName
        String pkey = attrMapCopy[pkeyAttrName]
        if (!pkey) {
            throw new LdapConnectorException("LDAP object is missing a required value for primary key $pkeyAttrName")
        }

        // Spring method naming is a little confusing.  Spring uses the word
        // "bind" and "rebind" to mean "create" and "update." In this
        // context, it does not mean "authenticate (bind) to the LDAP
        // server."

        // See if records belonging to the pkey exist
        List<DirContextAdapter> searchResults = search(eventId, (LdapObjectDefinition) objectDef, pkey)

        boolean isModified = false

        if (!isDelete) {
            String dn = attrMapCopy.dn
            if (!dn) {
                throw new LdapConnectorException("LDAP object for $pkeyAttrName $pkey does not contain the required dn attribute")
            }
            // Remove the dn from the object -- not an actual attribute
            attrMapCopy.remove("dn")

            //
            // There's only supposed to be one entry-per-pkey in the
            // directory, but this is not guaranteed to always be the case. 
            // When the search returns more than one entry, first see if
            // there's any one that already matches the DN of our downstream
            // object.  If none match and removeDuplicatePrimaryKeys()
            // returns true, pick one to rename and delete the rest.
            //

            DirContextAdapter existingEntry = searchResults?.find { DirContextAdapter entry ->
                entry.dn.toString() == dn
            }
            if (!existingEntry && searchResults.size() > 0) {
                // None match the DN, so use the first one that matches a
                // filter criteria
                existingEntry = searchResults.find { DirContextAdapter entry ->
                    ((LdapObjectDefinition) objectDef).acceptAsExistingDn(entry.dn.toString())
                }
            }

            if (((LdapObjectDefinition) objectDef).removeDuplicatePrimaryKeys()) {
                // Delete all the entries that we're not keeping as the
                // existingEntry
                searchResults.each { DirContextAdapter entry ->
                    if (entry.dn != existingEntry?.dn) {
                        delete(eventId, (LdapObjectDefinition) objectDef, pkey, entry.dn.toString())
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
                    rename(eventId, (LdapObjectDefinition) objectDef, pkey, existingDn, dn)
                    existingEntry = lookup(eventId, (LdapObjectDefinition) objectDef, dn)
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
                insert(eventId, (LdapObjectDefinition) objectDef, pkey, dn, attrMapCopy)
                isModified = true
            }
        } else {
            // is a deletion for the pkey
            searchResults.each { DirContextAdapter entry ->
                delete(eventId, (LdapObjectDefinition) objectDef, pkey, entry.dn.toString())
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
            // Directory servers interpret numbers and booleans as
            // strings, so we use toString()
            return value.toString()
        } else if (value == null) {
            return null
        } else {
            throw new RuntimeException("Type ${value.getClass().name} is not a recognized list, string, number, boolean or null type")
        }
    }
}
