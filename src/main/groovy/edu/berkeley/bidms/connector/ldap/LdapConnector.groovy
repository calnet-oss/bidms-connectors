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

    DirContextAdapter lookup(String eventId, String dn) {
        return (DirContextAdapter) ldapTemplate.lookup(buildDnName(dn))
    }

    void delete(String eventId, String pkey, String dn) throws LdapConnectorException {
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
                deleteEventCallbacks.each { it.failure(eventId, pkey, dn, exception) }
            } else {
                deleteEventCallbacks.each { it.success(eventId, pkey, dn) }
            }
        }
    }

    void rename(String eventId, String pkey, String oldDn, String newDn) throws LdapConnectorException {
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
                renameEventCallbacks.each { it.failure(eventId, pkey, oldDn, newDn, exception) }
            } else {
                renameEventCallbacks.each { it.success(eventId, pkey, oldDn, newDn) }
            }
        }
    }

    /**
     * Update an existing LDAP object with values in newAttributeMap.
     *
     * Attribute names in the newAttributeMap are case sensitive.  Attribute
     * strings must match the case of the attribute names of the directory
     * schema, as retrieved from the directory via a search() or lookup().
     */
    void update(String eventId, String pkey, DirContextAdapter existingEntry, String dn, Map<String, Object> newAttributeMap, boolean keepExistingAttributes) throws LdapConnectorException {
        Throwable exception
        Map<String, Object> oldAttributeMap = null
        try {
            oldAttributeMap = toMapContextMapper.mapFromContext(existingEntry)
            oldAttributeMap.remove("dn")
            Map<String, Object> attributesToKeepOrUpdate = (keepExistingAttributes ? new LinkedHashMap<String, Object>(oldAttributeMap) : newAttributeMap)
            if (keepExistingAttributes) {
                attributesToKeepOrUpdate.putAll(newAttributeMap)
            }
            Map<String, Object> changedAttributes = attributesToKeepOrUpdate - oldAttributeMap
            Collection<String> attributesToRemove = (!keepExistingAttributes ? oldAttributeMap.keySet() - attributesToKeepOrUpdate.keySet() : null)

            attributesToRemove?.each { String attrName ->
                existingEntry.setAttributeValues(attrName, null)
            }

            changedAttributes.each { Map.Entry<String, Object> entry ->
                if (entry.value instanceof Collection) {
                    existingEntry.setAttributeValues(entry.key, ((Collection) entry.value).toArray())
                } else {
                    existingEntry.setAttributeValues(entry.key, [entry.value] as Object[])
                }
            }

            ldapTemplate.modifyAttributes(existingEntry)
        }
        catch (Throwable t) {
            exception = t
            throw new LdapConnectorException(t)
        }
        finally {
            if (exception) {
                updateEventCallbacks.each { it.failure(eventId, pkey, oldAttributeMap, dn, newAttributeMap, exception) }
            } else {
                updateEventCallbacks.each { it.success(eventId, pkey, oldAttributeMap, dn, newAttributeMap) }
            }
        }
    }

    void insert(String eventId, String pkey, String dn, Map<String, Object> attributeMap) throws LdapConnectorException {
        Throwable exception
        try {
            ldapTemplate.bind(buildDnName(dn), null, buildAttributes(attributeMap))
        }
        catch (Throwable t) {
            exception = t
            throw new LdapConnectorException(t)
        }
        finally {
            if (exception) {
                insertEventCallbacks.each { it.failure(eventId, pkey, dn, attributeMap, exception) }
            } else {
                insertEventCallbacks.each { it.success(eventId, pkey, dn, attributeMap) }
            }
        }
    }

    /**
     * Persist the values in jsonObject to LDAP.
     *
     * Attribute names in the newAttributeMap are case sensitive.  Attribute
     * strings must match the case of the attribute names in the directory
     * schema, as when retrieved from the directory via a search() or
     * lookup().
     *
     * @param objectDef Object definition of the LDAP object
     * @param jsonObject The objJson extracted from a LDAP DownstreamObject. 
     *        Keys are case sensitive and must match the case of the
     *        attribute names in the directory schema.
     * @return true if the object was successfully persisted to LDAP.  false
     *         indicates an error.
     */
    @Override
    void persist(String eventId, ObjectDefinition objectDef, Map<String, Object> jsonObject) throws LdapConnectorException {
        String pkeyAttrName = ((LdapObjectDefinition) objectDef).primaryKeyAttributeName
        String pkey = jsonObject[pkeyAttrName]
        if (!pkey) {
            throw new LdapConnectorException("LDAP object is missing a required value for primary key $pkeyAttrName")
        }
        String dn = jsonObject.dn
        if (!dn) {
            throw new LdapConnectorException("LDAP object for $pkeyAttrName $pkey does not contain the required dn attribute")
        }
        // Remove the dn from the object -- not an actual attribute
        jsonObject.remove("dn")

        // Spring method naming is a little confusing.  Spring uses the word
        // "bind" and "rebind" to mean "create" and "update." In this
        // context, it does not mean "authenticate (bind) to the LDAP
        // server."

        // See if the record already exists
        List<DirContextAdapter> searchResults = search(eventId, (LdapObjectDefinition) objectDef, pkey)

        //
        // There's only supposed to be one entry-per-pkey in the directory,
        // but this is not guaranteed to always be the case.  When the
        // search returns more than one entry, first see if there's any one
        // that already matches the DN of our downstream object.  If none
        // match, pick one to rename and delete the rest.
        //

        DirContextAdapter existingEntry = searchResults?.find { DirContextAdapter entry ->
            entry.dn.toString() == dn
        }
        if (!existingEntry && searchResults.size() > 0) {
            // None match the DN, so use the first one that matches a filter
            // criteria
            existingEntry = searchResults.find { DirContextAdapter entry ->
                ((LdapObjectDefinition) objectDef).acceptAsExistingDn(entry.dn.toString())
            }
        }

        // Delete all the entries that we're not keeping as the existingEntry
        searchResults.each { DirContextAdapter entry ->
            if (entry.dn != existingEntry?.dn) {
                delete(eventId, pkey, entry.dn.toString())
            }
        }

        if (existingEntry) {
            // Already exists -- update

            String existingDn = existingEntry.dn

            // Check for need to move DNs
            if (existingDn != dn) {
                // Move DN
                rename(eventId, pkey, existingDn, dn)
                existingEntry = lookup(eventId, dn)
                if (!existingEntry) {
                    throw new LdapConnectorException("Unable to lookup $dn right after an existing object was renamed to this DN")
                }
            }

            if (!existingEntry.updateMode) {
                existingEntry.updateMode = true
            }
            update(eventId, pkey, existingEntry, dn, jsonObject, ((LdapObjectDefinition) objectDef).keepExistingAttributesWhenUpdating())
        } else {
            // Doesn't already exist -- create
            insert(eventId, pkey, dn, jsonObject)
        }
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
                BasicAttribute attr = new BasicAttribute(entry.key)
                entry.value.each {
                    attr.add(it)
                }
                attrs.put(attr)
            } else if (entry.value instanceof String || entry.value instanceof Number || entry.value instanceof Boolean) {
                // Directory servers interpret numbers and booleans as
                // strings, so we use toString()
                attrs.put(entry.key, entry.value?.toString())
            } else if (entry.value == null) {
                attrs.remove(entry.key)
            } else {
                throw new RuntimeException("Type ${entry.value.getClass().name} for key ${entry.key} is not a recognized list, string, number, boolean or null type")
            }
        }
        return attrs
    }
}
