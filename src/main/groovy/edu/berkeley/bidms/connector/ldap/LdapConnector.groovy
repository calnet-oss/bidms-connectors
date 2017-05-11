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
import org.springframework.ldap.core.LdapTemplate
import org.springframework.ldap.support.LdapNameBuilder

import javax.naming.Name
import javax.naming.directory.Attributes
import javax.naming.directory.BasicAttribute
import javax.naming.directory.BasicAttributes

@Slf4j
class LdapConnector implements Connector {

    LdapTemplate ldapTemplate
    ContextMapper<Map<String, Object>> toMapContextMapper = new ToMapContextMapper()

    List<LdapDeleteEventCallback> deleteEventCallbacks = []
    List<LdapRenameEventCallback> renameEventCallbacks = []
    List<LdapUpdateEventCallback> updateEventCallbacks = []
    List<LdapInsertEventCallback> insertEventCallbacks = []

    List<Map<String, Object>> search(LdapObjectDefinition objectDef, String pkey) {
        return ldapTemplate.search(objectDef.getLdapQueryForPrimaryKey(pkey), toMapContextMapper)
    }

    void delete(String dn) throws LdapConnectorException {
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
                deleteEventCallbacks.each { it.failure(dn, exception) }
            } else {
                deleteEventCallbacks.each { it.success(dn) }
            }
        }
    }

    void rename(String oldDn, String newDn) throws LdapConnectorException {
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
                renameEventCallbacks.each { it.failure(oldDn, newDn, exception) }
            } else {
                renameEventCallbacks.each { it.success(oldDn, newDn) }
            }
        }
    }

    void update(String dn, Map<String, Object> oldAttributeMap, Map<String, Object> newAttributeMap) throws LdapConnectorException {
        Throwable exception
        try {
            ldapTemplate.rebind(buildDnName(dn), null, buildAttributes(newAttributeMap))
        }
        catch (Throwable t) {
            exception = t
            throw new LdapConnectorException(t)
        }
        finally {
            if (exception) {
                updateEventCallbacks.each { it.failure(dn, oldAttributeMap, newAttributeMap, exception) }
            } else {
                updateEventCallbacks.each { it.success(dn, oldAttributeMap, newAttributeMap) }
            }
        }
    }

    void insert(String dn, Map<String, Object> attributeMap) throws LdapConnectorException {
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
                insertEventCallbacks.each { it.failure(dn, attributeMap, exception) }
            } else {
                insertEventCallbacks.each { it.success(dn, attributeMap) }
            }
        }
    }

    /**
     * @param objectDef Object definition of the LDAP object
     * @param jsonObject The objJson extracted from a LDAP DownstreamObject
     * @return true if the object was successfully persisted to LDAP.  false indicates an error.
     */
    @Override
    boolean persist(ObjectDefinition objectDef, Map<String, Object> jsonObject) throws LdapConnectorException {
        String pkeyAttrName = ((LdapObjectDefinition) objectDef).primaryKeyAttributeName
        String pkey = jsonObject[pkeyAttrName]
        String dn = jsonObject.dn
        if (!dn) {
            log.warn("Downstream LDAP object for $pkeyAttrName $pkey does not contain the required dn attribute")
            return false
        }
        // Remove the dn from the object -- not an actual attribute
        jsonObject.remove("dn")

        log.debug("Persisting $pkeyAttrName $pkey, dn $dn")

        // Spring method naming is a little confusing.
        // Spring uses the word "bind" and "rebind" to mean "create" and "update."
        // In this context, it does not mean "authenticate (bind) to the LDAP server."

        // See if the record already exists
        List<Map<String, Object>> searchResults = search((LdapObjectDefinition) objectDef, pkey)

        //
        // There's only supposed to be one entry-per-pkey in the directory, but this is not guaranteed to always be the case.
        // When the search returns more than one entry, first see if there's any one that already matches the DN of our downstream object.
        // If none match, pick one to rename and delete the rest.
        //

        Map<String, Object> existingEntry = searchResults?.find { Map<String, Object> entry ->
            entry.dn == dn
        }
        if (!existingEntry && searchResults.size() > 0) {
            // None match the DN, so use the first one that matches a filter criteria
            existingEntry = searchResults.find { Map<String, Object> entry ->
                ((LdapObjectDefinition) objectDef).acceptAsExistingDn(entry.dn.toString())
            }
        }

        // Delete all the entries that we're not keeping as the existingEntry
        searchResults.each { Map<String, Object> entry ->
            if (entry.dn != existingEntry?.dn) {
                delete(entry.dn.toString())
            }
        }

        if (existingEntry) {
            // Already exists -- update

            // Check for need to move DNs
            if (existingEntry.dn != dn) {
                // Move DN
                rename(existingEntry.dn.toString(), dn)
            }

            update(dn, existingEntry, jsonObject)
        } else {
            // Doesn't already exist -- create
            insert(dn, jsonObject)
        }

        return true
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
                attrs.put(entry.key, entry.value)
            } else {
                throw new RuntimeException("Type ${entry.value.getClass().name} for key ${entry.key} is not a recognized list, string, number or boolean type")
            }
        }
        return attrs
    }
}
