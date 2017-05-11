package edu.berkeley.bidms.connector.ldap

import edu.berkeley.bidms.connector.Connector
import edu.berkeley.bidms.connector.ObjectDefinition
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

    List<Map<String, Object>> search(LdapObjectDefinition objectDef, String uid) {
        return ldapTemplate.search(objectDef.getLdapQueryForPrimaryKey(uid), toMapContextMapper)
    }

    void delete(String dn) {
        ldapTemplate.unbind(buildDnName(dn))
    }

    void rename(String oldDn, String newDn) {
        ldapTemplate.rename(buildDnName(oldDn), buildDnName(newDn))
    }

    void update(String dn, Map<String, Object> attributeMap) {
        ldapTemplate.rebind(buildDnName(dn), null, buildAttributes(attributeMap))
    }

    void insert(String dn, Map<String, Object> attributeMap) {
        ldapTemplate.bind(buildDnName(dn), null, buildAttributes(attributeMap))
    }

    /**
     *
     * @param uid owner of the LDAP jsonObject to provision
     * @param jsonObject The objJson extracted from a LDAP DownstreamObject
     * @return true if the object was successfully persisted to LDAP.  false indicates an error.
     */
    @Override
    boolean persist(ObjectDefinition objectDef, Map<String, Object> jsonObject) {
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

            update(dn, jsonObject)
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
