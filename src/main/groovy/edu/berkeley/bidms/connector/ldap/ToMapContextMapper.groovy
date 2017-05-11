package edu.berkeley.bidms.connector.ldap

import org.springframework.ldap.core.ContextMapper
import org.springframework.ldap.core.DirContextAdapter

import javax.naming.NamingException
import javax.naming.directory.Attribute

/**
 * Converts the attributes of a search result to a Map
 */
class ToMapContextMapper implements ContextMapper<Map<String, Object>> {
    @Override
    Map<String, Object> mapFromContext(Object ctx) throws NamingException {
        DirContextAdapter searchResult
        if (ctx instanceof DirContextAdapter) {
            searchResult = (DirContextAdapter) ctx
        } else {
            throw new RuntimeException("Not supported for ctx type ${ctx?.getClass()?.name}.  Only DirContextAdapter objects are supported.")
        }

        Map<String, Object> result = [:]
        searchResult.attributes.all.each { Attribute attr ->
            if (attr.size() == 1) {
                result.put(attr.ID, attr.get())
            } else if (attr.size() > 1) {
                def list = []
                attr.all.each {
                    list.add(it)
                }
                result.put(attr.ID, list)
            }
        }

        // dn is added as an extra pseudo-attribute
        result.put("dn", searchResult.dn.toString())

        return result
    }
}
