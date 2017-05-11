package edu.berkeley.bidms.connector.ldap

import org.springframework.ldap.query.LdapQuery

import static org.springframework.ldap.query.LdapQueryBuilder.query

/**
 * An object definition for LDAP objects where the primary key is the uid
 * attribute.
 */
class UidObjectDefinition implements LdapObjectDefinition {
    /**
     * The objectClass to filter by when searching for uids.
     * The "person" objectClass would be a typical example.
     */
    String objectClass

    UidObjectDefinition(String objectClass) {
        this.objectClass = objectClass
    }

    @Override
    String getPrimaryKeyAttributeName() {
        return "uid"
    }

    @Override
    LdapQuery getLdapQueryForPrimaryKey(String uid) {
        return query()
                .where("objectClass").is(objectClass)
                .and("uid").is(uid)
    }

    @Override
    boolean acceptAsExistingDn(String dn) {
        // We disregard "entryuuid" entries because of a bug in OpenDJ that
        // creates these during replication.  Whenever an "entryuuid" entry
        // exists, it's a duplicate of another entry that has a proper DN. 
        // So these are never a "primary" entry and should always be
        // removed.
        return !dn.startsWith("entryuuid=")
    }
}
