package edu.berkeley.bidms.connector.ldap

import edu.berkeley.bidms.connector.ObjectDefinition
import org.springframework.ldap.query.LdapQuery

interface LdapObjectDefinition extends ObjectDefinition {
    String getPrimaryKeyAttributeName()
    LdapQuery getLdapQueryForPrimaryKey(String pkey)
    boolean acceptAsExistingDn(String dn)
}
