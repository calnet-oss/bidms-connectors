package edu.berkeley.bidms.connector.ldap

import org.springframework.ldap.core.DirContextAdapter
import org.springframework.ldap.core.LdapTemplate
import org.springframework.ldap.core.support.LdapContextSource
import software.apacheds.embedded.EmbeddedLdapServer
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import javax.naming.Name

import static org.springframework.ldap.query.LdapQueryBuilder.query

class LdapConnectorSpec extends Specification {
    @Shared
    EmbeddedLdapServer embeddedLdapServer

    @Shared
    LdapContextSource ldapContextSource

    @Shared
    LdapTemplate ldapTemplate

    @Shared
    UidObjectDefinition uidObjectDef = new UidObjectDefinition("person")

    LdapConnector ldapConnector = Spy(LdapConnector)

    void setupSpec() {
        this.embeddedLdapServer = new EmbeddedLdapServer() {
            @Override
            String getBasePartitionName() {
                return "berkeley"
            }

            @Override
            String getBaseStructure() {
                return "dc=berkeley,dc=edu"
            }
        }
        embeddedLdapServer.init()

        this.ldapContextSource = new LdapContextSource()
        ldapContextSource.with {
            userDn = "uid=admin,ou=system"
            password = "secret"
            url = "ldap://localhost:10389"
        }
        ldapContextSource.afterPropertiesSet()
        this.ldapTemplate = new LdapTemplate(ldapContextSource)
        ldapTemplate.afterPropertiesSet()
    }

    void cleanupSpec() {
        embeddedLdapServer.destroy()
    }

    void setup() {
        ldapConnector.ldapTemplate = ldapTemplate
    }

    void addOu(String ou) {
        Name dnName = ldapConnector.buildDnName("ou=$ou,dc=berkeley,dc=edu")
        ldapTemplate.bind(dnName, null, ldapConnector.buildAttributes([
                ou         : ou,
                objectClass: ["top", "organizationalUnit"]
        ]))
    }

    void deleteOu(String ou) {
        ldapTemplate.unbind(ldapConnector.buildDnName("ou=$ou,dc=berkeley,dc=edu"))
    }

    void addTestEntry(String dn, String uid) {
        Name dnName = ldapConnector.buildDnName(dn)
        ldapTemplate.bind(dnName, null, ldapConnector.buildAttributes([
                uid        : uid,
                objectClass: ["top", "person", "inetOrgPerson"],
                sn         : "User",
                cn         : "Test User",
                description: "initial test"
        ]))
    }

    void deleteDn(String dn) {
        ldapTemplate.unbind(ldapConnector.buildDnName(dn))
    }

    List<Map<String, Object>> searchForUid(String uid) {
        return ldapTemplate.search(query()
                .where("objectClass").is("person")
                .and("uid").is(uid),
                ldapConnector.toMapContextMapper)
    }

    @Unroll("#description")
    void "test LdapConnector persistence"() {
        given:
        Name dnName = ldapConnector.buildDnName(dn)

        when:
        addOu("people")
        addOu("expired people")
        addOu("the middle")
        if (createFirst) {
            addTestEntry("uid=$uid,ou=people,dc=berkeley,dc=edu", uid)
            assert ((DirContextAdapter) ldapTemplate.lookup("uid=$uid,ou=people,dc=berkeley,dc=edu")).getStringAttribute("description") == "initial test"
        }
        if (createDupe) {
            addTestEntry("uid=$uid,ou=expired people,dc=berkeley,dc=edu", uid)
            assert ((DirContextAdapter) ldapTemplate.lookup("uid=$uid,ou=expired people,dc=berkeley,dc=edu")).getStringAttribute("description") == "initial test"
        }
        boolean result = ldapConnector.persist(uidObjectDef, [
                dn         : dn,
                uid        : uid,
                objectClass: ["top", "person", "inetOrgPerson"],
                sn         : "User",
                cn         : "Test User",
                description: "updated"
        ])

        List<Map<String, Object>> retrieved = searchForUid(uid)

        and: "cleanup"
        deleteDn(dn)
        deleteOu("people")
        deleteOu("expired people")
        deleteOu("the middle")

        then:
        result
        retrieved.size() == 1
        retrieved.first().dn == dn
        retrieved.first().description == "updated"
        deletes * ldapConnector.delete(_)
        renames * ldapConnector.rename(_, _)
        updates * ldapConnector.update(_, _)
        inserts * ldapConnector.insert(_, _)

        where:
        description                                                | createFirst | createDupe | uid | dn                                           | deletes | renames | updates | inserts
        "test creation"                                            | false       | false      | "1" | "uid=1,ou=people,dc=berkeley,dc=edu"         | 0       | 0       | 0       | 1
        "test update"                                              | true        | false      | "1" | "uid=1,ou=people,dc=berkeley,dc=edu"         | 0       | 0       | 1       | 0
        "test rename"                                              | true        | false      | "1" | "uid=1,ou=expired people,dc=berkeley,dc=edu" | 0       | 1       | 1       | 0
        "test update and remove nonmatching dupe"                  | true        | true       | "1" | "uid=1,ou=people,dc=berkeley,dc=edu"         | 1       | 0       | 1       | 0
        "test update with two dupes, rename one, delete the other" | true        | true       | "1" | "uid=1,ou=the middle,dc=berkeley,dc=edu"     | 1       | 1       | 1       | 0
    }
}
