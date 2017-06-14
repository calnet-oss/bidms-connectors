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

import edu.berkeley.bidms.connector.ldap.event.*
import edu.berkeley.bidms.connector.ldap.event.message.LdapDeleteEventMessage
import edu.berkeley.bidms.connector.ldap.event.message.LdapInsertEventMessage
import edu.berkeley.bidms.connector.ldap.event.message.LdapRenameEventMessage
import edu.berkeley.bidms.connector.ldap.event.message.LdapUpdateEventMessage
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

    LdapInsertEventCallback insertEventCallback = Mock(LdapInsertEventCallback)
    LdapUpdateEventCallback updateEventCallback = Mock(LdapUpdateEventCallback)
    LdapRenameEventCallback renameEventCallback = Mock(LdapRenameEventCallback)
    LdapDeleteEventCallback deleteEventCallback = Mock(LdapDeleteEventCallback)

    LdapConnector ldapConnector = new LdapConnector(
            isSynchronousCallback: true,
            insertEventCallbacks: [insertEventCallback],
            updateEventCallbacks: [updateEventCallback],
            renameEventCallbacks: [renameEventCallback],
            deleteEventCallbacks: [deleteEventCallback]
    )

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

    /**
     * @return The entryUUID attribute on the newly created object
     */
    String addTestEntry(String dn, String uid, String cn = null) {
        Name dnName = ldapConnector.buildDnName(dn)
        ldapTemplate.bind(dnName, null, ldapConnector.buildAttributes([
                uid        : uid,
                objectClass: ["top", "person", "inetOrgPerson"],
                sn         : "User",
                cn         : cn ?: "Test User",
                description: "initial test"
        ]))
        Map<String, Object> found = ldapTemplate.lookup(dnName, ["entryUUID"] as String[], ldapConnector.toMapContextMapper)
        return found?.entryUUID
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
    void "test keepExistingAttributesWhenUpdating"() {
        given:
        UidObjectDefinition objDef = new UidObjectDefinition("person", keepExistingAttributesWhenUpdating, true, appendAttrs as String[])

        when:
        addOu("people")
        String dn = "uid=1,ou=people,dc=berkeley,dc=edu"
        String uid = "1"
        String eventId = "eventId"
        // create
        boolean didCreate = ldapConnector.persist(eventId, objDef, null, [
                dn         : dn,
                uid        : uid,
                objectClass: ["top", "person", "inetOrgPerson", "organizationalPerson"],
                sn         : "User",
                cn         : "Test User",
                description: "initial test",
                mail       : ["test@berkeley.edu"]
        ], false)
        // update - description is kept or removed based on the value of isKeepExistingAttributesWhenUpdating in objDef
        boolean didUpdate = ldapConnector.persist(eventId, objDef, null, [
                dn         : dn,
                uid        : uid,
                objectClass: ["top", "person", "inetOrgPerson", "organizationalPerson"],
                sn         : "User",
                cn         : "Test User",
                mail       : ["test2@berkeley.edu"]
        ] + (updateDescAttr || nullOutDescAttr ? ["description": (nullOutDescAttr ? null : updateDescAttr)] : [:]), false)
        List<Map<String, Object>> retrieved = searchForUid(uid)

        and: "cleanup"
        deleteDn(dn)
        deleteOu("people")

        then:
        didCreate
        didUpdate
        retrieved.size() == 1
        retrieved.first().description == expectedDescription
        retrieved.first().mail == expectedMail

        where:
        description                                                                               | keepExistingAttributesWhenUpdating | updateDescAttr | nullOutDescAttr | appendAttrs | expectedDescription | expectedMail
        "isKeepExistingAttributesWhenUpdating=true"                                                 | true                               | null           | false           | null        | "initial test"      | "test2@berkeley.edu"
        "isKeepExistingAttributesWhenUpdating=false"                                                | false                              | null           | false           | null        | null                | "test2@berkeley.edu"
        "isKeepExistingAttributesWhenUpdating=true, update existing description"                    | true                               | "updated"      | false           | null        | "updated"           | "test2@berkeley.edu"
        "isKeepExistingAttributesWhenUpdating=true, update existing description and append to mail" | true                               | "updated"      | false           | ["mail"]    | "updated"           | ["test@berkeley.edu", "test2@berkeley.edu"]
        "isKeepExistingAttributesWhenUpdating=true, remove existing description by explicit null"   | true                               | null           | true            | null        | null                | "test2@berkeley.edu"
    }

    @Unroll("#description")
    void "test LdapConnector persistence"() {
        given:
        UidObjectDefinition uidObjectDef = new UidObjectDefinition("person", true, removeDupes, null)
        String eventId = "eventId"

        List<String> objectClasses = ["top", "person", "inetOrgPerson", "organizationalPerson"]
        Map<String, Object> persistAttrMap = [
                dn         : dn,
                uid        : uid,
                objectClass: objectClasses,
                sn         : "User",
                cn         : "Test User",
                mail       : [],
                description: ["updated"]
        ]

        when:
        addOu("people")
        addOu("expired people")
        addOu("the middle")
        String firstEntryUUID = null
        if (createFirst) {
            firstEntryUUID = addTestEntry("uid=$uid,ou=people,dc=berkeley,dc=edu", uid)
            assert firstEntryUUID
            assert ((DirContextAdapter) ldapTemplate.lookup("uid=$uid,ou=people,dc=berkeley,dc=edu")).getStringAttribute("description") == "initial test"
        }
        if (createDupe) {
            addTestEntry("uid=$uid,ou=expired people,dc=berkeley,dc=edu", uid)
            assert ((DirContextAdapter) ldapTemplate.lookup("uid=$uid,ou=expired people,dc=berkeley,dc=edu")).getStringAttribute("description") == "initial test"
        }

        if (createFirst && srchFirstUUID) {
            persistAttrMap.entryUUID = firstEntryUUID
        }
        boolean isModified = ldapConnector.persist(eventId, uidObjectDef, null, persistAttrMap, doDelete)

        List<Map<String, Object>> retrieved = searchForUid(uid)
        Map<String, Object> foundDn = retrieved.find {
            it.dn == dn
        }

        and: "cleanup"
        if (!doDelete) {
            // if the doDelete flag is set, that means we already deleted it
            deleteDn(dn)
        }
        if (createDupe && !removeDupes) {
            deleteDn("uid=$uid,ou=expired people,dc=berkeley,dc=edu")
        }
        deleteOu("people")
        deleteOu("expired people")
        deleteOu("the middle")

        then:
        isModified
        retrieved.size() == (!doDelete ? (createDupe && !removeDupes ? 2 : 1) : 0)
        (!doDelete ? foundDn.dn : null) == (!doDelete ? dn : null)
        (!doDelete ? foundDn.description : null) == (!doDelete ? "updated" : null)
        deletes * deleteEventCallback.receive(_) >> { LdapDeleteEventMessage msg ->
            assert msg.isSuccess
            assert msg.eventId == eventId
            assert msg.objectDef == uidObjectDef
            assert msg.pkey in delPkey
            assert msg.dn in delDn
        }
        renames * renameEventCallback.receive(_) >> { LdapRenameEventMessage msg ->
            assert msg.isSuccess
            assert msg.eventId == eventId
            assert msg.objectDef == uidObjectDef
            assert msg.pkey == uid
            assert msg.oldDn in renameOldDn
            assert msg.newDn == dn
        }
        updates * updateEventCallback.receive(new LdapUpdateEventMessage(
                isSuccess: true,
                eventId: eventId,
                objectDef: uidObjectDef,
                foundMethod: foundMethod,
                pkey: uid,
                oldAttributes: [
                        uid        : uid,
                        description: "initial test",
                        sn         : "User",
                        cn         : "Test User",
                        objectClass: objectClasses
                ],
                dn: dn,
                newAttributes: [
                        uid        : uid,
                        objectClass: objectClasses,
                        sn         : "User",
                        cn         : "Test User",
                        description: "updated"
                ]
        )) >> { LdapUpdateEventMessage msg ->
            assert msg.modificationItems?.size()
        }
        inserts * insertEventCallback.receive(_) >> { LdapInsertEventMessage msg ->
            assert msg.isSuccess
            assert msg.eventId == eventId
            assert msg.objectDef == uidObjectDef
            assert msg.pkey == uid
            assert msg.dn == dn
            assert msg.newAttributes == [
                    uid        : uid,
                    objectClass: objectClasses,
                    sn         : "User",
                    cn         : "Test User",
                    description: "updated"
            ]
            assert msg.globallyUniqueIdentifier
        }

        where:
        description                                                                                          | createFirst | srchFirstUUID | createDupe | doDelete | removeDupes | uid | dn                                           | deletes | renames | updates | inserts | foundMethod                                  | delPkey | delDn                                                                                | renameOldDn
        "test creation"                                                                                      | false       | false         | false      | false    | true        | "1" | "uid=1,ou=people,dc=berkeley,dc=edu"         | 0       | 0       | 0       | 1       | null                                         | null    | null                                                                                 | null
        "test update, find by pkey"                                                                          | true        | false         | false      | false    | true        | "1" | "uid=1,ou=people,dc=berkeley,dc=edu"         | 0       | 0       | 1       | 0       | FoundObjectMethod.BY_DN_MATCHED_KEY          | null    | null                                                                                 | null
        "test update, find by entryUUID"                                                                     | true        | true          | false      | false    | true        | "1" | "uid=1,ou=people,dc=berkeley,dc=edu"         | 0       | 0       | 1       | 0       | FoundObjectMethod.BY_DN_MATCHED_KEY          | null    | null                                                                                 | null
        "test rename, find by pkey but mismatching dn"                                                       | true        | false         | false      | false    | true        | "1" | "uid=1,ou=expired people,dc=berkeley,dc=edu" | 0       | 1       | 1       | 0       | FoundObjectMethod.BY_MATCHED_KEY_DN_MISMATCH | null    | null                                                                                 | ["uid=1,ou=people,dc=berkeley,dc=edu"]
        "test rename, find by entryUUID but mismatching dn"                                                  | true        | true          | false      | false    | true        | "1" | "uid=1,ou=expired people,dc=berkeley,dc=edu" | 0       | 1       | 1       | 0       | FoundObjectMethod.BY_MATCHED_KEY_DN_MISMATCH | null    | null                                                                                 | ["uid=1,ou=people,dc=berkeley,dc=edu"]
        "test update by finding pkey and remove nonmatching dupe"                                            | true        | false         | true       | false    | true        | "1" | "uid=1,ou=people,dc=berkeley,dc=edu"         | 1       | 0       | 1       | 0       | FoundObjectMethod.BY_DN_MATCHED_KEY          | ["1"]   | ["uid=1,ou=expired people,dc=berkeley,dc=edu"]                                       | null
        "test update by finding entryUUID and remove nonmatching dupe"                                       | true        | true          | true       | false    | true        | "1" | "uid=1,ou=people,dc=berkeley,dc=edu"         | 1       | 0       | 1       | 0       | FoundObjectMethod.BY_DN_MATCHED_KEY          | ["1"]   | ["uid=1,ou=expired people,dc=berkeley,dc=edu"]                                       | null
        "test update by finding pkey and don't remove nonmatching dupe"                                      | true        | false         | true       | false    | false       | "1" | "uid=1,ou=people,dc=berkeley,dc=edu"         | 0       | 0       | 1       | 0       | FoundObjectMethod.BY_DN_MATCHED_KEY          | null    | null                                                                                 | null
        "test update by finding entryUUID and don't remove nonmatching dupe"                                 | true        | true          | true       | false    | false       | "1" | "uid=1,ou=people,dc=berkeley,dc=edu"         | 0       | 0       | 1       | 0       | FoundObjectMethod.BY_DN_MATCHED_KEY          | null    | null                                                                                 | null
        "test update with two dupes by finding by first-found, rename one, delete the other"                 | true        | false         | true       | false    | true        | "1" | "uid=1,ou=the middle,dc=berkeley,dc=edu"     | 1       | 1       | 1       | 0       | FoundObjectMethod.BY_FIRST_FOUND             | ["1"]   | ["uid=1,ou=people,dc=berkeley,dc=edu", "uid=1,ou=expired people,dc=berkeley,dc=edu"] | ["uid=1,ou=people,dc=berkeley,dc=edu", "uid=1,ou=expired people,dc=berkeley,dc=edu"]
        "test update with two dupes by finding by entryUUID but mismatched dn, rename one, delete the other" | true        | true          | true       | false    | true        | "1" | "uid=1,ou=the middle,dc=berkeley,dc=edu"     | 1       | 1       | 1       | 0       | FoundObjectMethod.BY_MATCHED_KEY_DN_MISMATCH | ["1"]   | ["uid=1,ou=expired people,dc=berkeley,dc=edu"]                                       | ["uid=1,ou=people,dc=berkeley,dc=edu"]
        "test delete"                                                                                        | true        | false         | false      | true     | true        | "1" | "uid=1,ou=people,dc=berkeley,dc=edu"         | 1       | 0       | 0       | 0       | null                                         | ["1"]   | ["uid=1,ou=people,dc=berkeley,dc=edu"]                                               | null
        "test multi-delete"                                                                                  | true        | false         | true       | true     | true        | "1" | "uid=1,ou=people,dc=berkeley,dc=edu"         | 2       | 0       | 0       | 0       | null                                         | ["1"]   | ["uid=1,ou=people,dc=berkeley,dc=edu", "uid=1,ou=expired people,dc=berkeley,dc=edu"] | null
    }

    void "test persist return value on a non-modification"() {
        given:
        UidObjectDefinition objDef = new UidObjectDefinition("person", true, true, null)

        when:
        addOu("people")
        String dn = "uid=1,ou=people,dc=berkeley,dc=edu"
        String uid = "1"
        String eventId = "eventId"
        Map<String, Object> map = [
                dn         : dn,
                uid        : uid,
                objectClass: ["top", "person", "inetOrgPerson", "organizationalPerson"],
                sn         : "User",
                cn         : "Test User",
                description: "initial test"
        ]
        // create
        boolean didCreate = ldapConnector.persist(eventId, objDef, null, map, false)
        // update with same data such that no modification should occur
        boolean didUpdate = ldapConnector.persist(eventId, objDef, null, map, false)
        List<Map<String, Object>> retrieved = searchForUid(uid)

        and: "cleanup"
        deleteDn(dn)
        deleteOu("people")

        then:
        didCreate
        // no actual modification should have happened
        !didUpdate
        retrieved.size() == 1
        retrieved.first().description == "initial test"
    }

    @Unroll("#description")
    void "test LdapConnector persistence when primary key is not in DN and primary key changes"() {
        given:
        UidObjectDefinition uidObjectDef = new UidObjectDefinition("person", true, true, null)

        addOu("namespace")

        // create initial entry for cn with the primary key of createUid
        String dn = "cn=$cn,ou=namespace,dc=berkeley,dc=edu"
        addTestEntry(dn, createUid, cn)
        assert ((DirContextAdapter) ldapTemplate.lookup(dn)).getStringAttribute("description") == "initial test"

        String eventId = "eventId"
        List<String> objectClasses = ["top", "person", "inetOrgPerson", "organizationalPerson"]
        // update the entry for cn, but change the primary key within the entry to updateUid
        Map<String, Object> attrMap = [
                dn         : dn,
                uid        : updateUid,
                objectClass: objectClasses,
                sn         : "User",
                cn         : cn,
                mail       : [],
                description: ["updated"]
        ]

        when:
        boolean isModified = ldapConnector.persist(eventId, uidObjectDef, null, attrMap, false)

        List<Map<String, Object>> retrieved = searchForUid(updateUid)
        Map<String, Object> foundDn = retrieved.find {
            it.dn == dn
        }

        and: "cleanup"
        deleteDn(dn)
        deleteOu("namespace")

        then:
        isModified
        retrieved.size() == 1
        foundDn.dn == dn
        foundDn.description == "updated"
        1 * updateEventCallback.receive(
                new LdapUpdateEventMessage(
                        isSuccess: true,
                        eventId: eventId,
                        objectDef: uidObjectDef,
                        foundMethod: FoundObjectMethod.BY_DN_MISMATCHED_KEYS,
                        pkey: updateUid,
                        oldAttributes: [
                                uid        : createUid,
                                description: "initial test",
                                sn         : "User",
                                cn         : cn,
                                objectClass: objectClasses
                        ],
                        dn: dn,
                        newAttributes: [
                                uid        : updateUid,
                                objectClass: objectClasses,
                                sn         : "User",
                                cn         : cn,
                                description: "updated"
                        ]
                )
        )

        where:
        description                           | cn         | createUid | updateUid
        "test update with primary key change" | "testName" | "1"       | "2"
    }

    void "test asynchronous callback"() {
        given:
        UidObjectDefinition objDef = new UidObjectDefinition("person", true, true, null)
        List<String> objectClasses = ["top", "person", "inetOrgPerson", "organizationalPerson"]
        String eventId = "eventId"
        String dn = "uid=1,ou=people,dc=berkeley,dc=edu"
        String uid = "1"

        LdapConnector ldapConnector = new LdapConnector(
                ldapTemplate: ldapTemplate,
                isSynchronousCallback: false,
                insertEventCallbacks: [insertEventCallback],
                updateEventCallbacks: [updateEventCallback],
                renameEventCallbacks: [renameEventCallback],
                deleteEventCallbacks: [deleteEventCallback]
        )

        LdapInsertEventMessage msg = null
        insertEventCallback.receive(_) >> { LdapInsertEventMessage _msg ->
            msg = _msg
        }

        when:
        ldapConnector.start()
        addOu("people")
        Boolean didCreate = null
        synchronized (ldapConnector.callbackMonitorThread) {
            didCreate = ldapConnector.persist(eventId, objDef, null, [
                    dn         : dn,
                    uid        : uid,
                    objectClass: objectClasses,
                    sn         : "User",
                    cn         : "Test User",
                    description: "initial test"
            ], false)
            // wait for notification that the asynchronous callback queue was emptied
            ldapConnector.callbackMonitorThread.wait(20000)
        }
        List<Map<String, Object>> retrieved = searchForUid(uid)

        and: "cleanup"
        deleteDn(dn)
        deleteOu("people")
        ldapConnector.stop()

        then:
        didCreate
        retrieved.size() == 1
        retrieved.first().description == expectedDescription
        msg.isSuccess
        msg.eventId == eventId
        msg.objectDef == objDef
        msg.pkey == uid
        msg.dn == dn
        msg.newAttributes == [
                uid        : uid,
                objectClass: objectClasses,
                sn         : "User",
                cn         : "Test User",
                description: expectedDescription
        ]
        msg.globallyUniqueIdentifier

        where:
        description | expectedDescription
        "test"      | "initial test"
    }
}
