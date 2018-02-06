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

import edu.berkeley.bidms.connector.ConnectorObjectNotFoundException
import edu.berkeley.bidms.connector.ldap.event.LdapCallbackContext
import edu.berkeley.bidms.connector.ldap.event.LdapDeleteEventCallback
import edu.berkeley.bidms.connector.ldap.event.LdapEventType
import edu.berkeley.bidms.connector.ldap.event.LdapInsertEventCallback
import edu.berkeley.bidms.connector.ldap.event.LdapRemoveAttributesEventCallback
import edu.berkeley.bidms.connector.ldap.event.LdapRenameEventCallback
import edu.berkeley.bidms.connector.ldap.event.LdapSetAttributeEventCallback
import edu.berkeley.bidms.connector.ldap.event.LdapUniqueIdentifierEventCallback
import edu.berkeley.bidms.connector.ldap.event.LdapUpdateEventCallback
import edu.berkeley.bidms.connector.ldap.event.message.LdapDeleteEventMessage
import edu.berkeley.bidms.connector.ldap.event.message.LdapInsertEventMessage
import edu.berkeley.bidms.connector.ldap.event.message.LdapRenameEventMessage
import edu.berkeley.bidms.connector.ldap.event.message.LdapUniqueIdentifierEventMessage
import edu.berkeley.bidms.connector.ldap.event.message.LdapUpdateEventMessage
import org.springframework.ldap.NameNotFoundException
import org.springframework.ldap.core.DirContextAdapter
import org.springframework.ldap.core.LdapTemplate
import org.springframework.ldap.core.support.LdapContextSource
import org.springframework.ldap.query.LdapQuery
import org.springframework.ldap.transaction.compensating.manager.TransactionAwareContextSourceProxy
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
    LdapUniqueIdentifierEventCallback uniqueIdentifierEventCallback = Mock(LdapUniqueIdentifierEventCallback)
    LdapRemoveAttributesEventCallback removeAttributesEventCallback = Mock(LdapRemoveAttributesEventCallback)
    LdapSetAttributeEventCallback setAttributeEventCallback = Mock(LdapSetAttributeEventCallback)

    LdapConnector ldapConnector = new LdapConnector(
            isSynchronousCallback: true,
            insertEventCallbacks: [insertEventCallback],
            updateEventCallbacks: [updateEventCallback],
            renameEventCallbacks: [renameEventCallback],
            deleteEventCallbacks: [deleteEventCallback],
            uniqueIdentifierEventCallbacks: [uniqueIdentifierEventCallback],
            removeAttributesEventCallbacks: [removeAttributesEventCallback],
            setAttributeEventCallbacks: [setAttributeEventCallback]
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
        this.ldapTemplate = new LdapTemplate(new TransactionAwareContextSourceProxy(ldapContextSource))
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

    Map<String, Object> lookupDn(String dn) {
        try {
            return ldapTemplate.lookup(dn, ldapConnector.toMapContextMapper)
        }
        catch (NameNotFoundException ignored) {
            return null
        }
    }

    @Unroll("#description")
    void "test keepExistingAttributesWhenUpdating"() {
        given:
        UidObjectDefinition objDef = new UidObjectDefinition(
                objectClass: "person",
                keepExistingAttributesWhenUpdating: keepExistingAttributesWhenUpdating,
                removeDuplicatePrimaryKeys: true,
                dynamicAttributeNames: dynamicAttrs as String[]
        )

        when:
        addOu("people")
        String dn = "uid=1,ou=people,dc=berkeley,dc=edu"
        String uid = "1"
        String eventId = "eventId"
        // create
        boolean didCreate = ldapConnector.persist(eventId, objDef, null, [
                dn                                                              : dn,
                uid                                                             : uid,
                objectClass                                                     : ["top", "person", "inetOrgPerson", "organizationalPerson"],
                sn                                                              : "User",
                cn                                                              : "Test User",
                description                                                     : "initial test",
                (dynamicAttrs?.contains("mail.APPEND") ? "mail.APPEND" : "mail"): ["test@berkeley.edu"]
        ], false)
        // update - description is kept or removed based on the value of isKeepExistingAttributesWhenUpdating in objDef
        boolean didUpdate = ldapConnector.persist(eventId, objDef, null, [
                dn                                                              : dn,
                uid                                                             : uid,
                objectClass                                                     : ["top", "person", "inetOrgPerson", "organizationalPerson"],
                sn                                                              : "User",
                cn                                                              : "Test User",
                (dynamicAttrs?.contains("mail.APPEND") ? "mail.APPEND" : "mail"): mail2Override ?: ["test2@berkeley.edu"]
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
        description                                                                                                             | keepExistingAttributesWhenUpdating | updateDescAttr | nullOutDescAttr | dynamicAttrs    | mail2Override          || expectedDescription | expectedMail
        "isKeepExistingAttributesWhenUpdating=true"                                                                             | true                               | null           | false           | null            | null                   || "initial test"      | "test2@berkeley.edu"
        "isKeepExistingAttributesWhenUpdating=false"                                                                            | false                              | null           | false           | null            | null                   || null                | "test2@berkeley.edu"
        "isKeepExistingAttributesWhenUpdating=true, update existing description"                                                | true                               | "updated"      | false           | null            | null                   || "updated"           | "test2@berkeley.edu"
        "isKeepExistingAttributesWhenUpdating=true, update existing description and append to mail"                             | true                               | "updated"      | false           | ["mail.APPEND"] | null                   || "updated"           | ["test@berkeley.edu", "test2@berkeley.edu"]
        "isKeepExistingAttributesWhenUpdating=true, update existing description and append to mail with different case as orig" | true                               | "updated"      | false           | ["mail.APPEND"] | ["test@BERKELEY.EDU"]  || "updated"           | "test@berkeley.edu"
        "isKeepExistingAttributesWhenUpdating=true, update existing description and append to mail with leading space"          | true                               | "updated"      | false           | ["mail.APPEND"] | [" test@berkeley.edu"] || "updated"           | "test@berkeley.edu"
        "isKeepExistingAttributesWhenUpdating=true, update existing description and append to mail with trailing space"         | true                               | "updated"      | false           | ["mail.APPEND"] | ["test@berkeley.edu "] || "updated"           | "test@berkeley.edu"
        "isKeepExistingAttributesWhenUpdating=true, update existing description and append to mail using string not list"       | true                               | "updated"      | false           | ["mail.APPEND"] | "test@berkeley.edu"    || "updated"           | "test@berkeley.edu"
        "isKeepExistingAttributesWhenUpdating=true, remove existing description by explicit null"                               | true                               | null           | true            | null            | null                   || null                | "test2@berkeley.edu"
    }

    @Unroll("#description")
    void "test LdapConnector persistence"() {
        given:
        UidObjectDefinition uidObjectDef = new UidObjectDefinition(
                objectClass: "person",
                keepExistingAttributesWhenUpdating: true,
                removeDuplicatePrimaryKeys: removeDupes
        )
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
            assert msg.success
            assert msg.eventId == eventId
            assert msg.objectDef == uidObjectDef
            assert msg.pkey in delPkey
            assert msg.dn in delDn
        }
        renames * renameEventCallback.receive(_) >> { LdapRenameEventMessage msg ->
            assert msg.success
            assert msg.eventId == eventId
            assert msg.objectDef == uidObjectDef
            assert msg.pkey == uid
            assert msg.oldDn in renameOldDn
            assert msg.newDn == dn
        }
        updates * updateEventCallback.receive(new LdapUpdateEventMessage(
                success: true,
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
        inserts * insertEventCallback.receive(new LdapInsertEventMessage(
                success: true,
                eventId: eventId,
                objectDef: uidObjectDef,
                pkey: uid,
                dn: dn,
                newAttributes: [
                        uid        : uid,
                        objectClass: objectClasses,
                        sn         : "User",
                        cn         : "Test User",
                        description: "updated"
                ]
        ))
        uniqIdCBs * uniqueIdentifierEventCallback.receive(_) >> { LdapUniqueIdentifierEventMessage msg ->
            assert msg.success
            assert msg.causingEvent
            assert msg.eventId == eventId
            assert msg.objectDef == uidObjectDef
            assert msg.pkey == uid
            if (renameOldDn) {
                assert msg.oldDn in renameOldDn
            }
            assert msg.newDn == dn
            assert msg.globallyUniqueIdentifier
            assert msg.wasRenamed == (renameOldDn != null)
        }

        where:
        description                                                                                          | createFirst | srchFirstUUID | createDupe | doDelete | removeDupes | uid | dn                                           | deletes | renames | updates | inserts | uniqIdCBs | foundMethod                                  | delPkey | delDn                                                                                | renameOldDn
        "test creation"                                                                                      | false       | false         | false      | false    | true        | "1" | "uid=1,ou=people,dc=berkeley,dc=edu"         | 0       | 0       | 0       | 1       | 1         | null                                         | null    | null                                                                                 | null
        "test update, find by pkey"                                                                          | true        | false         | false      | false    | true        | "1" | "uid=1,ou=people,dc=berkeley,dc=edu"         | 0       | 0       | 1       | 0       | 1         | FoundObjectMethod.BY_DN_MATCHED_KEY          | null    | null                                                                                 | null
        "test update, find by entryUUID"                                                                     | true        | true          | false      | false    | true        | "1" | "uid=1,ou=people,dc=berkeley,dc=edu"         | 0       | 0       | 1       | 0       | 0         | FoundObjectMethod.BY_DN_MATCHED_KEY          | null    | null                                                                                 | null
        "test rename, find by pkey but mismatching dn"                                                       | true        | false         | false      | false    | true        | "1" | "uid=1,ou=expired people,dc=berkeley,dc=edu" | 0       | 1       | 1       | 0       | 1         | FoundObjectMethod.BY_MATCHED_KEY_DN_MISMATCH | null    | null                                                                                 | ["uid=1,ou=people,dc=berkeley,dc=edu"]
        "test rename, find by entryUUID but mismatching dn"                                                  | true        | true          | false      | false    | true        | "1" | "uid=1,ou=expired people,dc=berkeley,dc=edu" | 0       | 1       | 1       | 0       | 1         | FoundObjectMethod.BY_MATCHED_KEY_DN_MISMATCH | null    | null                                                                                 | ["uid=1,ou=people,dc=berkeley,dc=edu"]
        "test update by finding pkey and remove nonmatching dupe"                                            | true        | false         | true       | false    | true        | "1" | "uid=1,ou=people,dc=berkeley,dc=edu"         | 1       | 0       | 1       | 0       | 1         | FoundObjectMethod.BY_DN_MATCHED_KEY          | ["1"]   | ["uid=1,ou=expired people,dc=berkeley,dc=edu"]                                       | null
        "test update by finding entryUUID and remove nonmatching dupe"                                       | true        | true          | true       | false    | true        | "1" | "uid=1,ou=people,dc=berkeley,dc=edu"         | 1       | 0       | 1       | 0       | 0         | FoundObjectMethod.BY_DN_MATCHED_KEY          | ["1"]   | ["uid=1,ou=expired people,dc=berkeley,dc=edu"]                                       | null
        "test update by finding pkey and don't remove nonmatching dupe"                                      | true        | false         | true       | false    | false       | "1" | "uid=1,ou=people,dc=berkeley,dc=edu"         | 0       | 0       | 1       | 0       | 1         | FoundObjectMethod.BY_DN_MATCHED_KEY          | null    | null                                                                                 | null
        "test update by finding entryUUID and don't remove nonmatching dupe"                                 | true        | true          | true       | false    | false       | "1" | "uid=1,ou=people,dc=berkeley,dc=edu"         | 0       | 0       | 1       | 0       | 0         | FoundObjectMethod.BY_DN_MATCHED_KEY          | null    | null                                                                                 | null
        "test update with two dupes by finding by first-found, rename one, delete the other"                 | true        | false         | true       | false    | true        | "1" | "uid=1,ou=the middle,dc=berkeley,dc=edu"     | 1       | 1       | 1       | 0       | 1         | FoundObjectMethod.BY_FIRST_FOUND             | ["1"]   | ["uid=1,ou=people,dc=berkeley,dc=edu", "uid=1,ou=expired people,dc=berkeley,dc=edu"] | ["uid=1,ou=people,dc=berkeley,dc=edu", "uid=1,ou=expired people,dc=berkeley,dc=edu"]
        "test update with two dupes by finding by entryUUID but mismatched dn, rename one, delete the other" | true        | true          | true       | false    | true        | "1" | "uid=1,ou=the middle,dc=berkeley,dc=edu"     | 1       | 1       | 1       | 0       | 1         | FoundObjectMethod.BY_MATCHED_KEY_DN_MISMATCH | ["1"]   | ["uid=1,ou=expired people,dc=berkeley,dc=edu"]                                       | ["uid=1,ou=people,dc=berkeley,dc=edu"]
        "test delete"                                                                                        | true        | false         | false      | true     | true        | "1" | "uid=1,ou=people,dc=berkeley,dc=edu"         | 1       | 0       | 0       | 0       | 0         | null                                         | ["1"]   | ["uid=1,ou=people,dc=berkeley,dc=edu"]                                               | null
        "test multi-delete"                                                                                  | true        | false         | true       | true     | true        | "1" | "uid=1,ou=people,dc=berkeley,dc=edu"         | 2       | 0       | 0       | 0       | 0         | null                                         | ["1"]   | ["uid=1,ou=people,dc=berkeley,dc=edu", "uid=1,ou=expired people,dc=berkeley,dc=edu"] | null
    }

    void "test persist return value on a non-modification"() {
        given:
        UidObjectDefinition objDef = new UidObjectDefinition(
                objectClass: "person",
                keepExistingAttributesWhenUpdating: true,
                removeDuplicatePrimaryKeys: true
        )

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
        UidObjectDefinition uidObjectDef = new UidObjectDefinition(
                objectClass: "person",
                keepExistingAttributesWhenUpdating: true,
                removeDuplicatePrimaryKeys: true
        )

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
                        success: true,
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
        UidObjectDefinition objDef = new UidObjectDefinition(
                objectClass: "person",
                keepExistingAttributesWhenUpdating: true,
                removeDuplicatePrimaryKeys: true
        )
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
                deleteEventCallbacks: [deleteEventCallback],
                uniqueIdentifierEventCallbacks: [uniqueIdentifierEventCallback]
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
        retrieved.first().description == "initial test"
        msg.success
        msg.eventId == eventId
        msg.objectDef == objDef
        msg.pkey == uid
        msg.dn == dn
        msg.newAttributes == [
                uid        : uid,
                objectClass: objectClasses,
                sn         : "User",
                cn         : "Test User",
                description: "initial test"
        ]
    }

    @Unroll("#description")
    void "test deletes"() {
        given:
        UidObjectDefinition objDef = new UidObjectDefinition(
                objectClass: "person",
                keepExistingAttributesWhenUpdating: true,
                removeDuplicatePrimaryKeys: removeDupePkeys
        )
        List<String> objectClasses = ["top", "person", "inetOrgPerson", "organizationalPerson"]
        String eventId = "eventId"
        String dn = "uid=1,ou=people,dc=berkeley,dc=edu"
        String uid = "1"

        when:
        addOu("people")
        addOu("expired people")

        // initial create
        assert ldapConnector.persist(eventId, objDef, null, [
                dn         : dn,
                uid        : uid,
                objectClass: objectClasses,
                sn         : "User",
                cn         : "Test User",
                description: "initial test"
        ], false)

        if (createDupe) {
            // duplicate
            addTestEntry("uid=$uid,ou=expired people,dc=berkeley,dc=edu", uid)
            assert ((DirContextAdapter) ldapTemplate.lookup("uid=$uid,ou=expired people,dc=berkeley,dc=edu")).getStringAttribute("description") == "initial test"
        }

        // a subordinate is a leaf of the DN
        if (createSubordinate) {
            addTestEntry("uid=$uid,$dn", uid)
        }

        boolean wasDeleted = ldapConnector.persist(eventId, objDef, null, [dn: delDn, uid: delUid], true)

        Map<String, Object> retrievedByDn = lookupDn(dn)
        List<Map<String, Object>> retrievedByUid = searchForUid(uid)

        and: "cleanup"
        if (retrievedByDn) {
            deleteDn(dn)
        }
        List<Map<String, Object>> uidsToCleanUp = searchForUid(uid)
        uidsToCleanUp?.each { Map<String, Object> map ->
            deleteDn(map.dn.toString())
        }
        deleteOu("people")
        deleteOu("expired people")

        then:
        retrievedByUid.size() == remainingUids
        (!remainingDN ? retrievedByDn == null : retrievedByDn != null)
        deletes * deleteEventCallback.receive(_)
        (deletes ? wasDeleted : !wasDeleted)

        where:
        description                                               | removeDupePkeys | createDupe | createSubordinate | delDn                                | delUid   || remainingDN | remainingUids | deletes
        "delete by DN"                                            | true            | false      | false             | "uid=1,ou=people,dc=berkeley,dc=edu" | null     || false       | 0             | 1
        "delete by uid"                                           | true            | false      | false             | null                                 | "1"      || false       | 0             | 1
        "delete all by uid"                                       | true            | true       | false             | null                                 | "1"      || false       | 0             | 2
        "delete all by dn and uid"                                | true            | true       | false             | "uid=1,ou=people,dc=berkeley,dc=edu" | "1"      || false       | 0             | 2
        "delete by dn but not uids because dupe removal disabled" | false           | true       | false             | "uid=1,ou=people,dc=berkeley,dc=edu" | "1"      || false       | 1             | 1
        "no delete by uid because dupe removal disabled"          | false           | true       | false             | null                                 | "1"      || true        | 2             | 0
        "attempt deletion of non-existant DN"                     | true            | false      | false             | "uid=foobar,dc=berkeley,dc=edu"      | null     || true        | 1             | 0
        "attempt deletion of non-existant uid"                    | true            | false      | false             | null                                 | "foobar" || true        | 1             | 0
        "delete by DN when there's a subordinate"                 | true            | false      | true              | "uid=1,ou=people,dc=berkeley,dc=edu" | null     || false       | 0             | 2
    }

    void "test updates without specifying a DN"() {
        given:
        UidObjectDefinition objDef = new UidObjectDefinition(
                objectClass: "person",
                keepExistingAttributesWhenUpdating: true,
                removeDuplicatePrimaryKeys: true
        )
        List<String> objectClasses = ["top", "person", "inetOrgPerson", "organizationalPerson"]
        String eventId = "eventId"
        String uid = "1"

        when:
        addOu("people")
        // initial create
        addTestEntry("uid=1,ou=people,dc=berkeley,dc=edu", uid)

        // update
        boolean didUpdate = ldapConnector.persist(eventId, objDef, null, [
                uid        : uid,
                objectClass: objectClasses,
                sn         : "User",
                cn         : "Test User",
                description: "updated"
        ], false)

        List<Map<String, Object>> retrieved = searchForUid(uid)

        and: "cleanup"
        deleteDn("uid=1,ou=people,dc=berkeley,dc=edu")
        deleteOu("people")

        then:
        didUpdate
        retrieved.size() == 1
        retrieved.first().description == "updated"
        1 * updateEventCallback.receive(_)
        1 * uniqueIdentifierEventCallback.receive(_)
    }

    @Unroll("#description")
    void "test dynamic attributes"() {
        given:
        UidObjectDefinition objDef = new UidObjectDefinition(
                objectClass: "person",
                keepExistingAttributesWhenUpdating: true,
                removeDuplicatePrimaryKeys: true,
                dynamicAttributeNames: ["description.ONCREATE", "description.ONUPDATE", "description.CONDITION"] as String[]
        )
        List<String> objectClasses = ["top", "person", "inetOrgPerson", "organizationalPerson"]
        String eventId = "eventId"
        String uid = "1"
        String dn = "uid=1,ou=people,dc=berkeley,dc=edu"

        when: "initialize callbacks"
        boolean _conditionIsSet = conditionActive
        ldapConnector.dynamicAttributeCallbacks["description.CONDITION"] = new LdapDynamicAttributeCallback() {
            @Override
            LdapDynamicAttributeCallbackResult attributeValue(
                    String _eventId,
                    LdapObjectDefinition objectDef,
                    LdapCallbackContext context,
                    FoundObjectMethod foundObjectMethod,
                    String pkey,
                    String _dn,
                    String attributeName,
                    Map<String, Object> existingAttributeMap,
                    Object existingValue,
                    String dynamicCallbackIndicator,
                    Object dynamicValueTemplate
            ) {
                if (_conditionIsSet) {
                    // update
                    return new LdapDynamicAttributeCallbackResult(
                            attributeValue: dynamicValueTemplate
                    )
                } else {
                    return null
                }
            }
        }

        and: "create directory entry"
        addOu("people")
        boolean didCreate = ldapConnector.persist(eventId, objDef, null, [
                dn              : dn,
                uid             : uid,
                objectClass     : objectClasses,
                sn              : "User",
                cn              : "Test User",
                (createDescAttr): "initial description"
        ], false)

        and: "update directory entry, if requested"
        boolean didUpdate = false
        if (updateDescAttr) {
            didUpdate = ldapConnector.persist(eventId, objDef, null, [
                    dn              : dn,
                    uid             : uid,
                    objectClass     : objectClasses,
                    (updateDescAttr): "updated description"
            ], false)
        }

        and: "retrieve directory entry"
        List<Map<String, Object>> retrieved = searchForUid(uid)

        and: "cleanup"
        ldapConnector.dynamicAttributeCallbacks.remove("description.CONDITION")
        deleteDn(dn)
        deleteOu("people")

        then:
        didCreate
        retrieved.size() == 1
        retrieved.first().description == exptdDescription

        where:
        description                                        | createDescAttr          | updateDescAttr          | conditionActive || exptdDescription
        "attribute condition is met"                       | "description.CONDITION" | null                    | true            || "initial description"
        "condition is not met"                             | "description.CONDITION" | null                    | false           || null
        "ONCREATE condition is met"                        | "description.ONCREATE"  | null                    | false           || "initial description"
        "update attribute condition is met"                | "description.CONDITION" | "description.ONUPDATE"  | true            || "updated description"
        "entry updated but only ONCREATE condition is met" | "description.ONCREATE"  | "description.CONDITION" | false           || "initial description"
    }

    void "test updates with renaming disabled"() {
        given:
        UidObjectDefinition objDef = new UidObjectDefinition(
                objectClass: "person",
                keepExistingAttributesWhenUpdating: true,
                removeDuplicatePrimaryKeys: true,
                dynamicAttributeNames: ["dn.ONCREATE"] as String[]
        )
        List<String> objectClasses = ["top", "person", "inetOrgPerson", "organizationalPerson"]
        String eventId = "eventId"
        String uid = "1"

        when:
        addOu("people")
        // initial create
        addTestEntry("uid=1,ou=people,dc=berkeley,dc=edu", uid)

        // update
        boolean didUpdate = ldapConnector.persist(eventId, objDef, null, [
                "dn.ONCREATE": "uid=1,ou=expired people,dc=berkeley,dc=edu", // different dn than what we created with
                uid          : uid,
                objectClass  : objectClasses,
                sn           : "User",
                cn           : "Test User",
                description  : "updated"
        ], false)

        List<Map<String, Object>> retrieved = searchForUid(uid)

        and: "cleanup"
        deleteDn("uid=1,ou=people,dc=berkeley,dc=edu")
        deleteOu("people")

        then:
        didUpdate
        retrieved.size() == 1
        // not renamed
        retrieved.first().dn == "uid=1,ou=people,dc=berkeley,dc=edu" // not renamed
        retrieved.first().description == "updated"
        1 * updateEventCallback.receive(_)
        // but a uniqueId callback was produced signaling possible globally unique identifier change
        1 * uniqueIdentifierEventCallback.receive(_) >> { LdapUniqueIdentifierEventMessage msg ->
            assert msg.success
            assert msg.causingEvent == LdapEventType.UPDATE_EVENT
            assert msg.eventId == eventId
            assert msg.objectDef == objDef
            assert msg.pkey == "1"
            assert msg.oldDn == "uid=1,ou=expired people,dc=berkeley,dc=edu" // requested dn
            assert msg.newDn == "uid=1,ou=people,dc=berkeley,dc=edu" // actual dn
            assert msg.globallyUniqueIdentifier
            assert !msg.wasRenamed

        }
    }

    void "test retrieving globally unique identifier"() {
        given:
        UidObjectDefinition objDef = new UidObjectDefinition(
                objectClass: "person",
                keepExistingAttributesWhenUpdating: true,
                removeDuplicatePrimaryKeys: true
        )
        List<String> objectClasses = ["top", "person", "inetOrgPerson", "organizationalPerson"]
        String eventId = "eventId"
        String uid = "1"
        String dn = "uid=1,ou=people,dc=berkeley,dc=edu"

        when:
        addOu("people")
        // initial create
        boolean didCreate = ldapConnector.persist(eventId, objDef, null, [
                dn         : dn,
                uid        : uid,
                objectClass: objectClasses,
                sn         : "User",
                cn         : "Test User",
                description: "initial test"
        ], false)


        List<Map<String, Object>> retrieved = searchForUid(uid)

        String uniqueIdentifier = ldapConnector.getGloballyUniqueIdentifier(eventId, objDef, null, dn)

        and: "cleanup"
        deleteDn(dn)
        deleteOu("people")

        then:
        didCreate
        retrieved.size() == 1
        retrieved.first().description == "initial test"
        1 * insertEventCallback.receive(_)
        uniqueIdentifier?.length()
    }

    void "test persistence when retrieving-by-primary-key is disabled"() {
        given:
        UidObjectDefinition objDef = new UidObjectDefinition() {
            @Override
            LdapQuery getLdapQueryForPrimaryKey(String pkey) {
                // searching by primary key is disabled by returning null
                return null
            }
        }
        objDef.with {
            objectClass = "person"
            keepExistingAttributesWhenUpdating = true
            removeDuplicatePrimaryKeys = false
        }
        List<String> objectClasses = ["top", "person", "inetOrgPerson", "organizationalPerson"]
        String eventId = "eventId"
        String uid = "1"
        String dn1 = "uid=1,ou=people,dc=berkeley,dc=edu"
        String dn2 = "uid=1,ou=expired people,dc=berkeley,dc=edu"

        when:
        addOu("people")
        addOu("expired people")
        // create #1
        boolean didCreate1 = ldapConnector.persist(eventId, objDef, null, [
                dn         : dn1,
                uid        : uid,
                objectClass: objectClasses,
                sn         : "User",
                cn         : "Test User",
                description: "test #1"
        ], false)

        // create #2
        boolean didCreate2 = ldapConnector.persist(eventId, objDef, null, [
                dn         : dn2,
                uid        : uid,
                objectClass: objectClasses,
                sn         : "User",
                cn         : "Test User",
                description: "test #2"
        ], false)


        List<Map<String, Object>> retrieved = searchForUid(uid)

        and: "cleanup"
        deleteDn(dn1)
        deleteDn(dn2)
        deleteOu("people")
        deleteOu("expired people")

        then:
        didCreate1
        retrieved.size() == 2
        retrieved*.description.sort() == ["test #1", "test #2"].sort()
        2 * insertEventCallback.receive(_)
        0 * updateEventCallback.receive(_)
        0 * renameEventCallback.receive(_)
    }


    @Unroll("#description")
    void "test changing lists that have same values, just in a different case"() {
        given:
        UidObjectDefinition objDef = new UidObjectDefinition(
                objectClass: "person",
                keepExistingAttributesWhenUpdating: true,
                removeDuplicatePrimaryKeys: true
        )
        String eventId = "eventId"
        String uid = "1"
        String dn = "uid=1,ou=people,dc=berkeley,dc=edu"

        when:
        addOu("people")
        // initial create
        addTestEntry(dn, uid)

        // update
        boolean didUpdate = ldapConnector.persist(eventId, objDef, null, [
                dn         : dn,
                uid        : uid,
                objectClass: objectClasses,
                sn         : "User",
                cn         : "Test User",
                description: "updated"
        ], false)

        List<Map<String, Object>> retrieved = searchForUid(uid)

        and: "cleanup"
        deleteDn(dn)
        deleteOu("people")

        then:
        didUpdate
        retrieved.size() == 1
        retrieved.first().description == "updated"
        retrieved.first().objectClass.sort() == objectClasses.sort()
        1 * updateEventCallback.receive(_) >> { LdapUpdateEventMessage msg ->
            if (expectListReplace) {
                // all the mods should be REPLACE_ATTRIBUTE
                assert !msg.modificationItems.any { it.modificationOp != DirContextAdapter.REPLACE_ATTRIBUTE }
            } else {
                // when not replacing the entire list, there should be at least one REMOVE
                assert msg.modificationItems.any { it.modificationOp == DirContextAdapter.REMOVE_ATTRIBUTE }
            }
        }

        where:
        description                               | objectClasses                                              | expectListReplace
        "same list, just differently cases"       | ["top", "inetorgperson", "person", "organizationalperson"] | true
        "remove one attribute from original list" | ["top", "inetOrgPerson", "person"]                         | false
    }

    void "test insert-only attribute"() {
        given:
        UidObjectDefinition objDef = new UidObjectDefinition(
                objectClass: "person",
                keepExistingAttributesWhenUpdating: true,
                removeDuplicatePrimaryKeys: true,
                dynamicAttributeNames: ["cn.ONCREATE"] as String[]
        )
        List<String> objectClasses = ["top", "person", "inetOrgPerson", "organizationalPerson"]
        String eventId = "eventId"
        String uid = "1"
        String dn = "uid=1,ou=people,dc=berkeley,dc=edu"

        when:
        addOu("people")
        // create
        boolean didCreate = ldapConnector.persist(eventId, objDef, null, [
                dn           : dn,
                uid          : uid,
                objectClass  : objectClasses,
                sn           : "User",
                "cn.ONCREATE": "Test User",
                description  : "initial test"
        ], false)

        // update
        boolean didUpdate = ldapConnector.persist(eventId, objDef, null, [
                dn           : dn,
                uid          : uid,
                objectClass  : objectClasses,
                sn           : "User",
                "cn.ONCREATE": "IGNORED", // not updated because cn is insert-only
                description  : "updated"
        ], false)


        List<Map<String, Object>> retrieved = searchForUid(uid)

        and: "cleanup"
        deleteDn(dn)
        deleteOu("people")

        then:
        didCreate
        didUpdate
        retrieved.size() == 1
        retrieved.first().description == "updated"
        retrieved.first().cn == "Test User"
        1 * insertEventCallback.receive(_)
        1 * updateEventCallback.receive(_)
    }

    void "test update-only attribute"() {
        given:
        UidObjectDefinition objDef = new UidObjectDefinition(
                objectClass: "person",
                keepExistingAttributesWhenUpdating: true,
                removeDuplicatePrimaryKeys: true,
                dynamicAttributeNames: ["description.ONUPDATE"] as String[]
        )
        List<String> objectClasses = ["top", "person", "inetOrgPerson", "organizationalPerson"]
        String eventId = "eventId"
        String uid = "1"
        String dn = "uid=1,ou=people,dc=berkeley,dc=edu"

        when:
        addOu("people")
        boolean didCreate = ldapConnector.persist(eventId, objDef, null, [
                dn                    : dn,
                uid                   : uid,
                objectClass           : objectClasses,
                sn                    : "User",
                cn                    : "Test User",
                "description.ONUPDATE": "initial test"
        ], false)

        List<Map<String, Object>> retrieved = searchForUid(uid)

        and: "cleanup"
        deleteDn(dn)
        deleteOu("people")

        then:
        didCreate
        retrieved.size() == 1
        // description is the update-only attribute, but is still there because the object was updated after the insert
        retrieved.first().description == "initial test"
        1 * insertEventCallback.receive(_)
        1 * uniqueIdentifierEventCallback.receive(_)
        // update should have been called to add the description attribute
        1 * updateEventCallback.receive(_)
    }

    void "test attribute removal"() {
        given:
        UidObjectDefinition objDef = new UidObjectDefinition(
                objectClass: "person",
                keepExistingAttributesWhenUpdating: true,
                removeDuplicatePrimaryKeys: true
        )
        List<String> objectClasses = ["top", "person", "inetOrgPerson", "organizationalPerson"]
        String eventId = "eventId"
        String uid = "1"
        String dn = "uid=1,ou=people,dc=berkeley,dc=edu"

        when:
        addOu("people")
        boolean didCreate = ldapConnector.persist(eventId, objDef, null, [
                dn          : dn,
                uid         : uid,
                objectClass : objectClasses,
                sn          : "User",
                cn          : "Test User",
                description : "initial test",
                userPassword: "foobar"
        ], false)

        List<Map<String, Object>> retrieved = searchForUid(uid)
        assert retrieved.first().description == "initial test"
        assert retrieved.first().userPassword

        boolean wasModified = ldapConnector.removeAttributes(eventId, objDef, null, null, uid, null, ["description", "userPassword"] as String[])
        retrieved = searchForUid(uid)

        and: "cleanup"
        deleteDn(dn)
        deleteOu("people")

        then:
        didCreate
        wasModified
        retrieved.size() == 1
        retrieved.first().cn
        !retrieved.first().description
        !retrieved.first().userPassword
        1 * insertEventCallback.receive(_)
        1 * removeAttributesEventCallback.receive(_)
    }

    void "test attempted attribute removal on a nonexistent object"() {
        given:
        UidObjectDefinition objDef = new UidObjectDefinition(
                objectClass: "person",
                keepExistingAttributesWhenUpdating: true,
                removeDuplicatePrimaryKeys: true
        )
        String eventId = "eventId"
        String uid = "bogus"
        String dn = "uid=bogus,ou=people,dc=berkeley,dc=edu"

        when:
        LdapConnectorException exception = null
        try {
            ldapConnector.removeAttributes(eventId, objDef, null, dn, uid, null, ["description"] as String[])
        }
        catch (LdapConnectorException e) {
            exception = e
        }

        then:
        exception.cause instanceof ConnectorObjectNotFoundException
    }

    void "test setAttribute()"() {
        given:
        UidObjectDefinition objDef = new UidObjectDefinition(
                objectClass: "person",
                keepExistingAttributesWhenUpdating: true,
                removeDuplicatePrimaryKeys: true
        )
        List<String> objectClasses = ["top", "person", "inetOrgPerson", "organizationalPerson"]
        String eventId = "eventId"
        String uid = "1"
        String dn = "uid=1,ou=people,dc=berkeley,dc=edu"

        when:
        addOu("people")
        boolean didCreate = ldapConnector.persist(eventId, objDef, null, [
                dn         : dn,
                uid        : uid,
                objectClass: objectClasses,
                sn         : "User",
                cn         : "Test User",
                description: "initial test"
        ], false)

        List<Map<String, Object>> retrieved = searchForUid(uid)
        assert retrieved.first().description == "initial test"

        boolean wasModified1 = ldapConnector.setAttribute(eventId, objDef, null, null, uid, null, "description", "updated")
        boolean wasModified2 = ldapConnector.setAttribute(eventId, objDef, null, null, uid, null, "userPassword", "foobar")
        boolean wasModified3 = ldapConnector.setAttribute(eventId, objDef, null, null, uid, null, "userPassword", "foobar2")
        retrieved = searchForUid(uid)

        and: "cleanup"
        deleteDn(dn)
        deleteOu("people")

        then:
        didCreate
        wasModified1
        wasModified2
        wasModified3
        retrieved.size() == 1
        retrieved.first().description == "updated"
        retrieved.first().userPassword
        1 * insertEventCallback.receive(_)
        3 * setAttributeEventCallback.receive(_)
    }

    void "test attempted setAttribute() on a nonexistent object"() {
        given:
        UidObjectDefinition objDef = new UidObjectDefinition(
                objectClass: "person",
                keepExistingAttributesWhenUpdating: true,
                removeDuplicatePrimaryKeys: true
        )
        String eventId = "eventId"
        String uid = "bogus"
        String dn = "uid=bogus,ou=people,dc=berkeley,dc=edu"

        when:
        LdapConnectorException exception = null
        try {
            ldapConnector.setAttribute(eventId, objDef, null, null, uid, null, "description", "updated")
        }
        catch (LdapConnectorException e) {
            exception = e
        }

        then:
        exception.cause instanceof ConnectorObjectNotFoundException
    }
}
