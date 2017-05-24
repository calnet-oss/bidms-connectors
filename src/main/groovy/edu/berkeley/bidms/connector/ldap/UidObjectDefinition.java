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

package edu.berkeley.bidms.connector.ldap;

import org.springframework.ldap.query.LdapQuery;
import org.springframework.ldap.query.LdapQueryBuilder;

/**
 * An object definition for LDAP objects where the primary key is the uid
 * attribute.
 */
public class UidObjectDefinition implements LdapObjectDefinition {
    /**
     * The objectClass to filter by when searching for uids.
     * The "person" objectClass would be a typical example.
     */
    private String objectClass;

    /**
     * If true, when updating, the existing attributes that aren't in the
     * update map will be kept instead of removed.
     */
    private boolean keepExistingAttributesWhenUpdating;

    /**
     * If true then entires in the directory with the same primary key that
     * aren't considered the primary entry will be removed.  The primary
     * entry is decided by the first entry encountered where
     * acceptAsExistingDn(dn) returns true.
     */
    private boolean removeDuplicatePrimaryKeys;

    public UidObjectDefinition(String objectClass, boolean keepExistingAttributesWhenUpdating, boolean removeDuplicatePrimaryKeys) {
        this.objectClass = objectClass;
        this.keepExistingAttributesWhenUpdating = keepExistingAttributesWhenUpdating;
        this.removeDuplicatePrimaryKeys = removeDuplicatePrimaryKeys;
    }

    @Override
    public String getPrimaryKeyAttributeName() {
        return "uid";
    }

    @Override
    public LdapQuery getLdapQueryForPrimaryKey(String uid) {
        return LdapQueryBuilder.query().where("objectClass").is(objectClass).and("uid").is(uid);

    }

    @Override
    public boolean acceptAsExistingDn(String dn) {
        // We disregard "entryuuid" entries because of a bug in OpenDJ that
        // creates these during replication.  Whenever an "entryuuid" entry
        // exists, it's a duplicate of another entry that has a proper DN. 
        // So these are never a "primary" entry and should always be
        // removed.
        return !dn.startsWith("entryuuid=");
    }

    @Override
    public boolean keepExistingAttributesWhenUpdating() {
        return keepExistingAttributesWhenUpdating;
    }

    @Override
    public boolean removeDuplicatePrimaryKeys() {
        return removeDuplicatePrimaryKeys;
    }

    public String getObjectClass() {
        return objectClass;
    }

    public void setObjectClass(String objectClass) {
        this.objectClass = objectClass;
    }

    public boolean isKeepExistingAttributesWhenUpdating() {
        return keepExistingAttributesWhenUpdating;
    }

    public void setKeepExistingAttributesWhenUpdating(boolean keepExistingAttributesWhenUpdating) {
        this.keepExistingAttributesWhenUpdating = keepExistingAttributesWhenUpdating;
    }

    public boolean isRemoveDuplicatePrimaryKeys() {
        return removeDuplicatePrimaryKeys;
    }

    public void setRemoveDuplicatePrimaryKeys(boolean removeDuplicatePrimaryKeys) {
        this.removeDuplicatePrimaryKeys = removeDuplicatePrimaryKeys;
    }
}
