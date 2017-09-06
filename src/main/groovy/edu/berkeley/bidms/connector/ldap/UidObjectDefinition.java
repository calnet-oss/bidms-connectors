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

import static org.springframework.ldap.query.LdapQueryBuilder.query;

/**
 * An object definition for LDAP objects where the primary key is the uid
 * attribute and the globally unique identifier is the entryUUID attribute.
 */
public class UidObjectDefinition implements LdapObjectDefinition {
    /**
     * The objectClass to filter by when searching for primary keys. The
     * "person" objectClass would be a typical example.
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

    /**
     * An array of attribute names (with their dynamic indicators) for which
     * the values of these attributes are dynamically determined by a
     * callback that is assigned to the dynamic indicator.
     * <p/>
     * The strings in this array have the following naming convention:
     * <code>attributeName.indicator</code> where <i>attributeName</i> is the
     * attribute in the downstream system and <i>indicator</i> is a string
     * that identifies which callback to use.  The callbacks are configured
     * in the connector's dynamicAttributeCallbacks map, where the map key is
     * <i>attributeName.indicator</i> or <i>indicator</i> (for an indicator
     * that applies to all attributes) and the value is the instance of the
     * callback.
     * <p/>
     * 'dn.ONCREATE' may be included, which is a special case that will
     * disable renaming of the object.
     */
    private String[] dynamicAttributeNames;

    /**
     * The globally unique identifier attribute in the directory, which is
     * typically an operational attribute.
     * <p>
     * This is often entryUUID in LDAP directories and objectGUID in Active
     * Directory directories.
     *
     * @return The globally unique identifier attribute name.
     */
    @Override
    public String getGloballyUniqueIdentifierAttributeName() {
        return "entryUUID";
    }

    /**
     * The primary key attribute in the directory.  For a person, this is
     * often uid.
     *
     * @return The primary key attribute name.
     */
    @Override
    public String getPrimaryKeyAttributeName() {
        return "uid";
    }

    /**
     * Get a Spring LdapQuery object to query the directory for objects by a
     * globally unique identifier.
     *
     * @param pkey             When searching by globally unique identifier,
     *                         the object must also match this expected
     *                         primary key.
     * @param uniqueIdentifier The globally unique identifier value.
     * @return The Spring LdapQuery object to query the directory for objects
     * by their globally unique identifier.
     */
    @Override
    public LdapQuery getLdapQueryForGloballyUniqueIdentifier(String pkey, Object uniqueIdentifier) {
        return query()
                .where("objectClass")
                .is(objectClass)
                .and(getPrimaryKeyAttributeName())
                .is(pkey)
                .and(getGloballyUniqueIdentifierAttributeName())
                .is(uniqueIdentifier.toString());
    }

    /**
     * Get a Spring LdapQuery object to query the directory for objects by a
     * primary key value.
     *
     * @param pkey The primary key value.
     * @return The Spring LdapQuery object to query the directory for objects
     * by their primary key.
     */
    @Override
    public LdapQuery getLdapQueryForPrimaryKey(String pkey) {
        return query()
                .where("objectClass")
                .is(objectClass)
                .and(getPrimaryKeyAttributeName())
                .is(pkey);
    }

    /**
     * Implements criteria for successfully accepting a DN as a good entry
     * when resolving multiple entries when searching by primary key.  In
     * some (probably rare) cases, the directory may contain entries with the
     * same primary key but are known not ever to be a "primary" entry.  For
     * example, it's been observed that a vendor LDAP directory will leave
     * undesired replication artifacts in the directory when replicating
     * within a cluster.  These replication artifacts are to be disregarded
     * and deleted as undesired duplicates.
     * <p>
     * Note this is *only* used when resolving results when searching by
     * primary key.
     *
     * @param dn The dn to evaluate for acceptance.
     * @return true if the dn is accepted or false if the dn is rejected.
     */
    @Override
    public boolean acceptAsExistingDn(String dn) {
        // We disregard "entryuuid" entries because of a bug in OpenDJ that
        // creates these during replication.  Whenever an "entryuuid" entry
        // exists, it's a duplicate of another entry that has a proper DN. 
        // So these are never a "primary" entry and should always be
        // removed.
        return !dn.startsWith("entryuuid=");
    }

    /**
     * @return true indicates that when updating, the existing attributes
     * that aren't in the update map will be kept instead of removed.
     */
    @Override
    public boolean isKeepExistingAttributesWhenUpdating() {
        return keepExistingAttributesWhenUpdating;
    }

    /**
     * @param keepExistingAttributesWhenUpdating true indicates that when
     *                                           updating, the existing
     *                                           attributes that aren't in
     *                                           the update map will be kept
     *                                           instead of removed.
     */
    public void setKeepExistingAttributesWhenUpdating(boolean keepExistingAttributesWhenUpdating) {
        this.keepExistingAttributesWhenUpdating = keepExistingAttributesWhenUpdating;
    }

    /**
     * @return true indicates that entires in the directory with the same
     * primary key that aren't considered the primary entry will be removed.
     * The primary entry is decided by the first entry encountered where
     * acceptAsExistingDn(dn) returns true.
     */
    @Override
    public boolean isRemoveDuplicatePrimaryKeys() {
        return removeDuplicatePrimaryKeys;
    }

    /**
     * @param removeDuplicatePrimaryKeys true indicates that entires in the
     *                                   directory with the same primary key
     *                                   that aren't considered the primary
     *                                   entry will be removed. The primary
     *                                   entry is decided by the first entry
     *                                   encountered where acceptAsExistingDn(dn)
     *                                   returns true.
     */
    public void setRemoveDuplicatePrimaryKeys(boolean removeDuplicatePrimaryKeys) {
        this.removeDuplicatePrimaryKeys = removeDuplicatePrimaryKeys;
    }

    /**
     * @return The objectClass to filter by when searching for primary keys.
     */
    @Override
    public String getObjectClass() {
        return objectClass;
    }

    /**
     * @param objectClass The objectClass to filter by when searching for
     *                    primary keys.
     */
    public void setObjectClass(String objectClass) {
        this.objectClass = objectClass;
    }

    /**
     * @return An array of attribute names (with their dynamic indicators)
     * for which the values of these attributes are dynamically determined by
     * a callback that is assigned to the dynamic indicator.
     */
    @Override
    public String[] getDynamicAttributeNames() {
        return dynamicAttributeNames;
    }

    /**
     * @param dynamicAttributeNames An array of attribute names (with their
     *                              dynamic indicators) for which the values
     *                              of these attributes are dynamically
     *                              determined by a callback that is assigned
     *                              to the dynamic indicator.
     */
    public void setDynamicAttributeNames(String[] dynamicAttributeNames) {
        this.dynamicAttributeNames = dynamicAttributeNames;
    }
}
