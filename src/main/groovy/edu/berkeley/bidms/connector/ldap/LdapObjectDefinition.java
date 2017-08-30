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

import edu.berkeley.bidms.connector.ObjectDefinition;
import org.springframework.ldap.query.LdapQuery;

public interface LdapObjectDefinition extends ObjectDefinition {
    /**
     * The globally unique identifier attribute in the directory, which is
     * typically an operational attribute.
     * <p>
     * This is often entryUUID in LDAP directories and objectGUID in Active
     * Directory directories.
     *
     * @return The globally unique identifier attribute name.
     */
    String getGloballyUniqueIdentifierAttributeName();

    /**
     * The primary key attribute in the directory.  For a person, this is
     * often uid.
     *
     * @return The primary key attribute name.
     */
    String getPrimaryKeyAttributeName();

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
    LdapQuery getLdapQueryForGloballyUniqueIdentifier(String pkey, Object uniqueIdentifier);

    /**
     * Get a Spring LdapQuery object to query the directory for objects by a
     * primary key value.
     *
     * @param pkey The primary key value.
     * @return The Spring LdapQuery object to query the directory for objects
     * by their primary key.  null if searching by primary key is not
     * supported.
     */
    LdapQuery getLdapQueryForPrimaryKey(String pkey);

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
    boolean acceptAsExistingDn(String dn);

    /**
     * @return true indicates that when updating, the existing attributes
     * that aren't in the update map will be kept instead of removed.
     */
    boolean isKeepExistingAttributesWhenUpdating();

    /**
     * @return true indicates that entires in the directory with the same
     * primary key that aren't considered the primary entry will be removed.
     * The primary entry is decided by the first entry encountered where
     * acceptAsExistingDn(dn) returns true.
     */
    boolean isRemoveDuplicatePrimaryKeys();

    /**
     * @return The objectClass to filter by when searching for primary keys.
     */
    String getObjectClass();

    /**
     * @return A list of attribute names, which must be multi-value
     * attributes, to append to rather than overwrite when updating.
     */
    String[] getAppendOnlyAttributeNames();

    /**
     * @return An array of attribute names indicating attributes that should
     * only be inserted and should be left alone during updates.  'dn' may be
     * included, which is a special case that will disable renaming the
     * object.
     */
    String[] getInsertOnlyAttributeNames();

    /**
     * @return An array of attribute names indicating attributes that should
     * only be updated and should be left out during inserts.
     */
    String[] getUpdateOnlyAttributeNames();

    /**
     * Returns An array of attribute names indicating attributes that are
     * only set if a condition indicator is present in the conditional
     * indicator collection created by a conditional callback that's
     * configured for the connector.
     * <p/>
     * The strings in this array follow naming convention for conditional
     * attributes: <code>attributeName.condition</code> where
     * <i>attributeName</i> is the attribute in the downstream system and
     * <i>condition</i> is the indicator string that is possibly present in
     * the conditional indicator collection.  If it is present in the
     * conditional indicator collection, the attribute is set.  If it's not
     * present, it's not set.  <i>ONCREATE</i> and <i>ONUPDATE</i> are
     * pre-set global conditions depending on whether the downstream entry
     * exists or not at time of persistence.
     *
     * @return An array of attribute names indicating attributes that are
     * only set if a condition indicator is present in the conditional
     * indicator collection created by a conditional callback that's
     * configured for the connector.
     */
    String[] getConditionalAttributeNames();
}
