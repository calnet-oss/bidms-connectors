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
package edu.berkeley.bidms.connector.ldap.event.message;

import edu.berkeley.bidms.connector.ldap.FoundObjectMethod;
import edu.berkeley.bidms.connector.ldap.LdapObjectDefinition;
import edu.berkeley.bidms.connector.ldap.event.LdapCallbackContext;
import edu.berkeley.bidms.connector.ldap.event.LdapEventType;

import javax.naming.directory.ModificationItem;
import java.util.Arrays;
import java.util.Objects;

/**
 * Remove attributes messages sent to callbacks.
 */
public class LdapRemoveAttributesEventMessage implements LdapEventMessage {

    private boolean success;
    private String eventId;
    private LdapObjectDefinition objectDef;
    private LdapCallbackContext context;
    private FoundObjectMethod foundMethod;
    private String pkey;
    private String[] removedAttributeNames;
    private String dn;
    private transient ModificationItem[] modificationItems;
    private Throwable exception;

    @Override
    public LdapEventType getEventType() {
        return LdapEventType.REMOVE_ATTRIBUTES_EVENT;
    }

    @Override
    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public LdapObjectDefinition getObjectDef() {
        return objectDef;
    }

    public void setObjectDef(LdapObjectDefinition objectDef) {
        this.objectDef = objectDef;
    }

    public LdapCallbackContext getContext() {
        return context;
    }

    public void setContext(LdapCallbackContext context) {
        this.context = context;
    }

    public FoundObjectMethod getFoundMethod() {
        return foundMethod;
    }

    public void setFoundMethod(FoundObjectMethod foundMethod) {
        this.foundMethod = foundMethod;
    }

    public String getPkey() {
        return pkey;
    }

    public void setPkey(String pkey) {
        this.pkey = pkey;
    }

    public String[] getRemovedAttributeNames() {
        return removedAttributeNames;
    }

    public void setRemovedAttributeNames(String[] removedAttributeNames) {
        this.removedAttributeNames = removedAttributeNames;
    }

    public String getDn() {
        return dn;
    }

    public void setDn(String dn) {
        this.dn = dn;
    }

    public ModificationItem[] getModificationItems() {
        return modificationItems;
    }

    public void setModificationItems(ModificationItem[] modificationItems) {
        this.modificationItems = modificationItems;
    }

    public Throwable getException() {
        return exception;
    }

    public void setException(Throwable exception) {
        this.exception = exception;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LdapRemoveAttributesEventMessage)) return false;
        LdapRemoveAttributesEventMessage that = (LdapRemoveAttributesEventMessage) o;
        return success == that.isSuccess() &&
                getEventType() == that.getEventType() &&
                Objects.equals(eventId, that.getEventId()) &&
                Objects.equals(objectDef, that.getObjectDef()) &&
                Objects.equals(context, that.getContext()) &&
                foundMethod == that.getFoundMethod() &&
                Objects.equals(pkey, that.getPkey()) &&
                Arrays.equals(removedAttributeNames, that.getRemovedAttributeNames()) &&
                Objects.equals(dn, that.getDn()) &&
                Objects.equals(exception, that.getException());
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(getEventType(), success, eventId, objectDef, context, foundMethod, pkey, dn, exception);
        result = 31 * result + Arrays.hashCode(removedAttributeNames);
        return result;
    }

    @Override
    public String toString() {
        return "LdapRemoveAttributesEventMessage{" +
                "eventType=" + getEventType() +
                ", success=" + success +
                ", eventId='" + eventId + '\'' +
                ", objectDef=" + objectDef +
                ", context=" + context +
                ", foundMethod=" + foundMethod +
                ", pkey='" + pkey + '\'' +
                ", removedAttributeNames=" + Arrays.toString(removedAttributeNames) +
                ", dn='" + dn + '\'' +
                ", exception=" + exception +
                '}';
    }
}
