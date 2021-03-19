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

import edu.berkeley.bidms.connector.ldap.LdapObjectDefinition;
import edu.berkeley.bidms.connector.ldap.event.LdapCallbackContext;
import edu.berkeley.bidms.connector.ldap.event.LdapEventType;

import java.util.Objects;

/**
 * Unique identifier messages sent to callbacks where the unique identifier
 * has been created or possibly changed. It's possible this message is
 * generated on rename and update events where the directory has not actually
 * changed the unique identifier.
 */
public class LdapUniqueIdentifierEventMessage implements LdapEventMessage {

    private boolean success;
    private LdapEventType causingEvent; /* can be INSERT_EVENT, UPDATE_EVENT or RENAME_EVENT */
    private String eventId;
    private LdapObjectDefinition objectDef;
    private LdapCallbackContext context;
    private String pkey;
    private String oldDn;
    private String newDn;
    private Object globallyUniqueIdentifier;
    private boolean wasRenamed;
    private Throwable exception;

    @Override
    public LdapEventType getEventType() {
        return LdapEventType.UNIQUE_IDENTIFIER_EVENT;
    }

    @Override
    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public LdapEventType getCausingEvent() {
        return causingEvent;
    }

    public void setCausingEvent(LdapEventType causingEvent) {
        this.causingEvent = causingEvent;
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

    public String getPkey() {
        return pkey;
    }

    public void setPkey(String pkey) {
        this.pkey = pkey;
    }

    public String getOldDn() {
        return oldDn;
    }

    public void setOldDn(String oldDn) {
        this.oldDn = oldDn;
    }

    public String getNewDn() {
        return newDn;
    }

    public void setNewDn(String newDn) {
        this.newDn = newDn;
    }

    public Object getGloballyUniqueIdentifier() {
        return globallyUniqueIdentifier;
    }

    public void setGloballyUniqueIdentifier(Object globallyUniqueIdentifier) {
        this.globallyUniqueIdentifier = globallyUniqueIdentifier;
    }

    public boolean getWasRenamed() {
        return wasRenamed;
    }

    public void setWasRenamed(boolean wasRenamed) {
        this.wasRenamed = wasRenamed;
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
        if (!(o instanceof LdapUniqueIdentifierEventMessage)) return false;
        LdapUniqueIdentifierEventMessage that = (LdapUniqueIdentifierEventMessage) o;
        return success == that.isSuccess() &&
                wasRenamed == that.getWasRenamed() &&
                getEventType() == that.getEventType() &&
                causingEvent == that.getCausingEvent() &&
                Objects.equals(eventId, that.getEventId()) &&
                Objects.equals(objectDef, that.getObjectDef()) &&
                Objects.equals(context, that.getContext()) &&
                Objects.equals(pkey, that.getPkey()) &&
                Objects.equals(oldDn, that.getOldDn()) &&
                Objects.equals(newDn, that.getNewDn()) &&
                Objects.equals(globallyUniqueIdentifier, that.getGloballyUniqueIdentifier()) &&
                Objects.equals(exception, that.getException());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getEventType(), success, causingEvent, eventId, objectDef, context, pkey, oldDn, newDn, globallyUniqueIdentifier, wasRenamed, exception);
    }

    @Override
    public String toString() {
        return "LdapUniqueIdentifierEventMessage{" +
                "eventType=" + getEventType() +
                ", success=" + success +
                ", causingEvent=" + causingEvent +
                ", eventId='" + eventId + '\'' +
                ", objectDef=" + objectDef +
                ", context=" + context +
                ", pkey='" + pkey + '\'' +
                ", oldDn='" + oldDn + '\'' +
                ", newDn='" + newDn + '\'' +
                ", globallyUniqueIdentifier=" + globallyUniqueIdentifier +
                ", wasRenamed=" + wasRenamed +
                ", exception=" + exception +
                '}';
    }
}
