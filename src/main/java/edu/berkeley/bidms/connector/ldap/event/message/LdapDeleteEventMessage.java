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
 * Delete messages sent to callbacks.
 */
public class LdapDeleteEventMessage implements LdapEventMessage {

    private boolean success;
    private String eventId;
    private LdapObjectDefinition objectDef;
    private LdapCallbackContext context;
    private String pkey;
    private String dn;
    private Throwable exception;

    @Override
    public LdapEventType getEventType() {
        return LdapEventType.DELETE_EVENT;
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

    public String getPkey() {
        return pkey;
    }

    public void setPkey(String pkey) {
        this.pkey = pkey;
    }

    public String getDn() {
        return dn;
    }

    public void setDn(String dn) {
        this.dn = dn;
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
        if (!(o instanceof LdapDeleteEventMessage)) return false;
        LdapDeleteEventMessage that = (LdapDeleteEventMessage) o;
        return success == that.isSuccess() &&
                getEventType() == that.getEventType() &&
                Objects.equals(eventId, that.getEventId()) &&
                Objects.equals(objectDef, that.getObjectDef()) &&
                Objects.equals(context, that.getContext()) &&
                Objects.equals(pkey, that.getPkey()) &&
                Objects.equals(dn, that.getDn()) &&
                Objects.equals(exception, that.getException());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getEventType(), success, eventId, objectDef, context, pkey, dn, exception);
    }

    @Override
    public String toString() {
        return "LdapDeleteEventMessage{" +
                "eventType=" + getEventType() +
                ", success=" + success +
                ", eventId='" + eventId + '\'' +
                ", objectDef=" + objectDef +
                ", context=" + context +
                ", pkey='" + pkey + '\'' +
                ", dn='" + dn + '\'' +
                ", exception=" + exception +
                '}';
    }
}
