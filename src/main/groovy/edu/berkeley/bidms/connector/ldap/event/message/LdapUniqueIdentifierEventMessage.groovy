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

package edu.berkeley.bidms.connector.ldap.event.message

import edu.berkeley.bidms.connector.ldap.LdapObjectDefinition
import edu.berkeley.bidms.connector.ldap.event.LdapCallbackContext
import edu.berkeley.bidms.connector.ldap.event.LdapEventType

/**
 * Unique identifier messages sent to callbacks where the unique identifier has been created or possibly changed.
 * It's possible this message is generated on rename and update events where the directory has not actually changed the unique identifier.
 */
class LdapUniqueIdentifierEventMessage implements LdapEventMessage {
    LdapEventType getEventType() {
        return LdapEventType.UNIQUE_IDENTIFIER_EVENT
    }

    boolean success
    LdapEventType causingEvent /* can be INSERT_EVENT, UPDATE_EVENT or RENAME_EVENT */
    String eventId
    LdapObjectDefinition objectDef
    LdapCallbackContext context
    String pkey
    String oldDn
    String newDn
    Object globallyUniqueIdentifier
    Throwable exception
}
