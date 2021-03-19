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

import edu.berkeley.bidms.connector.ldap.event.LdapCallbackContext;

import javax.naming.Name;
import java.util.Map;

public interface LdapDynamicAttributeCallback {
    /**
     * @return A LdapDynamicAttributeCallbackResult instance that contains
     * the attributeValue or null if the attribute is not to be modified. If
     * you wish to remove the attribute, return an instance of
     * LdapDynamicAttributeCallbackResult where the attributeValue is set to
     * null.
     */
    LdapDynamicAttributeCallbackResult attributeValue(
            String eventId,
            LdapObjectDefinition objectDef,
            LdapCallbackContext context,
            FoundObjectMethod foundObjectMethod,
            String pkey,
            Name dn,
            String attributeName,
            Map<String, Object> newAttributeMap,
            Map<String, Object> existingAttributeMap,
            Object existingValue,
            String dynamicCallbackIndicator,
            Object dynamicValueTemplate
    );
}
