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

import org.springframework.ldap.core.ContextMapper
import org.springframework.ldap.core.DirContextAdapter

import javax.naming.NamingException
import javax.naming.directory.Attribute

/**
 * Converts the attributes of a search result to a Map
 */
class ToMapContextMapper implements ContextMapper<Map<String, Object>> {
    @Override
    Map<String, Object> mapFromContext(Object ctx) throws NamingException {
        DirContextAdapter searchResult
        if (ctx instanceof DirContextAdapter) {
            searchResult = (DirContextAdapter) ctx
        } else {
            throw new RuntimeException("Not supported for ctx type ${ctx?.getClass()?.name}.  Only DirContextAdapter objects are supported.")
        }

        Map<String, Object> result = [:]
        searchResult.attributes.all.each { Attribute attr ->
            if(attr.size() > 0) {
                result.put(attr.ID, convertAttribute(attr))
            }
        }

        // dn is added as an extra pseudo-attribute
        result.put("dn", searchResult.dn.toString())

        return result
    }

    static Object convertAttribute(Attribute attr) {
        if (attr.size() == 1) {
            return attr.get()
        } else if (attr.size() > 1) {
            def list = []
            attr.all.each {
                list.add(it)
            }
            return list
        }
        else {
            throw new RuntimeException("attr size is 0")
        }
    }
}
