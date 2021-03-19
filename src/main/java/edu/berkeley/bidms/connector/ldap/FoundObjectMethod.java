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

/**
 * Indicates how an object was found when an object is updated.
 */
public enum FoundObjectMethod {
    BY_DN_MATCHED_KEY,

    /**
     * Indicates the object was found by its DN but the object's globally
     * unique identifier and primary key don't match the desired values.
     */
    BY_DN_MISMATCHED_KEYS,

    /**
     * Indicates the object was found by its globally unique identifier or
     * its primary key but the object's DN does not match the desired DN.
     */
    BY_MATCHED_KEY_DN_MISMATCH,

    /**
     * Indicates the object was found by its primary key but the object's DN
     * was not provided during the persist request.
     */
    BY_MATCHED_KEY_DN_NOT_PROVIDED,

    /**
     * Indicates there are multiple objects in the directory with the same
     * primary key but none of them match the desired DN.  The first one
     * found was selected.
     */
    BY_FIRST_FOUND;
}
