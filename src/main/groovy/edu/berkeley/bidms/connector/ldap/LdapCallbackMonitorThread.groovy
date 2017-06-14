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

import edu.berkeley.bidms.connector.ldap.event.message.LdapEventMessage

/**
 * Monitor's the LdapConnector's callback queue and invokes callbacks asynchronously when messages are present on the queue.
 */
class LdapCallbackMonitorThread extends Thread {
    /**
     * Call requestStop() to request a stop of this monitor thread.
     */
    private volatile boolean requestStop = false

    private LdapConnector ldapConnector

    /**
     * @param ldapConnector The LdapConnector object containing the callback queue to monitor.
     */
    LdapCallbackMonitorThread(LdapConnector ldapConnector) {
        this.ldapConnector = ldapConnector
    }

    /**
     * Thread entry point to monitor the ldapConnector's callback queue.
     */
    @Override
    void run() {
        while (!requestStop) {
            try {
                synchronized (this) {
                    wait(1000)
                    LdapEventMessage eventMessage = null
                    int dequeueCount = 0
                    while (!requestStop && (eventMessage = ldapConnector.pollCallbackQueue())) {
                        if (eventMessage != null) {
                            ldapConnector.invokeCallback(eventMessage)
                            dequeueCount++
                        }
                    }
                    if (dequeueCount) {
                        notify()
                    }
                }
            }
            catch (InterruptedException ignored) {
                // no-op
            }
        }
    }

    /**
     * Request a stop of the monitor thread.
     */
    void requestStop() {
        this.requestStop = true
        interrupt()
    }
}
