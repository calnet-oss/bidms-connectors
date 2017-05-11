package edu.berkeley.bidms.connector

interface Connector {
    boolean persist(ObjectDefinition objectDef, Map<String, Object> jsonObject)
}
