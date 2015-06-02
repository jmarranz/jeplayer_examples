package jepl.impl.groovy.dsl

import jepl.JEPLListener

/**
 *
 * @author jmarranz
 */
class DSLDALQueryResults 
{
    String code = ""
    Object[] params = null
    Map paramsByName = null
    List<JEPLListener> listeners = null 
    Integer firstResult = null    
    Integer maxResults = null
    Integer strictMaxRows = null
    Integer strictMinRows = null
    
    Class generatedKeyClass = null
    Class oneRowFromSingleFieldClass = null
    
    boolean executeUpdate = false
    boolean getJEPLResultSet = false
    boolean getJEPLCachedResultSet = false
    
    def addListener(JEPLListener listener)
    {
        if (listeners == null) listeners = new LinkedList<JEPLListener>()
        listeners.add(listener)
    }

}

