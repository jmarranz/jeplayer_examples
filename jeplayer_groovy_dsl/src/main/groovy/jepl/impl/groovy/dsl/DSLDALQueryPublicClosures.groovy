
package jepl.impl.groovy.dsl

/**
 *
 * @author jmarranz
 */
class DSLDALQueryPublicClosures 
{
    Closure code
    Closure params
    Closure listener
    
    Closure firstResult    
    Closure maxResults
    Closure strictMaxRows    
    Closure strictMinRows     
    
    Closure limits
    
    Closure getGeneratedKey
    Closure executeUpdate
    Closure getOneRowFromSingleField
    Closure getJEPLResultSet
    Closure getJEPLCachedResultSet
}

