
package jepl.groovy.dsl

import jepl.JEPLDAL
import jepl.JEPLDALQuery
import jepl.JEPLListener
import jepl.JEPLResultSetDAO
import jepl.impl.groovy.dsl.DSLDALLimitsPublicClosures
import jepl.impl.groovy.dsl.DSLDALQueryPublicClosures
import jepl.impl.groovy.dsl.DSLDALQueryResults

class DSLDAL
{
    protected final JEPLDAL dal
    
    def DSLDAL(JEPLDAL dal)
    {
        this.dal = dal
    }

    JEPLDAL getJEPLDAL()
    {
        return dal
    }    
    
    def protected fillQueryResults(DSLDALQueryPublicClosures closures,DSLDALQueryResults results)
    {      
        Closure codeMethod = { String code -> results.code = code }
        Closure paramsMethod = 
        { Object[] params -> 
            if (params.length == 1 && params[0] instanceof Map)
                results.paramsByName = params[0]
            else
                results.params = params 
        }
        Closure addListenerMethod = { JEPLListener listener -> results.addListener(listener) }    
        
        Closure firstResultMethod = { int firstResult -> results.firstResult = firstResult }
        Closure maxResultsMethod = { int maxResults -> 
            results.maxResults = maxResults }        
        Closure strictMaxRowsMethod = { int strictMaxRows -> results.strictMaxRows = strictMaxRows }
        Closure strictMinRowsMethod = { int strictMinRows -> results.strictMinRows = strictMinRows }             
        
        // Alternativa para firstResult,maxResults,strictMaxRows,strictMinRows, se pueden poner bajo un limits { } además del primer nivel
        def limitClosures = new DSLDALLimitsPublicClosures(firstResult:firstResultMethod,maxResults:maxResultsMethod,strictMaxRows:strictMaxRowsMethod,strictMinRows:strictMinRowsMethod)
        Closure limitsMethod = 
            { Closure limitsUserClosure ->  
                limitsUserClosure.delegate = limitClosures
                limitsUserClosure.call()
            }
        
        Closure getGeneratedKeyMethod = { Class generatedKeyClass -> results.generatedKeyClass = generatedKeyClass }
        Closure executeUpdateMethod = { results.executeUpdate = true }                 
        Closure getOneRowFromSingleFieldMethod = { Class oneRowFromSingleFieldClass -> results.oneRowFromSingleFieldClass = oneRowFromSingleFieldClass }        
        Closure getJEPLResultSetMethod = { results.getJEPLResultSet = true }       
        Closure getJEPLCachedResultSetMethod = { results.getJEPLCachedResultSet = true }
        
        closures.code = codeMethod
        closures.params = paramsMethod
        closures.listener = addListenerMethod
        
        closures.firstResult = firstResultMethod        
        closures.maxResults = maxResultsMethod
        closures.strictMaxRows = strictMaxRowsMethod        
        closures.strictMinRows = strictMinRowsMethod        
        
        closures.limits = limitsMethod;
        
        closures.getGeneratedKey = getGeneratedKeyMethod
        closures.executeUpdate = executeUpdateMethod
        closures.getOneRowFromSingleField = getOneRowFromSingleFieldMethod  
        closures.getJEPLResultSet = getJEPLResultSetMethod
        closures.getJEPLCachedResultSet = getJEPLCachedResultSetMethod
    }
     
    def protected fillQueryResultsAndCall(Closure userClosure,DSLDALQueryPublicClosures closures,DSLDALQueryResults results)    
    {
        fillQueryResults(closures,results)
        
        userClosure.delegate = closures
        
        userClosure.call()        
    }
    
    def protected Object queryInternal(JEPLDALQuery query,Closure userClosure,DSLDALQueryResults results)
    {                     
        if (results.params != null)
            query.addParameters( results.params )
        
        if (results.paramsByName != null)        
            results.paramsByName.each{ k, v -> query.setParameter(k,v) }        
        
        if (results.listeners != null)
            results.listeners.each( { listener -> query.addJEPLListener(listener) } ) 
        
        if (results.firstResult != null)
            query.setFirstResult(results.firstResult)        
        
        if (results.maxResults != null)
            query.setMaxResults(results.maxResults)            
        
        if (results.strictMaxRows != null)
            query.setStrictMaxRows(results.strictMaxRows)         
        
        if (results.strictMinRows != null)
            query.setStrictMinRows(results.strictMinRows)         
        
        if (results.generatedKeyClass != null)
        {
            return query.getGeneratedKey( results.generatedKeyClass )
        }
        else if (results.executeUpdate)
        {
            int update = query.executeUpdate()
            return update
        }
        else if (results.oneRowFromSingleFieldClass != null)
        {
            return query.getOneRowFromSingleField(results.oneRowFromSingleFieldClass)
        }
        else if (results.getJEPLResultSet)
        {
            return query.getJEPLResultSet()
        }     
        else if (results.getJEPLCachedResultSet)
        {
            return query.getJEPLCachedResultSet()
        }
        else
        {
            if (! this instanceof DSLDAO )
                throw new RuntimeException("You must call getGeneratedKey or executeUpdate or getOneRowFromSingleField or getJEPLResultSet or getJEPLCachedResultSet")
            return null
        }
    }        
    
    def Object query(Closure userClosure)
    {       
        def closures = new DSLDALQueryPublicClosures()
        def results = new DSLDALQueryResults()
        fillQueryResultsAndCall(userClosure,closures,results)        
        
        JEPLDALQuery query = dal.createJEPLDALQuery( results.code )
        
        return queryInternal(query,userClosure,results)
    }
    

}


