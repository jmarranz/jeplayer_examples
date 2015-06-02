
package jepl.groovy.dsl

import jepl.JEPLDALQuery
import jepl.JEPLDAO
import jepl.JEPLDAOQuery
import jepl.impl.groovy.dsl.DSLDALQueryPublicClosures
import jepl.impl.groovy.dsl.DSLDALQueryResults
import jepl.impl.groovy.dsl.DSLDAOQueryPublicClosures
import jepl.impl.groovy.dsl.DSLDAOQueryResults

/**
 *
 * @author jmarranz
 */
class DSLDAO extends DSLDAL
{
    def DSLDAO(JEPLDAO dao)
    {
        super(dao)
    }	

    JEPLDAO getJEPLDAO()
    {
        return dal
    }
        
    def protected fillQueryResults(DSLDALQueryPublicClosures closures,DSLDALQueryResults results)
    {      
        super.fillQueryResults(closures,results)
        
        Closure getJEPLResultSetDAOMethod = { results.getJEPLResultSetDAO = true }        
        Closure getResultListMethod = { results.getResultList = true }
        
        closures.getJEPLResultSetDAO = getJEPLResultSetDAOMethod
        closures.getResultList = getResultListMethod
    }    
    
    def protected Object queryInternal(JEPLDALQuery query,Closure userClosure,DSLDALQueryResults results)
    {             
        Object res = super.queryInternal(query,userClosure,results)
        
        if (res != null) return res        
        
        JEPLDAOQuery queryDAO = (JEPLDAOQuery)query;
        DSLDAOQueryResults resultsDAO = (DSLDAOQueryResults)results;
        
        if (resultsDAO.getJEPLResultSetDAO)
        {
            return queryDAO.getJEPLResultSetDAO() // JEPLResultSetDAO
        }  
        else if (resultsDAO.getResultList)
        {
            return queryDAO.getResultList() // List
        }                  
        else
        {
            throw new RuntimeException("You must call getGeneratedKey or executeUpdate or getOneRowFromSingleField or getJEPLResultSet or getJEPLCachedResultSet or getJEPLResultSetDAO or getResultList");                
        }
        
    }
    
    def Object query(Closure userClosure)
    {      
        def closures = new DSLDAOQueryPublicClosures()
        def results = new DSLDAOQueryResults()
        fillQueryResultsAndCall(userClosure,closures,results) 
        
        JEPLDAOQuery query = getJEPLDAO().createJEPLDAOQuery( results.code )
        
        return queryInternal(query,userClosure,results)
    }    
    
    def Object insert(Object obj,Closure userClosure)
    {      
        def closures = new DSLDAOQueryPublicClosures()
        def results = new DSLDAOQueryResults()
        fillQueryResultsAndCall(userClosure,closures,results)        
        
        JEPLDAOQuery query = getJEPLDAO().insert( obj )             
        
        return queryInternal(query,userClosure,results)
    } 
    
    def Object update(Object obj,Closure userClosure)
    {      
        def closures = new DSLDAOQueryPublicClosures()
        def results = new DSLDAOQueryResults()
        fillQueryResultsAndCall(userClosure,closures,results)            
        
        JEPLDAOQuery query = getJEPLDAO().update( obj )         
        
        return queryInternal(query,userClosure,results)
    } 
    
    def Object delete(Object obj,Closure userClosure)
    {      
        def closures = new DSLDAOQueryPublicClosures()
        def results = new DSLDAOQueryResults()
        fillQueryResultsAndCall(userClosure,closures,results)            
        
        JEPLDAOQuery query = getJEPLDAO().delete( obj )         
        
        return queryInternal(query,userClosure,results)
    }    
}

