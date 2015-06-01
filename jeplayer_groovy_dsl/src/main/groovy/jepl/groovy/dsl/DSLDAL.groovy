
package jepl.groovy.dsl

import jepl.JEPLDAL
import jepl.JEPLDALQuery
import jepl.impl.groovy.dsl.DSLDALQueryClosures
import jepl.impl.groovy.dsl.DSLDALQueryResults

class DSLDAL
{
    JEPLDAL dal
    
    def DSLDAL(JEPLDAL dal)
    {
        this.dal = dal
    }

    def protected DSLDALQueryResults callQueryResults(Closure c)
    {
        DSLDALQueryResults results = new DSLDALQueryResults()      
        
        Closure codeMethod = { String code -> results.code = code }
        Closure paramsMethod = { Object[] params -> results.params = params }
        Closure generatedKeyMethod = { Class generatedKeyClass -> results.generatedKeyClass = generatedKeyClass }
        Closure executeUpdateMethod = { results.executeUpdate = true }        
        
        DSLDALQueryClosures closures = new DSLDALQueryClosures( 
            code: codeMethod, params: paramsMethod, generatedKey : generatedKeyMethod, executeUpdate : executeUpdateMethod 
        )
       
        /*
        closures.code = codeMethod   //closures.setProperty("code",codeMethod)
        closures.params = paramsMethod
        closures.generatedKey = generatedKeyMethod
        closures.executeUpdate = executeUpdateMethod
        */        
        
        c.delegate = closures
        
        c.call()    
        
        return results
    }

    def protected Object queryInternal(JEPLDALQuery query,DSLDALQueryResults results)
    {              
        if (results.params != null)
            query.addParameters( results.params )
        
        if (results.generatedKeyClass != null)
        {
            return query.getGeneratedKey( results.generatedKeyClass )
        }
        else if (results.executeUpdate)
        {
            int update = query.executeUpdate()
            return update
        }
        else
        {
            throw new RuntimeException("You must call generatedKey or executeUpdate");
        }
    }        
    
    def Object query(Closure c)
    {       
        DSLDALQueryResults results = callQueryResults(c)

        JEPLDALQuery query = dal.createJEPLDALQuery( results.code )
        
        return queryInternal(query,results)
    }
    

}


