
package example.jeplayer.groovydsl.dsl

import jepl.JEPLDAL
import jepl.JEPLDALQuery

public class DSLDAL
{
    JEPLDAL dal;
    
    private class DSLDALQueryClosures
    {
        Closure code
        Closure params
        Closure generatedKey
        Closure executeUpdate        
    }
    
    private class DSLDALQueryResults
    {
        String code = ""
        Object[] params = null
        Class generatedKeyClass = null
        boolean executeUpdate = false        
    }    
    
    def query(Closure c)
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

        JEPLDALQuery query = dal.createJEPLDALQuery( results.code )
        
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
}


