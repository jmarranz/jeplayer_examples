
package jepl.groovy.dsl

import jepl.JEPLDALQuery
import jepl.JEPLDAO
import jepl.JEPLDAOQuery
import jepl.impl.JEPLDAOImpl
import jepl.impl.groovy.dsl.DSLDALQueryClosures
import jepl.impl.groovy.dsl.DSLDALQueryResults

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
    
    def Object insert(Object obj,Closure c)
    {      
        JEPLDAOQuery query = getJEPLDAO().insert( obj )         
        
        DSLDALQueryResults results = callQueryResults(c)     
        
        return queryInternal(query,results)
    } 
}

