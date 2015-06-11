
package example.jeplayer.groovydsl

import com.mchange.v2.c3p0.ComboPooledDataSource
import com.mchange.v2.c3p0.DataSources
import example.jeplayer.groovydsl.dao.ContactDAO
import example.jeplayer.groovydsl.model.Contact

import java.beans.PropertyVetoException
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.util.List
import jepl.JEPLBootRoot
import jepl.JEPLCachedResultSet
import jepl.JEPLConnection
import jepl.JEPLConnectionListener
import jepl.JEPLDAL
import jepl.JEPLNonJTADataSource
import jepl.JEPLResultSet
import jepl.JEPLResultSetDAO
import jepl.JEPLTask
import jepl.JEPLTransaction
import jepl.JEPLTransactionalNonJTA
import jepl.groovy.dsl.DSLDAL

import static org.junit.Assert.assertFalse
import static org.junit.Assert.assertTrue


/**
 *
 * @author jmarranz
 */
class GroovyDSLExample 
{
    def run() 
    {
        0..2.each({ i -> 
            println("Mapping mode:" + i)
            runInternal((short)i)            
            })
    }
    
     
    def runInternal(short mappingMode) 
    {
        def ds = new ComboPooledDataSource()      
        try
        {
            configureDataSource(ds)
            
            def jds = JEPLBootRoot.get().createJEPLBootNonJTA().createJEPLNonJTADataSource(ds)
            def conListener = { JEPLConnection con,JEPLTask task -> 
                                con.getConnection().setAutoCommit(true)
                              } as JEPLConnectionListener  // public void setupJEPLConnection(JEPLConnection con,JEPLTask task) throws Exception

            jds.addJEPLListener(conListener) // Simple alternative:  jds.setDefaultAutoCommit(true)
         
            def dal = jds.createJEPLDAL() 
            def dslDAL = new DSLDAL(dal)
         
            createTables(dslDAL)
                
            def dao = new ContactDAO(jds,mappingMode)
            
            def contact1 = new Contact()
            contact1.name = "One Contact"
            contact1.phone = "1111111"
            contact1.email = "contactOne@world.com"            
            dao.insert(contact1)            
            
            def contact2 = new Contact()            
            contact2.name = "Another Contact"
            contact2.phone = "2222222"
            contact2.email = "contactAnother@world.com"
            dao.insertImplicitUpdateListener(contact2)  // just to play            
            
            def contact3 = new Contact()            
            contact3.name = "And other Contact"
            contact3.phone = "3333333"
            contact3.email = "contactAndOther@world.com"
            dao.insertExplicitResultSetListener(contact3) // just to play            

            def contact4 = new Contact()            
            contact4.name = "One more Contact"
            contact4.phone = "4444444"
            contact4.email = "oneMoreContact@world.com"
            dao.insertUsingNamedParams(contact4) // just to play            
                        
            def contact5 = new Contact()            
            contact5.name = "No more Contact"
            contact5.phone = "5555555"
            contact5.email = "noMoreContact@world.com"
            dao.insertUsingNumberedParams(contact5) // just to play            
            
            
            contact3.phone = "4444444"
            def updated = dao.update(contact3)
            assertTrue(updated) 
            
            contact3.phone = "3333333"
            updated = dao.updateImplicitUpdateListener(contact3) // just to play
            assertTrue(updated)              
            

            def task = { ->  
                    // Connection got
                    JEPLResultSetDAO list = dao.selectActiveResult() // JEPLResultSetDAO<Contact>
                    assertFalse(list.isClosed())                
                    def res = ContactDAO.toContactArray(list)
                    assertTrue(list.isClosed())
                    def size2 = dao.selectCount()
                    assertTrue(res.length == size2)
                    return res
                    // Connection released
            } as JEPLTask<Contact[]> // Contact[] exec()
            

            def array = dao.getDSLDAO().getJEPLDAO().getJEPLDataSource().exec(task) // Contact[]                                   
            println("Result:")
            array.each( { contact -> println("  Contact: " + contact.id + " " + contact.name + " " + contact.phone) } )            

            def list = dao.selectNotActiveResult()  // List<Contact>          
            println("Result:")
            list.each( { contact -> println("  Contact: " + contact.id + " " + contact.name + " " + contact.phone) } )            
            
            def maxResults = 2
            list = dao.selectNotActiveResult(maxResults)
            assertTrue(list.size() == maxResults)            
            println("Result maxResults (" + maxResults + "):")
            list.each( { contact -> println("  Contact: " + contact.id + " " + contact.name + " " + contact.phone) } )            
  
            list = dao.selectNotActiveResult2(maxResults)
            assertTrue(list.size() == maxResults)              
            println("Result maxResults (" + maxResults + "):")            
            list.each( { contact -> println("  Contact: " + contact.id + " " + contact.name + " " + contact.phone) } )            
            
            def from = 1
            def to = 2            
            list = dao.selectRange(from,to)
            assertTrue(list.size() == (to - from))            
            println("Result from/to " + from + "/" + to + ":")
            list.each( { contact -> println("  Contact: " + contact.id + " " + contact.name + " " + contact.phone) } )            
            
            list = dao.selectRange2(from,to)
            assertTrue(list.size() == (to - from))             
            println("Result from/to " + from + "/" + to + ":")
            list.each( { contact -> println("  Contact: " + contact.id + " " + contact.name + " " + contact.phone) } )            
            
            dalActiveSelect(dslDAL,5)
            
            dalNotActiveSelect(dslDAL,5)            

            def deleted = dao.delete(contact1)
            assertTrue(deleted)
            
            deleted = dao.deleteImplicitUpdateListener(contact2)            
            assertTrue(deleted)            
            
            deleted = dao.deleteAll()
            assertTrue(deleted)            
        }  
        finally
        {
            destroyDataSource(ds)
        }
                
    }
    
    def configureDataSource(ComboPooledDataSource cpds) throws PropertyVetoException
    {
        // Create before a database named "testjeplayer"
        def jdbcXADriver = "com.mysql.jdbc.jdbc2.optional.MysqlXADataSource"
        def jdbcURL="jdbc:mysql://127.0.0.1:3306/testjeplayer?pinGlobalTxToPhysicalConnection=true" 
        def jdbcUserName="root"
        def jdbcPassword="root2000"        
        def poolSize = 3
        def maxStatements = 180

        cpds.setDriverClass(jdbcXADriver)            
        cpds.setJdbcUrl(jdbcURL)
        cpds.setUser(jdbcUserName)
        cpds.setPassword(jdbcPassword)

        cpds.setMaxPoolSize(poolSize)
        cpds.setMaxStatements(maxStatements)    
    }
    
    def destroyDataSource(ComboPooledDataSource cpds) 
    {
        DataSources.destroy(cpds)
        cpds.close()
    }    
       
    
    def createTables(DSLDAL dslDAL)
    {      
        dslDAL.query
        {
            code "DROP TABLE IF EXISTS CONTACT"
            executeUpdate()
        }

        dslDAL.query
        {
            code '''
                CREATE TABLE CONTACT (
                  ID INT NOT NULL AUTO_INCREMENT,
                  EMAIL VARCHAR(255) NOT NULL,
                  NAME VARCHAR(255) NOT NULL,
                  PHONE VARCHAR(255) NOT NULL,
                  PRIMARY KEY (ID)
                 ) 
                ENGINE=InnoDB
                '''
            executeUpdate()
        }        
    }        
    
    def dalActiveSelect(DSLDAL dslDAL,countExpected)
    {                
        // Supposed 3 rows in contact table
        def task = {   // Void exec() throws Exception
        
            def resSet = dslDAL.query
            {
                code "SELECT COUNT(*) AS CO,AVG(ID) AS AV FROM CONTACT"
                getJEPLResultSet()
            }               
                        
            
            assertFalse(resSet.isClosed())                

            def rs = resSet.getResultSet()
            def metadata = rs.getMetaData() // ResultSetMetaData
            def ncols = metadata.getColumnCount()
            def colNames = new String[ncols]
            colNames.eachWithIndex({ name, i -> colNames[i] = metadata.getColumnLabel(i + 1) })                 

            assertTrue(colNames.length == 2)
            assertTrue(colNames[0].equals("CO"))
            assertTrue(colNames[1].equals("AV"))

            assertTrue(rs.getRow() == 0)                 

            assertFalse(resSet.isClosed())

            resSet.next()

            assertTrue(rs.getRow() == 1)

            int count = rs.getInt(1)
            assertTrue(count == countExpected)       
            count = rs.getInt("CO")
            assertTrue(count == countExpected)

            float avg = rs.getFloat(1)
            assertTrue(avg > 0)        
            avg = rs.getFloat("AV")
            assertTrue(avg > 0)                       

            assertFalse(resSet.next())                
            assertTrue(resSet.isClosed())                

            assertTrue(resSet.count() == 1) 
            return null            
        
        } as JEPLTask<Void> 

        dslDAL.getJEPLDAL().getJEPLDataSource().exec(task)
    }        
    
    def dalNotActiveSelect(DSLDAL dslDAL,countExpected)
    {          
        // Supposed 3 rows in contact table
        def resSet = dslDAL.query
        {
            code "SELECT COUNT(*) AS CO,AVG(ID) AS AV FROM CONTACT"
            getJEPLCachedResultSet()
        }       
        
        def colNames = resSet.getColumnLabels() // String[]
        assertTrue(colNames.length == 2)
        assertTrue(colNames[0].equals("CO"))
        assertTrue(colNames[1].equals("AV"))
        
        assertTrue(resSet.size() == 1)

        def count = resSet.getValue(1, 1, int.class) // Row 1, column 1
        assertTrue(count == countExpected)
        count = resSet.getValue(1, "CO", int.class)
        assertTrue(count == countExpected)

        def avg = resSet.getValue(1, 2, float.class) // Row 1, column 2
        assertTrue(avg > 0)
        avg = resSet.getValue(1, "AV", float.class)
        assertTrue(avg > 0)
    }        
            
    
}
