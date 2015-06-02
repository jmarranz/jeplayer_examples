
package example.jeplayer.groovyjooq

import com.mchange.v2.c3p0.ComboPooledDataSource
import com.mchange.v2.c3p0.DataSources
import example.jeplayer.groovyjooq.model.Contact
import example.jeplayer.groovyjooq.dao.ContactDAO
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
import org.jooq.SQLDialect
import static org.jooq.impl.DSL.avg
import static org.jooq.impl.DSL.count
import static org.jooq.impl.DSL.field
import static org.jooq.impl.DSL.table
import org.jooq.impl.DefaultDSLContext
import static org.junit.Assert.assertFalse
import static org.junit.Assert.assertTrue



/**
 *
 * @author jmarranz
 */
class GroovyJooqExample 
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

            def jooqCtx = new DefaultDSLContext(SQLDialect.MYSQL)
            
            def jds = JEPLBootRoot.get().createJEPLBootNonJTA().createJEPLNonJTADataSource(ds)
            def conListener = { JEPLConnection con,JEPLTask task -> 
                                con.getConnection().setAutoCommit(true)
                              } as JEPLConnectionListener  // public void setupJEPLConnection(JEPLConnection con,JEPLTask task) throws Exception

            jds.addJEPLListener(conListener) // Simple alternative:  jds.setDefaultAutoCommit(true)
            
            createTables(jds)
                        
            
            def dao = new ContactDAO(jds,jooqCtx,mappingMode)
            
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
                        
            contact3.phone = "4444444"
            def updated = dao.update(contact3)
            assertTrue(updated)            
            
            contact3.phone = "3333333"
            updated = dao.updateImplicitUpdateListener(contact3) // just to play
            assertTrue(updated)          
            
            def task = { ->  
                    // Connection got
                    def list = dao.selectActiveResult() // JEPLResultSetDAO<Contact>
                    assertFalse(list.isClosed())                
                    def res = ContactDAO.toContactArray(list)
                    assertTrue(list.isClosed())
                    def size2 = dao.selectCount()
                    assertTrue(res.length == size2)
                    return res
                    // Connection released
            } as JEPLTask<Contact[]> // Contact[] exec()
            

            def array = dao.getJEPLDAO().getJEPLDataSource().exec(task) // Contact[]                                   
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
  
            
            def dal = jds.createJEPLDAL()
            
            dalActiveSelect(dal,jooqCtx)
            
            dalNotActiveSelect(dal,jooqCtx)
            
            jdbcTxnExample(dao)            
         
            jdbcTxnExample2(dao)             
            
            jdbcTxnExample3(dao) 
            
            jdbcTxnExample4(dao)            
            
            jdbcTxnExample5(dao,true)
            
            jdbcTxnExample5(dao,false)            
        }  
        finally
        {
            destroyDataSource(ds)
        }
        
    }

  
    def configureDataSource(ComboPooledDataSource cpds) throws PropertyVetoException
    {
        // Create before a database named "testjooq"
        def jdbcXADriver = "com.mysql.jdbc.jdbc2.optional.MysqlXADataSource"
        def jdbcURL="jdbc:mysql://127.0.0.1:3306/testjeplayer?pinGlobalTxToPhysicalConnection=true" // testjooq
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
       
    
    def createTables(JEPLNonJTADataSource jds)
    {
        def dal = jds.createJEPLDAL()  
              
        dal.createJEPLDALQuery("DROP TABLE IF EXISTS CONTACT").executeUpdate()

        dal.createJEPLDALQuery('''
            CREATE TABLE CONTACT (
              ID INT NOT NULL AUTO_INCREMENT,
              EMAIL VARCHAR(255) NOT NULL,
              NAME VARCHAR(255) NOT NULL,
              PHONE VARCHAR(255) NOT NULL,
              PRIMARY KEY (ID)
             ) 
            ENGINE=InnoDB
            '''
        ).executeUpdate() 
    }
           
 
    def dalActiveSelect(JEPLDAL dal,DefaultDSLContext jooqCtx)
    {          
        // Supposed 3 rows in contact table
        def task = {   // Void exec() throws Exception
        
            def resSet = dal.createJEPLDALQuery(
                    jooqCtx.select(count().as("CO"),avg(field("ID",int.class)).as("AV")).from(table("CONTACT")).getSQL()) // SELECT COUNT(*) AS CO,AVG(ID) AS AV FROM CONTACT  
                    .getJEPLResultSet()

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
            assertTrue(count == 3)       
            count = rs.getInt("CO")
            assertTrue(count == 3)

            float avg = rs.getFloat(1)
            assertTrue(avg > 0)        
            avg = rs.getFloat("AV")
            assertTrue(avg > 0)                       

            assertFalse(resSet.next())                
            assertTrue(resSet.isClosed())                

            assertTrue(resSet.count() == 1) 
            return null            
        
        } as JEPLTask<Void> 

        dal.getJEPLDataSource().exec(task)
    }        
    
    def dalNotActiveSelect(JEPLDAL dal,DefaultDSLContext jooqCtx)
    {          
        // Supposed 3 rows in contact table
        def resSet = dal.createJEPLDALQuery(
                jooqCtx.select(count().as("CO"),avg(field("ID",int.class)).as("AV")).from(table("CONTACT")).getSQL()) // SELECT COUNT(*) AS CO,AVG(ID) AS AV FROM CONTACT     
                .getJEPLCachedResultSet()
        def colNames = resSet.getColumnLabels() // String[]
        assertTrue(colNames.length == 2)
        assertTrue(colNames[0].equals("CO"))
        assertTrue(colNames[1].equals("AV"))
        
        assertTrue(resSet.size() == 1)

        def count = resSet.getValue(1, 1, int.class) // Row 1, column 1
        assertTrue(count == 3)
        count = resSet.getValue(1, "CO", int.class)
        assertTrue(count == 3)

        def avg = resSet.getValue(1, 2, float.class) // Row 1, column 2
        assertTrue(avg > 0)
        avg = resSet.getValue(1, "AV", float.class)
        assertTrue(avg > 0)
    }        
        
  
    def static jdbcTxnExample(ContactDAO dao)
    {
        checkNotEmpty(dao)      
        
        def contacts = dao.selectNotActiveResult()   // List<Contact>     
        
        def task = { 
            contacts.each({ contact -> 
                            def deleted = dao.delete(contact)
                            assertTrue(deleted)       
                          })
                // No, no, we need a rollback
                throw new Exception("I want a rollback to avoid to delete rows")
        } as JEPLTask // Void exec() throws Exception
        
        try
        {
            def jds = dao.getJEPLDAO().getJEPLDataSource() // JEPLNonJTADataSource
            def autoCommit = false
            jds.exec(task,autoCommit) 
            
            throw new RuntimeException("Unexpected, ever executed rollback in this example")
        }
        catch(Exception ex)
        {
            // Rollback, data is still saved
            checkNotEmpty(dao)
        }
    }            
    
    def static jdbcTxnExample2(ContactDAO dao)
    {
        checkNotEmpty(dao)
        
        def contacts = dao.selectNotActiveResult()  // List<Contact>       
        
        def task = { 
            contacts.each({ contact -> 
                            def deleted = dao.deleteImplicitUpdateListener(contact) // just to play
                            assertTrue(deleted)       
                          })
                // No, no, we need a rollback
                throw new Exception("I want a rollback to avoid to delete rows")
        } as JEPLTask // Void exec() throws Exception        
        
        
        def conListener = { JEPLConnection con,JEPLTask task2 -> 
                            con.getConnection().setAutoCommit(false)
                          } as JEPLConnectionListener  // void setupJEPLConnection(JEPLConnection con,JEPLTask task2) throws Exception

        
        try
        {
            dao.getJEPLDAO().getJEPLDataSource().exec(task,conListener)
            
            throw new RuntimeException("Unexpected, ever executed rollback in this example")
        }
        catch(Exception ex)
        {
            // Rollback, data is still saved
            checkNotEmpty(dao)
        }
    }            
          
    def static jdbcTxnExample3(ContactDAO dao)
    {
        checkNotEmpty(dao)     
        
        def contacts = dao.selectNotActiveResult() // List<Contact> 
        
        def task = { 
            contacts.each({ contact -> 
                            def deleted = dao.delete(contact)
                            assertTrue(deleted)       
                          })
                // No, no, we need a rollback
                throw new Exception("I want a rollback to avoid to delete rows")
        } as JEPLTask // Void exec() throws Exception
        
        def connListener = { JEPLConnection con,JEPLTask task2 -> 
                                con.getConnection().setAutoCommit(false) // transaction
                                try
                                {
                                    task2.exec()
                                    con.getConnection().commit()
                                }
                                catch(Exception ex)
                                {
                                    con.getConnection().rollback()
                                    throw new SQLException(ex)
                                }
                          } as JEPLConnectionListener  // void setupJEPLConnection(JEPLConnection con,JEPLTask task2) throws Exception        
        
        try
        {
            dao.getJEPLDAO().getJEPLDataSource().exec(task,connListener) 
            
            throw new RuntimeException("Unexpected, ever executed rollback in this example")
        }
        catch(Exception ex)
        {
            // Rollback, data is still saved
            checkNotEmpty(dao)
        }
    }
       
    
    def static jdbcTxnExample4(ContactDAO dao)
    {
        checkNotEmpty(dao)      
        
        def contacts = dao.selectNotActiveResult() // List<Contact>
        
        def task = { 
            contacts.each({ contact -> 
                            def deleted = dao.delete(contact)
                            assertTrue(deleted)       
                          })
                // No, no, we need a rollback
                throw new Exception("I want a rollback to avoid to delete rows")
        } as JEPLTask // Void exec() throws Exception
        
        def connListener = { JEPLConnection con,JEPLTask task2 -> 
                                JEPLTransaction txn = con.getJEPLTransaction()
                                txn.begin() // Executes setDefaultAutoCommit(false)

                                try
                                {
                                    task2.exec()
                                    txn.commit()
                                }
                                catch(Exception ex)
                                {
                                    txn.rollback()
                                    throw ex
                                }
                          } as JEPLConnectionListener  // void setupJEPLConnection(JEPLConnection con,JEPLTask task2) throws Exception         
        
        try
        {
            JEPLNonJTADataSource jds = (JEPLNonJTADataSource)dao.getJEPLDAO().getJEPLDataSource()
            jds.exec(task,connListener) 
            
            throw new RuntimeException("Unexpected, ever executed rollback in this example")
        }
        catch(Exception ex)
        {
            // Rollback, data is still saved
            checkNotEmpty(dao)
        }
    }    
        
    def static jdbcTxnExample5(ContactDAO dao,simulateRollback)
    {
        checkNotEmpty(dao)              
        
        JEPLTask<Void> task = new JEPLTask<Void>() {

            @Override
            @JEPLTransactionalNonJTA  // Is equals to @JEPLTransactionalNonJTA(autoCommit = false)
            public Void exec() throws SQLException
            { // Connection got
                def list = dao.selectActiveResult()  // JEPLResultSetDAO<Contact>                
                assertFalse(list.isClosed())

                // JEPLResultSetDAO also implements the List interface
                list.each({contact -> 
                        def deleted = dao.delete(contact)
                        assertTrue(deleted)

                        if (simulateRollback)
                            throw new RuntimeException("Force Rollback")
                    })
                
                assertTrue(list.isClosed())
                
                checkEmpty(dao)
                
                return null
            } // Connection released
        }
        
        try
        {
            dao.getJEPLDAO().getJEPLDataSource().exec(task)
            
            checkEmpty(dao)
        }
        catch(Exception ex)
        {
            // Rollback when simulateFailed = true
            checkNotEmpty(dao)
        }
    }    
    
    def static checkEmpty(ContactDAO dao)
    {            
        assertTrue(dao.selectCount() == 0)           
    }
    
    def static checkNotEmpty(ContactDAO dao)
    {            
        assertFalse(dao.selectCount() == 0)         
    }    
    
}

