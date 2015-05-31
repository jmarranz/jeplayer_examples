
package example.jeplayer.groovydsl

import com.mchange.v2.c3p0.ComboPooledDataSource
import com.mchange.v2.c3p0.DataSources
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
import static org.junit.Assert.assertFalse
import static org.junit.Assert.assertTrue
import example.jeplayer.groovydsl.dsl.DSLTest
//import example.jeplayer.groovydsl.dsl.DSLDAL

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
            
            createTables(jds)
                
            def contact1 = new Contact()
            contact1.name = "One Contact"
            contact1.phone = "1111111"
            contact1.email = "contactOne@world.com"            
            DSLTest.exec(jds,contact1)
      
            
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
}
