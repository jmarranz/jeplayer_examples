
package example.jeplayer.groovydsl.dao

import example.jeplayer.groovydsl.model.Contact
import java.lang.reflect.Method
import jepl.JEPLColumnDesc
import jepl.JEPLConnection
import jepl.JEPLDAO
import jepl.JEPLNonJTADataSource
import jepl.JEPLResultSet
import jepl.JEPLResultSetDAOBeanMapper
import jepl.JEPLResultSetDAOListener
import jepl.JEPLTask
import jepl.JEPLUpdateDAOBeanMapper
import jepl.JEPLUpdateDAOListener
import jepl.JEPLPersistAction
import jepl.JEPLResultSetDALListener
import jepl.JEPLResultSetDAO
import jepl.JEPLPreparedStatement
import jepl.JEPLPreparedStatementListener
import jepl.groovy.dsl.DSLDAL
import jepl.groovy.dsl.DSLDAO

/**
 *
 * @author jmarranz
 */
class ContactDAO 
{
    def private dslDAO
    def private updateListener;    
    def private resultSetListener;    
    
    def ContactDAO(JEPLNonJTADataSource jds,short mappingMode)
    {
        def dao = jds.createJEPLDAO()
        this.dslDAO = new DSLDAO(dao)   
        
        // This 3 mapping approaches provides the same behaviour in this simple example, they are coded 
        // just to show the different options for mapping         
        switch(mappingMode)
        {
            case 0: // default mapping attribs and columns by name ignoring case
                this.updateListener = jds.createJEPLUpdateDAOListenerDefault(Contact.class)
                this.resultSetListener = jds.createJEPLResultSetDAOListenerDefault(Contact.class)
                break
            case 1: // custom mapping 
                this.updateListener = 
                [
                    getTable : { JEPLConnection jcon, Contact obj -> "CONTACT" 
                    },
                    getColumnDescAndValues : { JEPLConnection jcon, Contact obj, JEPLPersistAction action ->
                            Map.Entry[] result = 
                            [
                                new AbstractMap.SimpleEntry(new JEPLColumnDesc("ID").setAutoIncrement(true).setPrimaryKey(true),obj.id),
                                new AbstractMap.SimpleEntry(new JEPLColumnDesc("NAME"),obj.name),                    
                                new AbstractMap.SimpleEntry(new JEPLColumnDesc("PHONE"),obj.phone),                    
                                new AbstractMap.SimpleEntry(new JEPLColumnDesc("EMAIL"),obj.email)                    
                            ]
                            return result                        
                    }
                ] as JEPLUpdateDAOListener<Contact>
                

                this.resultSetListener = 
                [
                    setupJEPLResultSet : { JEPLResultSet jrs,JEPLTask<?> task ->  },
                    createObject : { JEPLResultSet jrs -> new Contact() },
                    fillObject : { Contact obj,JEPLResultSet jrs ->
                        def rs = jrs.getResultSet()
                        obj.id = rs.getInt("ID")
                        obj.name = rs.getString("NAME")
                        obj.phone = rs.getString("PHONE")
                        obj.email = rs.getString("EMAIL")
                    }
                ] as JEPLResultSetDAOListener<Contact>

                break
            case 2:  // default mapping using custom row-mappers              
                def updateMapper = { Contact obj, JEPLConnection jcon, String columnName, Method getter, JEPLPersistAction action ->
                        if (columnName.equalsIgnoreCase("email")) {
                            return obj.email
                        }
                        return JEPLUpdateDAOBeanMapper.NO_VALUE                    
                } as JEPLUpdateDAOBeanMapper<Contact> // Object getColumnFromBean(Contact obj, JEPLConnection jcon, String columnName, Method getter, JEPLPersistAction action) throws Exception
                                   
                this.updateListener = jds.createJEPLUpdateDAOListenerDefault(Contact.class,updateMapper)
                    
                def resultMapper = { Contact obj,JEPLResultSet jrs,int col,String columnName,Object value,Method setter ->
                        if (columnName.equalsIgnoreCase("email")) {
                            obj.email = (String)value
                            return true
                        }
                        return false   
                } as JEPLResultSetDAOBeanMapper<Contact> // boolean setColumnInBean(Contact obj,JEPLResultSet jrs,int col,String columnName,Object value,Method setter)
                
                this.resultSetListener = jds.createJEPLResultSetDAOListenerDefault(Contact.class,resultMapper)                
                break
            case 3:
                throw new RuntimeException("Unexpected")
        }
          
        dao.addJEPLListener updateListener        
        dao.addJEPLListener resultSetListener                 
    }
    
    def getDSLDAO()
    {
        return dslDAO
    }
        
    def insert(Contact contact)
    {        
        int key = dslDAO.query
        {
            code "INSERT INTO CONTACT (EMAIL, NAME, PHONE) VALUES (?, ?, ?)"    
            params contact.email,contact.name,contact.phone    
            getGeneratedKey(int.class)
        }         
        contact.id = key       
    }     	                    
    
    def insertImplicitUpdateListener(Contact contact)
    {
        int key = dslDAO.insert(contact)
        {    
            getGeneratedKey(int.class)
        }         
        contact.id = key          
    }        
    
    def insertExplicitResultSetListener(contact)
    {
        // Just to show how data conversion can be possible if required
        
        def resListener = 
            [ 
                setupJEPLResultSet : { JEPLResultSet jrs,JEPLTask<?> task -> 
                },
                getValue : { int columnIndex, Class returnType, JEPLResultSet jrs -> 
                    if (!returnType.equals(int.class)) throw new RuntimeException("UNEXPECTED")
                    // Expected columnIndex = 1 (only one row and one column is expected)
                    def rs = jrs.getResultSet()
                    def resInt = rs.getInt(columnIndex)
                    def resObj = rs.getObject(columnIndex)
                    def resIntObj = (Integer)jrs.getJEPLStatement().getJEPLDAL().cast(resObj, returnType)
                    if (resInt != resIntObj) throw new RuntimeException("UNEXPECTED")
                    return resIntObj
                }
            ] as JEPLResultSetDALListener

        int key = dslDAO.query
        {
            code "INSERT INTO CONTACT (EMAIL, NAME, PHONE) VALUES (?, ?, ?)"    
            params contact.email,contact.name,contact.phone    
            listener resListener
            // You can add here more JEPLListener listeners just adding: listener otherListener
            getGeneratedKey(int.class)
        }         
        contact.id = key            
    }            
         
    def insertUsingNamedParams(contact)
    {
        int key = dslDAO.query
        {
            code "INSERT INTO CONTACT (EMAIL, NAME, PHONE) VALUES (:email,:name,:phone)"    
            params email:contact.email,name:contact.name,phone:contact.phone    
            getGeneratedKey(int.class)
        }         
        contact.id = key        
    }     
    
    def insertUsingNumberedParams(contact)
    {
        int key = dslDAO.query
        {
            code "INSERT INTO CONTACT (EMAIL, NAME, PHONE) VALUES (?1,?2,?3)"    
            params 1:contact.email,2:contact.name,3:contact.phone    
            getGeneratedKey(int.class)
        }         
        contact.id = key        
    }         
    
    def update(Contact contact)
    {
        int updated = dslDAO.query
        {
            code "UPDATE CONTACT SET EMAIL = ?, NAME = ?, PHONE = ? WHERE ID = ?"    
            params contact.email,contact.name,contact.phone,contact.id    
            executeUpdate()
        }    

        return updated > 0         
    }        
    
    def updateImplicitUpdateListener(contact)
    {
        def updated = dslDAO.update(contact)
        {    
            executeUpdate()
        }         
        return updated > 0       
    }           
    
    def delete(contact)
    {
        return deleteById(contact.id)
    }
    
    def deleteById(id)
    {
        def deleted = dslDAO.query
        {
            code "DELETE FROM CONTACT WHERE ID = ?"   
            params id    
            executeUpdate()
        }          
        return deleted > 0
    }    
    
    def deleteImplicitUpdateListener(contact)
    {
        def deleted = dslDAO.delete(contact)
        {    
            executeUpdate()
        }         
        return deleted > 0        
    }    
    
    def deleteAll()
    {
        def deleted = dslDAO.query
        {
            code "DELETE FROM CONTACT"   
            executeUpdate()
        }          
        return deleted > 0               
    }        
    
    def selectActiveResult() // JEPLResultSetDAO<Contact>
    {
        def result = dslDAO.query
        {
            code "SELECT * FROM CONTACT"   
            getJEPLResultSetDAO()
        }  
        return result
    }                 
    
    def static toContactArray(JEPLResultSetDAO list) // Contact[]
    {
        if (list.isClosed()) throw new RuntimeException("Unexpected")
        def size = list.size()
        def res = list.toArray(new Contact[size]) // JEPLResultSetDAO implements List interface
        if (!list.isClosed()) throw new RuntimeException("Unexpected")
        return res
    }          
    
    def selectNotActiveResult() // List<Contact>
    {
        def result = dslDAO.query
        {
            code "SELECT * FROM CONTACT ORDER BY ID"   
            getResultList()
        }  
        return result        
    }        
    
    def selectNotActiveResult(maxNumResults) // List<Contact>
    {
        def result = dslDAO.query
        {
            code "SELECT * FROM CONTACT ORDER BY ID"   
            maxResults maxNumResults
            getResultList()
        }  
        return result
    }       
    
    def selectNotActiveResult2(maxNumResults)
    {
        // Another (verbose) approach using JDBC        
        def stmtListener = { JEPLPreparedStatement jstmt,JEPLTask<List<Contact>> task ->  // setupJEPLPreparedStatement(JEPLPreparedStatement jstmt,JEPLTask<List<Contact>> task) throws Exception
                def stmt = jstmt.getPreparedStatement()
                def old = stmt.getMaxRows()
                stmt.setMaxRows(maxNumResults)
                try
                {
                    List<Contact> res = task.exec()
                }
                finally
                {
                    stmt.setMaxRows(old) // Restore
                }                        
                        
        } as JEPLPreparedStatementListener<List<Contact>>;

        def result = dslDAO.query
        {
            code "SELECT * FROM CONTACT"  
            listener stmtListener
            maxResults maxNumResults
            getResultList()
        }  
        return result        
    }    

    def selectCount()  // selectCount method name is used instead "count" to avoid name clashing with jooq "org.jooq.impl.DSL.count()"
    {
        def count = dslDAO.query
        {
            code "SELECT COUNT(*) FROM CONTACT"   
            getOneRowFromSingleField(int.class)
        }  
        return count            
    }    
    
    def selectRange(from,to) // List<Contact>
    {
        // The simplest form
        
        def result = dslDAO.query
        {
            code "SELECT * FROM CONTACT ORDER BY ID"  
            firstResult from
            maxResults  (to - from)
            getResultList()
        }  
        return result        
    }    
    
    def selectRange2(from,to) // List<Contact>
    {
        // Another (verbose) approach using JDBC
        def resListener = 
        [
            setupJEPLResultSet: { JEPLResultSet jrs,JEPLTask<?> task -> 
                resultSetListener.setupJEPLResultSet(jrs, task)
                
                def rs = jrs.getResultSet()
                rs.absolute(from) // Is not still closed

                // Not needed, just to know the state of ResultSet and some demostrative check:
                def res = task.exec() // List<Contact>
                if (res.size() > to - from) throw new RuntimeException("Unexpected")            
            }, 
            createObject: { JEPLResultSet jrs ->
                resultSetListener.createObject(jrs)
            },
            fillObject : { Contact obj,JEPLResultSet jrs ->
                resultSetListener.fillObject(obj, jrs)
                
                def rs = jrs.getResultSet()
                def row = rs.getRow() 
                if (row + 1 == to)
                    jrs.stop()                
            }                     
               
        ] as JEPLResultSetDAOListener<Contact>
        

        def result = dslDAO.query
        {
            code "SELECT * FROM CONTACT ORDER BY ID"  
            listener resListener
            getResultList()
        }  
        return result         
    }    
    
    
}

