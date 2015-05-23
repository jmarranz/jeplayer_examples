package example.jeplayer.jooqgroovy.dao

import example.jeplayer.jooqgroovy.model.Contact
import java.lang.reflect.Method
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.AbstractMap
import java.util.List
import java.util.Map
import jepl.JEPLColumnDesc
import jepl.JEPLConnection
import jepl.JEPLDAO
import jepl.JEPLNonJTADataSource
import jepl.JEPLPersistAction
import jepl.JEPLPreparedStatement
import jepl.JEPLPreparedStatementListener
import jepl.JEPLResultSet
import jepl.JEPLResultSetDALListener
import jepl.JEPLResultSetDAO
import jepl.JEPLResultSetDAOBeanMapper
import jepl.JEPLResultSetDAOListener
import jepl.JEPLTask
import jepl.JEPLUpdateDAOBeanMapper
import jepl.JEPLUpdateDAOListener
import static org.jooq.impl.DSL.count
import static org.jooq.impl.DSL.field
import static org.jooq.impl.DSL.table
import org.jooq.impl.DefaultDSLContext

/**
 *  https://groups.google.com/forum/#!topic/jooq-user/2HTS-0DE1M8
 * 
 *  Because we're using JOOQ, binding params by name (provided by JEPLayer) are not used in this example
 * 
 * @author jmarranz
 */
class ContactDAO
{
    JEPLDAO<Contact> dao
    DefaultDSLContext jooqCtx
    JEPLUpdateDAOListener<Contact> updateListener
    JEPLResultSetDAOListener<Contact> resultSetListener
    
    ContactDAO(JEPLNonJTADataSource jds,DefaultDSLContext jooqCtx)
    {
        this(jds,jooqCtx,(short)0)        
    }
    
    ContactDAO(JEPLNonJTADataSource jds,DefaultDSLContext jooqCtx,short mappingMode)
    {
        this.dao = jds.createJEPLDAO(Contact.class) 
        this.jooqCtx = jooqCtx
        
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
                                new AbstractMap.SimpleEntry(new JEPLColumnDesc("ID").setAutoIncrement(true).setPrimaryKey(true),obj.getId()),
                                new AbstractMap.SimpleEntry(new JEPLColumnDesc("NAME"),obj.getName()),                    
                                new AbstractMap.SimpleEntry(new JEPLColumnDesc("PHONE"),obj.getPhone()),                    
                                new AbstractMap.SimpleEntry(new JEPLColumnDesc("EMAIL"),obj.getEmail())                    
                            ]
                            return result                        
                    }
                ] as JEPLUpdateDAOListener<Contact>
                

                this.resultSetListener = 
                [
                    setupJEPLResultSet : { JEPLResultSet jrs,JEPLTask<?> task ->                        
                    },
                    createObject : { JEPLResultSet jrs -> new Contact()                        
                    },
                    fillObject : { Contact obj,JEPLResultSet jrs ->
                        def rs = jrs.getResultSet()
                        obj.setId(rs.getInt("ID"))
                        obj.setName(rs.getString("NAME"))
                        obj.setPhone(rs.getString("PHONE"))
                        obj.setEmail(rs.getString("EMAIL"))                        
                    }
                ] as JEPLResultSetDAOListener<Contact>

                break
            case 2:  // default mapping using custom row-mappers              
                def updateMapper = { Contact obj, JEPLConnection jcon, String columnName, Method getter, JEPLPersistAction action ->
                        if (columnName.equalsIgnoreCase("email"))
                        {
                            return obj.getEmail()
                        }
                        return JEPLUpdateDAOBeanMapper.NO_VALUE                    
                } as JEPLUpdateDAOBeanMapper<Contact> // Object getColumnFromBean(Contact obj, JEPLConnection jcon, String columnName, Method getter, JEPLPersistAction action) throws Exception
                                   
                this.updateListener = jds.createJEPLUpdateDAOListenerDefault(Contact.class,updateMapper)
                    
                def resultMapper = { Contact obj,JEPLResultSet jrs,int col,String columnName,Object value,Method setter ->
                        if (columnName.equalsIgnoreCase("email"))
                        {
                            obj.setEmail((String)value)
                            return true
                        }
                        return false                    
                } as JEPLResultSetDAOBeanMapper<Contact> // boolean setColumnInBean(Contact obj,JEPLResultSet jrs,int col,String columnName,Object value,Method setter)
                
                this.resultSetListener = jds.createJEPLResultSetDAOListenerDefault(Contact.class,resultMapper)                
                break
            case 3:
                throw new RuntimeException("Unexpected")
        }
          
        dao.addJEPLListener(updateListener)        
        dao.addJEPLListener(resultSetListener)          
    }    
    
    def getJEPLDAO()
    {
        return dao
    }
    
    def insert(contact)
    {
        def key = dao.createJEPLDALQuery(
                 jooqCtx.insertInto(table("CONTACT"),field("EMAIL"),field("NAME"),field("PHONE"))
                         .values("email","name", "phone").getSQL()) // INSERT INTO CONTACT (EMAIL, NAME, PHONE) VALUES (?, ?, ?)
                .addParameters(contact.getEmail(),contact.getName(),contact.getPhone())
                .getGeneratedKey(int.class)
        contact.setId(key)
    }     
    
    def insertImplicitUpdateListener(contact)
    {
        def key = dao.insert(contact).getGeneratedKey(int.class)
        contact.setId(key)
    }        
    
    def insertExplicitResultSetListener(contact)
    {
        // Just to show how data conversion can be possible if required
        
        def listener = 
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

        def key = dao.createJEPLDALQuery(
                    jooqCtx.insertInto(table("CONTACT"),field("EMAIL"),field("NAME"),field("PHONE"))
                            .values("email","name", "phone").getSQL()) // INSERT INTO CONTACT (EMAIL, NAME, PHONE) VALUES (?, ?, ?)                
                    .addParameters(contact.getEmail(),contact.getName(),contact.getPhone())
                    .addJEPLListener(listener)
                    .getGeneratedKey(int.class)
         contact.setId(key)
    }    
    
    def update(contact)
    {
        def updated = dao.createJEPLDALQuery(
                jooqCtx.update(table("CONTACT"))
                        .set(field("EMAIL"), "email")
                        .set(field("NAME"),  "name")
                        .set(field("PHONE"), "phone")
                        .where(field("ID").equal(0)).getSQL()) // "UPDATE CONTACT SET EMAIL = ?, NAME = ?, PHONE = ? WHERE ID = ?")
                .addParameters(contact.getEmail(),contact.getName(),contact.getPhone(),contact.getId())
                .executeUpdate()
        return updated > 0
    }    
    
    def updateImplicitUpdateListener(contact)
    {
        def updated = dao.update(contact).executeUpdate()
        return updated > 0
    }        
    
    def delete(contact)
    {
        return deleteById(contact.getId())
    }
    
    def deleteById(id)
    {
        def deleted = dao.createJEPLDALQuery( jooqCtx.delete(table("CONTACT")).where(field("ID").equal(0)).getSQL() ) // "DELETE FROM CONTACT WHERE ID = ?" 
                        .addParameters(id)             
                        .executeUpdate()     
        return deleted > 0
    }    
    
    def deleteImplicitUpdateListener(contact)
    {
        def deleted = dao.delete( contact ).executeUpdate()     
        return deleted > 0
    }    
    
    def deleteAll()
    {
        return dao.createJEPLDALQuery( jooqCtx.delete(table("CONTACT")).getSQL() ) // "DELETE FROM CONTACT" 
                            .executeUpdate()      
    }      
    
    def selectActiveResult() // JEPLResultSetDAO<Contact>
    {
        return dao.createJEPLDAOQuery( jooqCtx.selectFrom(table("CONTACT")).getSQL() ) // "SELECT * FROM CONTACT"
                    .getJEPLResultSetDAO()    
    }                 
    
    def static toContactArray(JEPLResultSetDAO<Contact> list) // Contact[]
    {
        if (list.isClosed()) throw new RuntimeException("Unexpected")
        def size = list.size()
        def res = ((List<Contact>)list).toArray(new Contact[size]) // JEPLResultSetDAO implements List interface
        if (!list.isClosed()) throw new RuntimeException("Unexpected")
        return res
    }         
    
    def selectNotActiveResult() // List<Contact>
    {
        // "ORDER BY ID" is not really needed, is just to play with jooq
        def list = dao.createJEPLDAOQuery( jooqCtx.selectFrom(table("CONTACT")).orderBy(field("ID")).getSQL() ) // "SELECT * FROM CONTACT ORDER BY ID"
                        .getResultList()       
        return list
    }            
    
    def selectNotActiveResult(maxResults) // List<Contact>
    {
        // "ORDER BY" is not really needed, is just to play with jooq
        def list = dao.createJEPLDAOQuery( jooqCtx.selectFrom(table("CONTACT")).orderBy(field("ID"),field("NAME")).getSQL() ) // "SELECT * FROM CONTACT ORDER BY ID,NAME"
                .setMaxResults(maxResults)
                .getResultList()       
        return list
    }             
    
    def selectNotActiveResult2(maxResults)
    {
        // Another (verbose) approach using JDBC        
        def listener = { JEPLPreparedStatement jstmt,JEPLTask<List<Contact>> task ->  // setupJEPLPreparedStatement(JEPLPreparedStatement jstmt,JEPLTask<List<Contact>> task) throws Exception
                def stmt = jstmt.getPreparedStatement()
                def old = stmt.getMaxRows()
                stmt.setMaxRows(maxResults)
                try
                {
                    List<Contact> res = task.exec()
                }
                finally
                {
                    stmt.setMaxRows(old) // Restore
                }                        
                        
        } as JEPLPreparedStatementListener<List<Contact>>;    

        return dao.createJEPLDAOQuery(jooqCtx.selectFrom(table("CONTACT")).getSQL())
                .addJEPLListener(listener)
                .getResultList()
    }    
    
    def selectCount()  // selectCount method name is used instead "count" to avoid name clashing with jooq "org.jooq.impl.DSL.count()"
    {
        return dao.createJEPLDALQuery( jooqCtx.select(count()).from(table("CONTACT")).getSQL() )
                            .getOneRowFromSingleField(int.class)     
    }
    
    def selectRange(from,to) // List<Contact>
    {
        // The simplest form
        return dao.createJEPLDAOQuery(jooqCtx.selectFrom(table("CONTACT")).orderBy(field("ID")).getSQL() ) // "SELECT * FROM CONTACT ORDER BY ID"
                .setFirstResult(from)
                .setMaxResults(to - from)
                .getResultList()
    }    
    
    public def selectRange2(from,to) // List<Contact>
    {
        // Another (verbose) approach using JDBC
        def listener = 
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
        

        return dao.createJEPLDAOQuery(jooqCtx.selectFrom(table("CONTACT")).orderBy(field("ID")).getSQL() ) // "SELECT * FROM CONTACT ORDER BY ID"
                .addJEPLListener(listener)
                .getResultList()
    }    
}
