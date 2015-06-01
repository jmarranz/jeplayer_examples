
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
    
    def insert(Contact contact)
    {        
        int key = dslDAO.query
        {
            code "INSERT INTO CONTACT (EMAIL, NAME, PHONE) VALUES (?, ?, ?)"    
            params contact.email,contact.name,contact.phone    
            generatedKey(int.class)
        }         
        contact.id = key       
    }     	
    
    def insertImplicitUpdateListener(Contact contact)
    {
        int key = dslDAO.insert(contact)
        {    
            generatedKey(int.class)
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
}

