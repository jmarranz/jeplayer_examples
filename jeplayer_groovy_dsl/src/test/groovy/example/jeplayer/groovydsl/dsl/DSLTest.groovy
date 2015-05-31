
package example.jeplayer.groovydsl.dsl

import static org.junit.Assert.assertTrue
import jepl.JEPLDAL
import jepl.JEPLNonJTADataSource

import example.jeplayer.groovydsl.model.Contact
    

def static exec(JEPLNonJTADataSource jds,Contact contact) 
{
    def dal = jds.createJEPLDAL()
    def dslDAL = new DSLDAL(dal:dal)
    
    int key = dslDAL.query
    {
        code "INSERT INTO CONTACT (EMAIL, NAME, PHONE) VALUES (?, ?, ?)"    
        params contact.email,contact.name,contact.phone    
        generatedKey(int.class)
    }         
    contact.id = key
    assertTrue key > 0

    int updated = dslDAL.query
    {
        code "UPDATE CONTACT SET EMAIL = ?, NAME = ?, PHONE = ? WHERE ID = ?"    
        params contact.email,contact.name,contact.phone,contact.id    
        executeUpdate()
    }    
 
    assertTrue updated > 0
}
    
