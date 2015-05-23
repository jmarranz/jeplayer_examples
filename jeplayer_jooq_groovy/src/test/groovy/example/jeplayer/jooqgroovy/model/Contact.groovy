
package example.jeplayer.jooqgroovy.model

class Contact
{
    int id
    String name
    String phone
    String email
    String notPersisAttr
    
    def Contact(id, name, phone, email)
    {
        this.id = id
        this.name = name
        this.phone = phone
        this.email = email
    }

    def Contact()
    {
    }
}
