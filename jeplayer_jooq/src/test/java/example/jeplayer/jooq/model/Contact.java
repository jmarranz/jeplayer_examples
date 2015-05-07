
package example.jeplayer.jooq.model;

public class Contact
{
    protected int id;
    protected String name;
    protected String phone;
    protected String email;
    protected String notPersisAttr;
    
    public Contact(int id, String name, String phone, String email)
    {
        this.id = id;
        this.name = name;
        this.phone = phone;
        this.email = email;
    }

    public Contact()
    {
    }

    public String getEmail()
    {
        return email;
    }

    public void setEmail(String email)
    {
        this.email = email;
    }

    public int getId()
    {
        return id;
    }

    public void setId(int id)
    {
        this.id = id;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getPhone()
    {
        return phone;
    }

    public void setPhone(String phone)
    {
        this.phone = phone;
    }

    public String getNotPersisAttr() 
    {
        return notPersisAttr;
    }

    public void setNotPersisAttr(String notPersisAttr) 
    {
        this.notPersisAttr = notPersisAttr;
    }    
}
