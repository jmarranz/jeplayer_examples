package example.jeplayer.jooq;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.DataSources;
import example.jeplayer.jooq.dao.ContactDAO;
import example.jeplayer.jooq.model.Contact;
import java.beans.PropertyVetoException;
import java.sql.SQLException;
import java.util.List;
import jepl.JEPLBootRoot;
import jepl.JEPLConnection;
import jepl.JEPLConnectionListener;
import jepl.JEPLDAL;
import jepl.JEPLNonJTADataSource;
import jepl.JEPLResultSetDAO;
import jepl.JEPLTask;
import jepl.JEPLTransaction;
import jepl.JEPLTransactionalNonJTA;
import org.jooq.SQLDialect;
import org.jooq.impl.DefaultDSLContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author jmarranz
 */
public class TestJooq
{
    public TestJooq()
    {
    }
    
    // Here the test setup stuff...
    
    @BeforeClass
    public static void setUpClass()
    {
    }
    
    @AfterClass
    public static void tearDownClass()
    {
    }
    
    @Before
    public void setUp()
    {
    }
    
    @After
    public void tearDown()
    {
    }
        
    @Test
    public void jooqExample() throws Exception
    {
        ComboPooledDataSource ds = new ComboPooledDataSource();        
        try
        {
            configureDataSource(ds);

            DefaultDSLContext jooqCtx = new DefaultDSLContext(SQLDialect.MYSQL);            
            
            JEPLNonJTADataSource jds = JEPLBootRoot.get().createJEPLBootNonJTA().createJEPLNonJTADataSource(ds);           
            JEPLConnectionListener conListener = (JEPLConnection con,JEPLTask task2) -> { // void setupJEPLConnection(JEPLConnection con,JEPLTask task) throws Exception                      
                        con.getConnection().setAutoCommit(true); 
                    };            
            jds.addJEPLListener(conListener); // Simple alternative:  jds.setDefaultAutoCommit(true);
            
            createTables(jds);
                       
            ContactDAO dao = new ContactDAO(jds,jooqCtx);
          
            Contact contact1 = new Contact();
            contact1.setName("One Contact");
            contact1.setPhone("9999999");
            contact1.setEmail("contactOne@world.com");
            dao.insert(contact1);
            
            Contact contact2 = new Contact();            
            contact2.setName("Another Contact");
            contact2.setPhone("8888888");
            contact2.setEmail("contactAnother@world.com");            
            dao.insert(contact2);    
            
            Contact contact3 = new Contact();            
            contact3.setName("And other Contact");
            contact3.setPhone("6666666");
            contact3.setEmail("contactAndOther@world.com");            
            dao.insertExplicitResultSetListener(contact3); // just to play           
                        
            contact3.setPhone("7777777");            
            dao.update(contact3);
            
            JEPLTask<Contact[]> task = () ->
            { // Connection got
                JEPLResultSetDAO<Contact> list = dao.selectActiveResult();
                if (list.isClosed()) throw new RuntimeException("Unexpected");                
                Contact[] res = ContactDAO.toContactArray(list);
                if (!list.isClosed()) throw new RuntimeException("Unexpected");
             
                int size2 = dao.selectCount();
                if (res.length != size2)
                    throw new RuntimeException("Unexpected");

                return res;
            }; // Connection released
            Contact[] array = dao.getJEPLDAO().getJEPLDataSource().exec(task);                                   
            System.out.println("Result:");
            for(Contact contact : array)
                System.out.println("  Contact: " + contact.getId() + " " + contact.getName() + " " + contact.getPhone());
            
            List<Contact> list = dao.selectNotActiveResult();           
            System.out.println("Result:");            
            list.stream().forEach((contact) -> {            
                System.out.println("  Contact: " + contact.getId() + " " + contact.getName() + " " + contact.getPhone());
            });
            
            int maxResults = 2;
            list = dao.selectNotActiveResult(maxResults);
            if (list.size() != maxResults)
                throw new RuntimeException("Unexpected");             
            System.out.println("Result maxResults (" + maxResults + "):");            
            list.stream().forEach((contact) -> {            
                System.out.println("  Contact: " + contact.getId() + " " + contact.getName() + " " + contact.getPhone());
            });                        
            
            list = dao.selectNotActiveResult2(maxResults);
            if (list.size() != maxResults)
                throw new RuntimeException("Unexpected");              
            System.out.println("Result maxResults (" + maxResults + "):");            
            list.stream().forEach((contact) -> {            
                System.out.println("  Contact: " + contact.getId() + " " + contact.getName() + " " + contact.getPhone());
            });               
                       
            
            int from = 1;
            int to = 2;            
            list = dao.selectRange(from,to);
            if (list.size() != (to - from))
                throw new RuntimeException("Unexpected");             
            System.out.println("Result from/to " + from + "/" + to + ":");
            list.stream().forEach((contact) -> {            
                System.out.println("  Contact: " + contact.getId() + " " + contact.getName() + " " + contact.getPhone());
            });            
                               
            list = dao.selectRange2(from,to);
            if (list.size() != (to - from))
                throw new RuntimeException("Unexpected");             
            System.out.println("Result from/to " + from + "/" + to + ":");
            list.stream().forEach((contact) -> {            
                System.out.println("  Contact: " + contact.getId() + " " + contact.getName() + " " + contact.getPhone());
            });             
            
            jdbcTxnExample(dao);            
         
            jdbcTxnExample2(dao);             
            
            jdbcTxnExample3(dao); 
            
            jdbcTxnExample4(dao);            
            
            jdbcTxnExample5(dao,true);
            
            jdbcTxnExample5(dao,false);            
        }
        /*
        catch(Exception ex)
        {
            ex.printStackTrace();
            throw ex;
        } */       
        finally
        {
            destroyDataSource(ds);
        }
        
    }

    
    private static void configureDataSource(ComboPooledDataSource cpds) throws PropertyVetoException
    {
        // Create before a database named "testjooq"
        String jdbcXADriver = "com.mysql.jdbc.jdbc2.optional.MysqlXADataSource";
        String jdbcURL="jdbc:mysql://127.0.0.1:3306/testjooq?pinGlobalTxToPhysicalConnection=true"; // testjooq
        String jdbcUserName="root";
        String jdbcPassword="root2000";        
        int poolSize = 3;
        int maxStatements = 180;

        cpds.setDriverClass(jdbcXADriver);            
        cpds.setJdbcUrl(jdbcURL);
        cpds.setUser(jdbcUserName);
        cpds.setPassword(jdbcPassword);

        cpds.setMaxPoolSize(poolSize);
        cpds.setMaxStatements(maxStatements);    
    }
    
    private static void destroyDataSource(ComboPooledDataSource cpds) 
    {
        try
        {
            DataSources.destroy(cpds);
            cpds.close();
        }
        catch (SQLException ex)
        {
            throw new RuntimeException(ex);
        }          
    }    
       
    
    private static void createTables(JEPLNonJTADataSource jds)
    {
        JEPLDAL dal = jds.createJEPLDAL();  
              
        dal.createJEPLDALQuery("DROP TABLE IF EXISTS CONTACT").executeUpdate();

        dal.createJEPLDALQuery(
            "CREATE TABLE CONTACT (" +
            " ID INT NOT NULL AUTO_INCREMENT," +
            " EMAIL VARCHAR(255) NOT NULL," +
            " NAME VARCHAR(255) NOT NULL," +
            " PHONE VARCHAR(255) NOT NULL," +
            " PRIMARY KEY (ID)" +
            ")" +
            "ENGINE=InnoDB"
        ).executeUpdate(); 
    }
           
    private static void jdbcTxnExample(ContactDAO dao)
    {
        checkNotEmpty(dao);      
        
        List<Contact> contacts = dao.selectNotActiveResult();        
        
        JEPLTask<Void> task = () -> { // Void exec() throws Exception
            contacts.stream().forEach((contact) ->
            {            
                dao.delete(contact);
            });
            // No, no, we need a rollback
            throw new Exception("I want a rollback to avoid to delete rows");
        };
        
        try
        {
            JEPLNonJTADataSource jds = (JEPLNonJTADataSource)dao.getJEPLDAO().getJEPLDataSource();
            boolean autoCommit = false;
            jds.exec(task,autoCommit); 
            
            throw new RuntimeException("Unexpected, ever executed rollback in this example");
        }
        catch(Exception ex)
        {
            // Rollback, data is still saved
            checkNotEmpty(dao);
        }
    }            
    
    private static void jdbcTxnExample2(ContactDAO dao)
    {
        checkNotEmpty(dao);      
        
        List<Contact> contacts = dao.selectNotActiveResult();        
        
        JEPLTask<Void> task = () -> { // Void exec() throws Exception
            contacts.stream().forEach((contact) ->
            {            
                dao.delete(contact);
            });
            // No, no, we need a rollback
            throw new Exception("I want a rollback to avoid to delete rows");
        };
        
        JEPLConnectionListener conListener = (JEPLConnection con,JEPLTask task2) -> { // void setupJEPLConnection(JEPLConnection con,JEPLTask task) throws Exception                      
                    con.getConnection().setAutoCommit(false); 
                };        
        
        try
        {
            dao.getJEPLDAO().getJEPLDataSource().exec(task,conListener); 
            
            throw new RuntimeException("Unexpected, ever executed rollback in this example");
        }
        catch(Exception ex)
        {
            // Rollback, data is still saved
            checkNotEmpty(dao);
        }
    }            
    
    private static void jdbcTxnExample3(ContactDAO dao)
    {
        checkNotEmpty(dao);      
        
        List<Contact> contacts = dao.selectNotActiveResult();
        
        JEPLTask<Void> task = () -> { // Void exec() throws Exception
            contacts.stream().forEach((contact) ->
            {
                dao.delete(contact);            
            });
            // No, no, we need a rollback
            throw new Exception("I want a rollback to avoid to delete rows");
        };
        
        JEPLConnectionListener connListener = (JEPLConnection con,JEPLTask task2) -> 
            {
                con.getConnection().setAutoCommit(false); // transaction
                try
                {
                    task2.exec();
                    con.getConnection().commit();
                }
                catch(Exception ex)
                {
                    con.getConnection().rollback();
                    throw new SQLException(ex);
                }
            };
        
        try
        {
            dao.getJEPLDAO().getJEPLDataSource().exec(task,connListener); 
            
            throw new RuntimeException("Unexpected, ever executed rollback in this example");
        }
        catch(Exception ex)
        {
            // Rollback, data is still saved
            checkNotEmpty(dao);
        }
    }
       
    
    private static void jdbcTxnExample4(ContactDAO dao)
    {
        checkNotEmpty(dao);      
        
        List<Contact> contacts = dao.selectNotActiveResult();
        
        JEPLTask<Void> task = () -> { // Void exec() throws Exception
            contacts.stream().forEach((contact) ->
            {
                dao.delete(contact);            
            });
            // No, no, we need a rollback
            throw new Exception("I want a rollback to avoid to delete rows");
        };
        
        JEPLConnectionListener connListener = (JEPLConnection con,JEPLTask task2) -> 
            {
                JEPLTransaction txn = con.getJEPLTransaction();
                txn.begin(); // Executes setDefaultAutoCommit(false);

                try
                {
                    task2.exec();
                    txn.commit();
                }
                catch(Exception ex)
                {
                    txn.rollback();
                    throw ex;
                }
            };        
        
        try
        {
            JEPLNonJTADataSource jds = (JEPLNonJTADataSource)dao.getJEPLDAO().getJEPLDataSource();
            jds.exec(task,connListener); 
            
            throw new RuntimeException("Unexpected, ever executed rollback in this example");
        }
        catch(Exception ex)
        {
            // Rollback, data is still saved
            checkNotEmpty(dao);
        }
    }    
        
    private static void jdbcTxnExample5(ContactDAO dao,final boolean simulateRollback)
    {
        checkNotEmpty(dao);              
        
        JEPLTask<Void> task = new JEPLTask<Void>() {

            @Override
            @JEPLTransactionalNonJTA  // Is equals to @JEPLTransactionalNonJTA(autoCommit = false)
            public Void exec() throws SQLException
            { // Connection got
                JEPLResultSetDAO<Contact> list = dao.selectActiveResult();                  
                if (list.isClosed()) throw new RuntimeException("Unexpected");

                ((List<Contact>)list).stream().forEach((contact) ->  // JEPLResultSetDAO implements the List interface
                {
                    boolean deleted = dao.delete(contact);
                    if (!deleted)
                        throw new RuntimeException("Unexpected");

                    if (simulateRollback)
                        throw new RuntimeException("Force Rollback");
                });
                
                if (!list.isClosed()) throw new RuntimeException("Unexpected");
                
                checkEmpty(dao);
                
                return null;
            } // Connection released
        };
        
        try
        {
            dao.getJEPLDAO().getJEPLDataSource().exec(task); 
            
            checkEmpty(dao);
        }
        catch(Exception ex)
        {
            // Rollback when simulateFailed = true
            checkNotEmpty(dao);           
        }
    }    
    
    private static void checkEmpty(ContactDAO dao)
    {            
        if (dao.selectCount() != 0)
            throw new RuntimeException("Unexpected");            
    }
    
    private static void checkNotEmpty(ContactDAO dao)
    {            
        if (dao.selectCount() == 0)
            throw new RuntimeException("Unexpected");            
    }    
}

