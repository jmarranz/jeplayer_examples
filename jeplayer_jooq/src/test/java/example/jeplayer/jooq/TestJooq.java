package example.jeplayer.jooq;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.DataSources;
import example.jeplayer.jooq.dao.ContactDAO;
import example.jeplayer.jooq.model.Contact;
import java.beans.PropertyVetoException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import jepl.JEPLBootRoot;
import jepl.JEPLCachedResultSet;
import jepl.JEPLConnection;
import jepl.JEPLConnectionListener;
import jepl.JEPLDAL;
import jepl.JEPLNonJTADataSource;
import jepl.JEPLResultSet;
import jepl.JEPLResultSetDAO;
import jepl.JEPLTask;
import jepl.JEPLTransaction;
import jepl.JEPLTransactionalNonJTA;
import org.jooq.SQLDialect;
import static org.jooq.impl.DSL.avg;
import static org.jooq.impl.DSL.count;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;
import org.jooq.impl.DefaultDSLContext;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
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
        for(short i = 0; i < 3; i++)
        {
            System.out.println("Mapping mode:" + i);
            jooqExample(i);
        }
    }
    
    private void jooqExample(short mappingMode) throws Exception
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
                       
            ContactDAO dao = new ContactDAO(jds,jooqCtx,mappingMode);
          
            Contact contact1 = new Contact();
            contact1.setName("One Contact");
            contact1.setPhone("1111111");
            contact1.setEmail("contactOne@world.com");
            dao.insert(contact1);
            
            Contact contact2 = new Contact();            
            contact2.setName("Another Contact");
            contact2.setPhone("2222222");
            contact2.setEmail("contactAnother@world.com");            
            dao.insertImplicitUpdateListener(contact2);  // just to play  
            
            Contact contact3 = new Contact();            
            contact3.setName("And other Contact");
            contact3.setPhone("3333333");
            contact3.setEmail("contactAndOther@world.com");            
            dao.insertExplicitResultSetListener(contact3); // just to play           
                        
            contact3.setPhone("4444444");            
            boolean updated = dao.update(contact3);
            assertTrue(updated);            
            
            contact3.setPhone("3333333");            
            updated = dao.updateImplicitUpdateListener(contact3); // just to play
            assertTrue(updated);          
            
            JEPLTask<Contact[]> task = () -> // public Contact[] exec() throws Exception
            { // Connection got
                JEPLResultSetDAO<Contact> list = dao.selectActiveResult();
                assertFalse(list.isClosed());                
                Contact[] res = ContactDAO.toContactArray(list);
                assertTrue(list.isClosed());
             
                int size2 = dao.selectCount();
                assertTrue(res.length == size2);

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
            assertTrue(list.size() == maxResults);
            
            System.out.println("Result maxResults (" + maxResults + "):");            
            list.stream().forEach((contact) -> {            
                System.out.println("  Contact: " + contact.getId() + " " + contact.getName() + " " + contact.getPhone());
            });                        
            
            list = dao.selectNotActiveResult2(maxResults);
            assertTrue(list.size() == maxResults);              
            System.out.println("Result maxResults (" + maxResults + "):");            
            list.stream().forEach((contact) -> {            
                System.out.println("  Contact: " + contact.getId() + " " + contact.getName() + " " + contact.getPhone());
            });               
                       
            
            int from = 1;
            int to = 2;            
            list = dao.selectRange(from,to);
            assertTrue(list.size() == (to - from));            
            System.out.println("Result from/to " + from + "/" + to + ":");
            list.stream().forEach((contact) -> {            
                System.out.println("  Contact: " + contact.getId() + " " + contact.getName() + " " + contact.getPhone());
            });            
                               
            list = dao.selectRange2(from,to);
            assertTrue(list.size() == (to - from));             
            System.out.println("Result from/to " + from + "/" + to + ":");
            list.stream().forEach((contact) -> {            
                System.out.println("  Contact: " + contact.getId() + " " + contact.getName() + " " + contact.getPhone());
            });             
            
            JEPLDAL dal = jds.createJEPLDAL();
            
            dalActiveSelect(dal,jooqCtx);
            
            dalNotActiveSelect(dal,jooqCtx);
            
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
        String jdbcURL="jdbc:mysql://127.0.0.1:3306/testjeplayer?pinGlobalTxToPhysicalConnection=true"; // testjooq
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
           
    
    public static void dalActiveSelect(final JEPLDAL dal,DefaultDSLContext jooqCtx)
    {          
        // Supposed 3 rows in contact table
        JEPLTask<Void> task = () -> { // public Void exec() throws Exception
            JEPLResultSet resSet = dal.createJEPLDALQuery(
                    jooqCtx.select(count().as("CO"),avg(field("ID",int.class)).as("AV")).from(table("CONTACT")).getSQL()) // SELECT COUNT(*) AS CO,AVG(ID) AS AV FROM CONTACT  
                    .getJEPLResultSet();
          
            assertFalse(resSet.isClosed());                

            ResultSet rs = resSet.getResultSet();
            ResultSetMetaData metadata = rs.getMetaData();
            int ncols = metadata.getColumnCount();
            String[] colNames = new String[ncols];
            for(int i = 0; i < ncols; i++)
                colNames[i] = metadata.getColumnLabel(i + 1); // Starts at 1                     

            assertTrue(colNames.length == 2);
            assertTrue(colNames[0].equals("CO"));
            assertTrue(colNames[1].equals("AV"));

            assertTrue(rs.getRow() == 0);                 

            assertFalse(resSet.isClosed());

            resSet.next();

            assertTrue(rs.getRow() == 1);

            int count = rs.getInt(1);
            assertTrue(count == 3);       
            count = rs.getInt("CO");
            assertTrue(count == 3);

            float avg = rs.getFloat(1);
            assertTrue(avg > 0);        
            avg = rs.getFloat("AV");
            assertTrue(avg > 0);                       

            assertFalse(resSet.next());                
            assertTrue(resSet.isClosed());                

            assertTrue(resSet.count() == 1); 
            return null;
        };
        dal.getJEPLDataSource().exec(task);
    }        
    
    public static void dalNotActiveSelect(final JEPLDAL dal,DefaultDSLContext jooqCtx)
    {          
        // Supposed 3 rows in contact table
        JEPLCachedResultSet resSet = dal.createJEPLDALQuery(
                jooqCtx.select(count().as("CO"),avg(field("ID",int.class)).as("AV")).from(table("CONTACT")).getSQL()) // SELECT COUNT(*) AS CO,AVG(ID) AS AV FROM CONTACT     
                .getJEPLCachedResultSet();
        String[] colNames = resSet.getColumnLabels();
        assertTrue(colNames.length == 2);
        assertTrue(colNames[0].equals("CO"));
        assertTrue(colNames[1].equals("AV"));
        
        assertTrue(resSet.size() == 1);

        int count = resSet.getValue(1, 1, int.class); // Row 1, column 1
        assertTrue(count == 3);
        count = resSet.getValue(1, "CO", int.class);
        assertTrue(count == 3);

        float avg = resSet.getValue(1, 2, float.class); // Row 1, column 2
        assertTrue(avg > 0);
        avg = resSet.getValue(1, "AV", float.class);
        assertTrue(avg > 0);
    }        
        
    
    private static void jdbcTxnExample(ContactDAO dao)
    {
        checkNotEmpty(dao);      
        
        List<Contact> contacts = dao.selectNotActiveResult();        
        
        JEPLTask<Void> task = () -> { // Void exec() throws Exception
            contacts.stream().forEach((contact) ->
            {            
                boolean deleted = dao.delete(contact);
                assertTrue(deleted);               
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
                boolean deleted = dao.deleteImplicitUpdateListener(contact); // just to play
                assertTrue(deleted);               
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
                boolean deleted = dao.delete(contact);
                assertTrue(deleted);                
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
                boolean deleted = dao.delete(contact);
                assertTrue(deleted);            
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
                assertFalse(list.isClosed());

                ((List<Contact>)list).stream().forEach((contact) ->  // JEPLResultSetDAO implements the List interface
                {
                    boolean deleted = dao.delete(contact);
                    assertTrue(deleted);

                    if (simulateRollback)
                        throw new RuntimeException("Force Rollback");
                });
                
                assertTrue(list.isClosed());
                
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
        assertTrue(dao.selectCount() == 0);            
    }
    
    private static void checkNotEmpty(ContactDAO dao)
    {            
        assertFalse(dao.selectCount() == 0);            
    }    
}

